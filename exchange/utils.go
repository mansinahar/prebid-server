package exchange

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/buger/jsonparser"
	"github.com/mxmCherry/openrtb"
	"github.com/prebid/prebid-server/config"
	"github.com/prebid/prebid-server/openrtb_ext"
	"github.com/prebid/prebid-server/pbsmetrics"
	"github.com/prebid/prebid-server/privacy"
	"github.com/prebid/prebid-server/privacy/ccpa"
	"github.com/prebid/prebid-server/privacy/lmt"
)

var integrationTypeMap = map[pbsmetrics.RequestType]config.IntegrationType{
	pbsmetrics.ReqTypeAMP:      config.IntegrationTypeAMP,
	pbsmetrics.ReqTypeORTB2App: config.IntegrationTypeApp,
	pbsmetrics.ReqTypeVideo:    config.IntegrationTypeVideo,
	pbsmetrics.ReqTypeORTB2Web: config.IntegrationTypeWeb,
}

const unknownBidder string = ""

func BidderToPrebidSChains(req *openrtb_ext.ExtRequest) (map[string]*openrtb_ext.ExtRequestPrebidSChainSChain, error) {
	bidderToSChains := make(map[string]*openrtb_ext.ExtRequestPrebidSChainSChain)

	if req != nil {
		for _, schainWrapper := range req.Prebid.SChains {
			for _, bidder := range schainWrapper.Bidders {
				if _, present := bidderToSChains[bidder]; present {
					return nil, fmt.Errorf("request.ext.prebid.schains contains multiple schains for bidder %s; "+
						"it must contain no more than one per bidder.", bidder)
				} else {
					bidderToSChains[bidder] = &schainWrapper.SChain
				}
			}
		}
	}

	return bidderToSChains, nil
}

func gdprEnabled(account *config.Account, privacyConfig config.Privacy, integrationType config.IntegrationType) bool {
	if accountEnabled := account.GDPR.EnabledForIntegrationType(integrationType); accountEnabled != nil {
		return *accountEnabled
	}
	return privacyConfig.GDPR.Enabled
}

func ccpaEnabled(account *config.Account, privacyConfig config.Privacy, requestType config.IntegrationType) bool {
	if accountEnabled := account.CCPA.EnabledForIntegrationType(requestType); accountEnabled != nil {
		return *accountEnabled
	}
	return privacyConfig.CCPA.Enforce
}

func extractCCPA(orig *openrtb.BidRequest, privacyConfig config.Privacy, account *config.Account, aliases map[string]string, requestType config.IntegrationType) (privacy.PolicyEnforcer, error) {
	ccpaPolicy, err := ccpa.ReadFromRequest(orig)
	if err != nil {
		return privacy.NilPolicyEnforcer{}, err
	}

	validBidders := GetValidBidders(aliases)
	ccpaParsedPolicy, err := ccpaPolicy.Parse(validBidders)
	if err != nil {
		return privacy.NilPolicyEnforcer{}, err
	}

	ccpaEnforcer := privacy.EnabledPolicyEnforcer{
		Enabled:        ccpaEnabled(account, privacyConfig, requestType),
		PolicyEnforcer: ccpaParsedPolicy,
	}
	return ccpaEnforcer, nil
}

func extractLMT(orig *openrtb.BidRequest, privacyConfig config.Privacy) privacy.PolicyEnforcer {
	return privacy.EnabledPolicyEnforcer{
		Enabled:        privacyConfig.LMT.Enforce,
		PolicyEnforcer: lmt.ReadFromRequest(orig),
	}
}

func getAuctionBidderRequests(req AuctionRequest) (bidderRequests []BidderRequest, errs []error) {

	impsByBidder, errs := splitImps(req.BidRequest.Imp)
	if len(errs) > 0 {
		return
	}

	bidderRequests = make([]BidderRequest, 0, len(impsByBidder))

	explicitBuyerUIDs, err := extractBuyerUIDs(req.BidRequest.User)
	if err != nil {
		return nil, []error{err}
	}

	var sChainsByBidder map[string]*openrtb_ext.ExtRequestPrebidSChainSChain

	sChainsByBidder, err = BidderToPrebidSChains(req.Ext)
	if err != nil {
		return nil, []error{err}
	}

	reqExt, err := getExtJson(req.BidRequest, req.Ext)
	if err != nil {
		return nil, []error{err}
	}

	for bidder, imps := range impsByBidder {
		coreBidder := resolveBidder(bidder, req.Aliases)

		reqCopy := *req.BidRequest
		reqCopy.Imp = imps
		reqCopy.Ext = reqExt
		prepareSource(&reqCopy, bidder, sChainsByBidder)

		bidder := BidderRequest{
			BidderName:     openrtb_ext.BidderName(bidder),
			BidderCoreName: coreBidder,
			BidRequest:     &reqCopy,
			BidderLabels: pbsmetrics.AdapterLabels{
				Source:      req.LegacyLabels.Source,
				RType:       req.LegacyLabels.RType,
				Adapter:     coreBidder,
				PubID:       req.LegacyLabels.PubID,
				CookieFlag:  req.LegacyLabels.CookieFlag,
				AdapterBids: pbsmetrics.AdapterBidPresent,
			},
		}
		if hadSync := prepareUser(&reqCopy, bidder.BidderName.String(), coreBidder, explicitBuyerUIDs, req.UserSyncs); !hadSync && req.BidRequest.App == nil {
			bidder.BidderLabels.CookieFlag = pbsmetrics.CookieFlagNo
		} else {
			bidder.BidderLabels.CookieFlag = pbsmetrics.CookieFlagYes
		}

		bidderRequests = append(bidderRequests, bidder)
	}
	return bidderRequests, nil
}

func getExtJson(req *openrtb.BidRequest, unpackedExt *openrtb_ext.ExtRequest) (json.RawMessage, error) {
	if len(req.Ext) == 0 || unpackedExt == nil {
		return json.RawMessage(``), nil
	}

	extCopy := *unpackedExt
	extCopy.Prebid.SChains = nil
	return json.Marshal(extCopy)
}

func prepareSource(req *openrtb.BidRequest, bidder string, sChainsByBidder map[string]*openrtb_ext.ExtRequestPrebidSChainSChain) {
	const sChainWildCard = "*"
	var selectedSChain *openrtb_ext.ExtRequestPrebidSChainSChain

	wildCardSChain := sChainsByBidder[sChainWildCard]
	bidderSChain := sChainsByBidder[bidder]

	// source should not be modified
	if bidderSChain == nil && wildCardSChain == nil {
		return
	}

	if bidderSChain != nil {
		selectedSChain = bidderSChain
	} else {
		selectedSChain = wildCardSChain
	}

	// set source
	if req.Source == nil {
		req.Source = &openrtb.Source{}
	}
	schain := openrtb_ext.ExtRequestPrebidSChain{
		SChain: *selectedSChain,
	}
	sourceExt, err := json.Marshal(schain)
	if err == nil {
		req.Source.Ext = sourceExt
	}
}

// extractBuyerUIDs parses the values from user.ext.prebid.buyeruids, and then deletes those values from the ext.
// This prevents a Bidder from using these values to figure out who else is involved in the Auction.
func extractBuyerUIDs(user *openrtb.User) (map[string]string, error) {
	if user == nil {
		return nil, nil
	}
	if len(user.Ext) == 0 {
		return nil, nil
	}

	var userExt openrtb_ext.ExtUser
	if err := json.Unmarshal(user.Ext, &userExt); err != nil {
		return nil, err
	}
	if userExt.Prebid == nil {
		return nil, nil
	}

	// The API guarantees that user.ext.prebid.buyeruids exists and has at least one ID defined,
	// as long as user.ext.prebid exists.
	buyerUIDs := userExt.Prebid.BuyerUIDs
	userExt.Prebid = nil
	if userExt.Consent != "" || userExt.DigiTrust != nil {
		if newUserExtBytes, err := json.Marshal(userExt); err != nil {
			return nil, err
		} else {
			user.Ext = newUserExtBytes
		}
	} else {
		user.Ext = nil
	}
	return buyerUIDs, nil
}

// splitImps takes a list of Imps and returns a map of imps which have been sanitized for each bidder.
//
// For example, suppose imps has two elements. One goes to rubicon, while the other goes to appnexus and index.
// The returned map will have three keys: rubicon, appnexus, and index--each with one Imp.
// The "imp.ext" value of the appnexus Imp will only contain the "prebid" values, and "appnexus" value at the "bidder" key.
// The "imp.ext" value of the rubicon Imp will only contain the "prebid" values, and "rubicon" value at the "bidder" key.
//
// The goal here is so that Bidders only get Imps and Imp.Ext values which are intended for them.
func splitImps(imps []openrtb.Imp) (map[string][]openrtb.Imp, []error) {
	impExts, err := parseImpExts(imps)
	if err != nil {
		return nil, []error{err}
	}

	splitImps := make(map[string][]openrtb.Imp, len(imps))
	var errList []error

	for i := 0; i < len(imps); i++ {
		imp := imps[i]
		impExt := impExts[i]

		var firstPartyDataContext json.RawMessage
		if context, exists := impExt[openrtb_ext.FirstPartyDataContextExtKey]; exists {
			firstPartyDataContext = context
		}

		rawPrebidExt, ok := impExt[openrtb_ext.PrebidExtKey]

		if ok {
			var prebidExt openrtb_ext.ExtImpPrebid

			if err := json.Unmarshal(rawPrebidExt, &prebidExt); err == nil && prebidExt.Bidder != nil {
				if errs := sanitizedImpCopy(&imp, prebidExt.Bidder, rawPrebidExt, firstPartyDataContext, &splitImps); errs != nil {
					errList = append(errList, errs...)
				}

				continue
			}
		}

		if errs := sanitizedImpCopy(&imp, impExt, rawPrebidExt, firstPartyDataContext, &splitImps); errs != nil {
			errList = append(errList, errs...)
		}
	}

	return splitImps, nil
}

// sanitizedImpCopy returns a copy of imp with its ext filtered so that only "prebid", "context", and bidder params exist.
// It will not mutate the input imp.
// This function will write the new imps to the output map passed in
func sanitizedImpCopy(imp *openrtb.Imp,
	bidderExts map[string]json.RawMessage,
	rawPrebidExt json.RawMessage,
	firstPartyDataContext json.RawMessage,
	out *map[string][]openrtb.Imp) []error {

	var prebidExt map[string]json.RawMessage
	var errs []error

	if err := json.Unmarshal(rawPrebidExt, &prebidExt); err == nil {
		// Remove the entire bidder field. We will already have the content we need in bidderExts. We
		// don't want to include other demand partners' bidder params in the sanitized imp.
		if _, hasBidderField := prebidExt["bidder"]; hasBidderField {
			delete(prebidExt, "bidder")

			var err error
			if rawPrebidExt, err = json.Marshal(prebidExt); err != nil {
				errs = append(errs, err)
			}
		}
	}

	for bidder, ext := range bidderExts {
		if bidder == openrtb_ext.PrebidExtKey || bidder == openrtb_ext.FirstPartyDataContextExtKey {
			continue
		}

		impCopy := *imp
		newExt := make(map[string]json.RawMessage, 3)

		newExt["bidder"] = ext

		if rawPrebidExt != nil {
			newExt[openrtb_ext.PrebidExtKey] = rawPrebidExt
		}

		if len(firstPartyDataContext) > 0 {
			newExt[openrtb_ext.FirstPartyDataContextExtKey] = firstPartyDataContext
		}

		rawExt, err := json.Marshal(newExt)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		impCopy.Ext = rawExt

		otherImps, _ := (*out)[bidder]
		(*out)[bidder] = append(otherImps, impCopy)
	}

	if len(errs) > 0 {
		return errs
	}

	return nil
}

// prepareUser changes req.User so that it's ready for the given bidder.
// This *will* mutate the request, but will *not* mutate any objects nested inside it.
//
// In this function, "givenBidder" may or may not be an alias. "coreBidder" must *not* be an alias.
// It returns true if a Cookie User Sync existed, and false otherwise.
func prepareUser(req *openrtb.BidRequest, givenBidder string, coreBidder openrtb_ext.BidderName, explicitBuyerUIDs map[string]string, usersyncs IdFetcher) bool {
	cookieId, hadCookie := usersyncs.GetId(coreBidder)

	if id, ok := explicitBuyerUIDs[givenBidder]; ok {
		req.User = copyWithBuyerUID(req.User, id)
	} else if hadCookie {
		req.User = copyWithBuyerUID(req.User, cookieId)
	}

	return hadCookie
}

// copyWithBuyerUID either overwrites the BuyerUID property on user with the argument, or returns
// a new (empty) User with the BuyerUID already set.
func copyWithBuyerUID(user *openrtb.User, buyerUID string) *openrtb.User {
	if user == nil {
		return &openrtb.User{
			BuyerUID: buyerUID,
		}
	}
	if user.BuyerUID == "" {
		clone := *user
		clone.BuyerUID = buyerUID
		return &clone
	}
	return user
}

// resolveBidder returns the known BidderName associated with bidder, if bidder is an alias. If it's not an alias, the bidder is returned.
func resolveBidder(bidder string, aliases map[string]string) openrtb_ext.BidderName {
	if coreBidder, ok := aliases[bidder]; ok {
		return openrtb_ext.BidderName(coreBidder)
	}
	return openrtb_ext.BidderName(bidder)
}

// parseImpExts does a partial-unmarshal of the imp[].Ext field.
// The keys in the returned map are expected to be "prebid", "context", CoreBidderNames, or Aliases for this request.
func parseImpExts(imps []openrtb.Imp) ([]map[string]json.RawMessage, error) {
	exts := make([]map[string]json.RawMessage, len(imps))
	// Loop over every impression in the request
	for i := 0; i < len(imps); i++ {
		// Unpack each set of extensions found in the Imp array
		err := json.Unmarshal(imps[i].Ext, &exts[i])
		if err != nil {
			return nil, fmt.Errorf("Error unpacking extensions for Imp[%d]: %s", i, err.Error())
		}
	}
	return exts, nil
}

// parseAliases parses the aliases from the BidRequest
func parseAliases(orig *openrtb.BidRequest) (map[string]string, error) {
	var aliases map[string]string
	if value, dataType, _, err := jsonparser.Get(orig.Ext, openrtb_ext.PrebidExtKey, "aliases"); dataType == jsonparser.Object && err == nil {
		if err := json.Unmarshal(value, &aliases); err != nil {
			return nil, err
		}
	} else if dataType != jsonparser.NotExist && err != jsonparser.KeyPathNotFoundError {
		return nil, err
	}
	return aliases, nil
}

func GetValidBidders(aliases map[string]string) map[string]struct{} {
	validBidders := openrtb_ext.BuildBidderNameHashSet()

	for k := range aliases {
		validBidders[k] = struct{}{}
	}

	return validBidders
}

// Quick little randomizer for a list of strings. Stuffing it in utils to keep other files clean
func randomizeList(list []openrtb_ext.BidderName) {
	l := len(list)
	perm := rand.Perm(l)
	var j int
	for i := 0; i < l; i++ {
		j = perm[i]
		list[i], list[j] = list[j], list[i]
	}
}

func extractBidRequestExt(bidRequest *openrtb.BidRequest) (*openrtb_ext.ExtRequest, error) {
	requestExt := &openrtb_ext.ExtRequest{}

	if bidRequest == nil {
		return requestExt, fmt.Errorf("Error bidRequest should not be nil")
	}

	if len(bidRequest.Ext) > 0 {
		err := json.Unmarshal(bidRequest.Ext, &requestExt)
		if err != nil {
			return requestExt, fmt.Errorf("Error decoding Request.ext : %s", err.Error())
		}
	}
	return requestExt, nil
}

func getExtCacheInstructions(requestExt *openrtb_ext.ExtRequest) extCacheInstructions {
	//returnCreative defaults to true
	cacheInstructions := extCacheInstructions{returnCreative: true}
	foundBidsRC := false
	foundVastRC := false

	if requestExt != nil && requestExt.Prebid.Cache != nil {
		if requestExt.Prebid.Cache.Bids != nil {
			cacheInstructions.cacheBids = true
			if requestExt.Prebid.Cache.Bids.ReturnCreative != nil {
				cacheInstructions.returnCreative = *requestExt.Prebid.Cache.Bids.ReturnCreative
				foundBidsRC = true
			}
		}
		if requestExt.Prebid.Cache.VastXML != nil {
			cacheInstructions.cacheVAST = true
			if requestExt.Prebid.Cache.VastXML.ReturnCreative != nil {
				cacheInstructions.returnCreative = *requestExt.Prebid.Cache.VastXML.ReturnCreative
				foundVastRC = true
			}
		}
	}

	if foundBidsRC && foundVastRC {
		cacheInstructions.returnCreative = *requestExt.Prebid.Cache.Bids.ReturnCreative || *requestExt.Prebid.Cache.VastXML.ReturnCreative
	}

	return cacheInstructions
}

func getExtTargetData(requestExt *openrtb_ext.ExtRequest, cacheInstructions *extCacheInstructions) *targetData {
	var targData *targetData

	if requestExt != nil && requestExt.Prebid.Targeting != nil {
		targData = &targetData{
			priceGranularity:  requestExt.Prebid.Targeting.PriceGranularity,
			includeWinners:    requestExt.Prebid.Targeting.IncludeWinners,
			includeBidderKeys: requestExt.Prebid.Targeting.IncludeBidderKeys,
			includeCacheBids:  cacheInstructions.cacheBids,
			includeCacheVast:  cacheInstructions.cacheVAST,
			includeFormat:     requestExt.Prebid.Targeting.IncludeFormat,
			preferDeals:       requestExt.Prebid.Targeting.PreferDeals,
		}
	}
	return targData
}

func getDebugInfo(req AuctionRequest) bool {
	return (req.BidRequest != nil && req.BidRequest.Test == 1) || (req.Ext != nil && req.Ext.Prebid.Debug)
}

func getExtBidAdjustmentFactors(requestExt *openrtb_ext.ExtRequest) map[string]float64 {
	var bidAdjustmentFactors map[string]float64
	if requestExt != nil {
		bidAdjustmentFactors = requestExt.Prebid.BidAdjustmentFactors
	}
	return bidAdjustmentFactors
}
