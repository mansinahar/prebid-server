package generic

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"text/template"

	"github.com/golang/glog"
	"github.com/mxmCherry/openrtb"
	"github.com/prebid/prebid-server/adapters"
	"github.com/prebid/prebid-server/errortypes"
	"github.com/prebid/prebid-server/macros"
	"github.com/prebid/prebid-server/openrtb_ext"
)

// Adapter is a struct defining the Openrtb Generic Adapter
type Adapter struct {
	EndpointTemplate template.Template
}

// NewAdapter creates a new instance of the GenericAdapter
func NewAdapter(endpoint string) *Adapter {
	template, err := template.New("endpointTemplate").Parse(endpoint)
	if err != nil {
		glog.Fatal("Unable to parse endpoint url template")
		return nil
	}

	return &Adapter{EndpointTemplate: *template}
}

// MakeRequests converts the incoming request into requests for the Generic Adapter
func (a *Adapter) MakeRequests(request *openrtb.BidRequest, reqInfo *adapters.ExtraRequestInfo) ([]*adapters.RequestData, []error) {
	var err error
	errs := make([]error, 0, len(request.Imp))
	adapterRequests := make([]*adapters.RequestData, 0, len(request.Imp))
	headers := http.Header{
		"Content-Type": {"application/json"},
		"Accept":       {"application/json"},
	}

	if len(request.Imp) <= 0 {
		return nil, []error{errors.New("No imps present in request")}
	}

	var bidderParams openrtb_ext.ExtImpGeneric

	for _, imp := range request.Imp {
		if bidderParams, err = getBidderParams(&imp); err != nil {
			return nil, []error{errors.New("Unable to parse bidder ext. " + err.Error())}
		}

		urlParams := macros.EndpointTemplateParams{Host: bidderParams.Host}
		url, err := macros.ResolveMacros(a.EndpointTemplate, urlParams)
		if err != nil {
			return nil, []error{errors.New("Unable to contruct the URL using the provided host. " + err.Error())}
		}
		url = strings.TrimSuffix(url, "/")

		reqCopy := *request
		reqCopy.Imp = []openrtb.Imp{imp}
		requestJSON, err := json.Marshal(reqCopy)
		if err != nil {
			return nil, []error{errors.New("Unable to JSON marshal the request. " + err.Error())}
		}

		adapterRequests = append(adapterRequests, &adapters.RequestData{
			Method:  "POST",
			Uri:     url + "/?impID=" + imp.ID,
			Body:    requestJSON,
			Headers: headers,
		})
	}
	return adapterRequests, errs
}

func getBidderParams(imp *openrtb.Imp) (openrtb_ext.ExtImpGeneric, error) {
	var bidderExt adapters.ExtImpBidder
	var genericExt openrtb_ext.ExtImpGeneric
	if err := json.Unmarshal(imp.Ext, &bidderExt); err != nil {
		return genericExt, &errortypes.BadInput{
			Message: fmt.Sprintf("Missing bidder ext: %s", err.Error()),
		}
	}

	if err := json.Unmarshal(bidderExt.Bidder, &genericExt); err != nil {
		return genericExt, &errortypes.BadInput{
			Message: fmt.Sprintf("Bad bidder params in a bidder ext: %s", err.Error()),
		}
	}

	if len(genericExt.Host) < 1 {
		return genericExt, &errortypes.BadInput{
			Message: "Invalid/Missing Host",
		}
	}

	return genericExt, nil
}

// MakeBids converts the bids from the Geeric Adapter to the prebid server specific bids
func (a *Adapter) MakeBids(internalRequest *openrtb.BidRequest, externalRequest *adapters.RequestData, response *adapters.ResponseData) (*adapters.BidderResponse, []error) {
	var errs []error

	if response.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if response.StatusCode == http.StatusBadRequest {
		return nil, []error{&errortypes.BadInput{
			Message: fmt.Sprintf("Unexpected status code: %d. Run with request.debug = 1 for more info", response.StatusCode),
		}}
	}

	if response.StatusCode != http.StatusOK {
		return nil, []error{&errortypes.BadServerResponse{
			Message: fmt.Sprintf("Unexpected status code: %d. Run with request.debug = 1 for more info", response.StatusCode),
		}}
	}

	var bidResp openrtb.BidResponse

	if err := json.Unmarshal(response.Body, &bidResp); err != nil {
		return nil, []error{err}
	}

	bidResponse := adapters.NewBidderResponseWithBidsCapacity(5)

	for _, sb := range bidResp.SeatBid {
		for i := range sb.Bid {
			bidType, err := getMediaTypeForImp(sb.Bid[i].ImpID, internalRequest.Imp)
			if err != nil {
				errs = append(errs, err)
			} else {
				b := &adapters.TypedBid{
					Bid:     &sb.Bid[i],
					BidType: bidType,
				}
				bidResponse.Bids = append(bidResponse.Bids, b)
			}
		}
	}
	return bidResponse, errs
}

func getMediaTypeForImp(impID string, imps []openrtb.Imp) (openrtb_ext.BidType, error) {
	mediaType := openrtb_ext.BidTypeBanner
	for _, imp := range imps {
		if imp.ID == impID {
			if imp.Banner == nil && imp.Video != nil {
				mediaType = openrtb_ext.BidTypeVideo
			}
			return mediaType, nil
		}
	}

	// This shouldnt happen. Lets handle it just incase by returning an error.
	return "", &errortypes.BadInput{
		Message: fmt.Sprintf("Failed to find impression \"%s\" ", impID),
	}
}
