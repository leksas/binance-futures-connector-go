package binance_futures_connector

import (
	"context"
	"net/http"
)

// Binance If New User endpoint (GET /fapi/v1/apiReferral/ifNewUser)
type IfNewUserService struct {
	c        *Client
	brokerID string
	typ      *int
}

func (s *IfNewUserService) BrokerID(brokerID string) *IfNewUserService {
	s.brokerID = brokerID
	return s
}

func (s *IfNewUserService) Type(typ int) *IfNewUserService {
	s.typ = &typ
	return s
}

func (s *IfNewUserService) Do(ctx context.Context, opts ...RequestOption) (res *IfNewUserResponse, err error) {
	r := &request{
		method:   http.MethodGet,
		endpoint: "/fapi/v1/apiReferral/ifNewUser",
		secType:  secTypeSigned,
	}
	r.setParam("brokerId", s.brokerID)
	if s.typ != nil {
		r.setParam("type", *s.typ)
	}
	data, err := s.c.callAPI(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	err = Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

type IfNewUserResponse struct {
	BrokerID      string `json:"brokerId"`
	RebateWorking bool   `json:"rebateWorking"`
	IfNewUser     bool   `json:"ifNewUser"`
}
