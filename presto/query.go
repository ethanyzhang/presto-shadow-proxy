package presto

import (
	"context"
	"fmt"
	"net/http"
)

func (c *Client) requestQueryResults(ctx context.Context, req *http.Request) (*QueryResults, *http.Response, error) {
	qr := new(QueryResults)
	resp, err := c.Do(ctx, req, qr)
	if err != nil {
		return nil, resp, err
	}
	qr.client = c
	if qr.Error != nil {
		return qr, resp, qr.Error
	}
	return qr, resp, nil
}

func (c *Client) Query(ctx context.Context, query string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("POST",
		"v1/statement", query, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

func (c *Client) QueryWithPreMintedID(ctx context.Context, query, queryId, slug string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	if queryId == "" {
		return c.Query(ctx, query, opts...)
	}
	req, err := c.NewRequest("PUT",
		fmt.Sprintf("v1/statement/%s?slug=%s", queryId, slug), query, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

func (c *Client) FetchNextBatch(ctx context.Context, nextUri string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("GET",
		nextUri, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

func (c *Client) CancelQuery(ctx context.Context, nextUri string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("DELETE",
		nextUri, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}
