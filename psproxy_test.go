package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"presto-shadow-proxy/presto"
)

func TestHandlePrestoStatementWithoutNextURI(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origConfig := config
	origProdClient := prodClient
	origShadowClient := shadowClient
	defer func() {
		config = origConfig
		prodClient = origProdClient
		shadowClient = origShadowClient
	}()

	prodQueryID := "20250101_000000_00001_test"
	prodResponse := presto.QueryResults{
		Id:       prodQueryID,
		InfoUri:  "http://prod/query/" + prodQueryID,
		Stats:    presto.StatementStats{State: "FINISHED"},
		Warnings: []presto.Warning{},
	}

	prodServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method for production server: %s", r.Method)
		}
		if r.URL.Path != "/v1/statement" {
			t.Fatalf("unexpected path for production server: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(prodResponse); err != nil {
			t.Fatalf("failed to encode production response: %v", err)
		}
	}))
	defer prodServer.Close()

	shadowHit := make(chan struct{}, 1)
	shadowResponse := presto.QueryResults{
		Id:       "shadow-" + prodQueryID,
		InfoUri:  "http://shadow/query/" + prodQueryID,
		Stats:    presto.StatementStats{State: "FINISHED"},
		Warnings: []presto.Warning{},
	}

	shadowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("unexpected method for shadow server: %s", r.Method)
		}
		if !strings.HasPrefix(r.URL.Path, "/v1/statement/") {
			t.Fatalf("unexpected path for shadow server: %s", r.URL.Path)
		}
		if slug := r.URL.Query().Get("slug"); slug != "" {
			t.Fatalf("expected empty slug, got %q", slug)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(shadowResponse); err != nil {
			t.Fatalf("failed to encode shadow response: %v", err)
		}
		shadowHit <- struct{}{}
	}))
	defer shadowServer.Close()

	var err error
	prodClient, err = presto.NewClient(prodServer.URL, false)
	if err != nil {
		t.Fatalf("failed to create production client: %v", err)
	}
	shadowClient, err = presto.NewClient(shadowServer.URL, false)
	if err != nil {
		t.Fatalf("failed to create shadow client: %v", err)
	}

	config = &ShadowProxyConfig{
		ProdAddress:   prodServer.URL,
		ShadowAddress: shadowServer.URL,
	}

	body := "SELECT 1"
	req := httptest.NewRequest(http.MethodPost, "/v1/statement", strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = req

	handlePrestoStatement(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status code: got %d, want %d", recorder.Code, http.StatusOK)
	}

	var got presto.QueryResults
	if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if got.Id != prodResponse.Id {
		t.Fatalf("unexpected query id: got %s, want %s", got.Id, prodResponse.Id)
	}
	if got.NextUri != nil {
		t.Fatalf("expected nil nextUri, got %v", *got.NextUri)
	}

	select {
	case <-shadowHit:
	case <-time.After(time.Second):
		t.Fatal("shadow query was not executed")
	}
}
