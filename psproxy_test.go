package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
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
		assert.Equal(t, http.MethodPost, r.Method, "unexpected method for production server")
		assert.Equal(t, "/v1/statement", r.URL.Path, "unexpected path for production server")
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(prodResponse)
		assert.NoError(t, err, "failed to encode production response")
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
		assert.Equal(t, http.MethodPut, r.Method, "unexpected method for shadow server")
		assert.True(t, strings.HasPrefix(r.URL.Path, "/v1/statement/"), "unexpected path for shadow server: %s", r.URL.Path)
		assert.Empty(t, r.URL.Query().Get("slug"), "expected empty slug")
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(shadowResponse)
		assert.NoError(t, err, "failed to encode shadow response")
		shadowHit <- struct{}{}
	}))
	defer shadowServer.Close()

	var err error
	prodClient, err = presto.NewClient(prodServer.URL, false)
	assert.NoError(t, err, "failed to create production client")
	shadowClient, err = presto.NewClient(shadowServer.URL, false)
	assert.NoError(t, err, "failed to create shadow client")

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

	assert.Equal(t, http.StatusOK, recorder.Code, "unexpected status code")

	var got presto.QueryResults
	err = json.Unmarshal(recorder.Body.Bytes(), &got)
	assert.NoError(t, err, "failed to decode response body")

	assert.Equal(t, prodResponse.Id, got.Id, "unexpected query id")
	assert.Nil(t, got.NextUri, "expected nil nextUri")

	select {
	case <-shadowHit:
	case <-time.After(time.Second):
		assert.Fail(t, "shadow query was not executed")
	}
}

func TestPatchConfigInvalidUpdateDoesNotMutateCurrentConfig(t *testing.T) {
	gin.SetMode(gin.TestMode)

	origConfig := config
	origProdClient := prodClient
	origShadowClient := shadowClient
	origProxy := proxy
	defer func() {
		config = origConfig
		prodClient = origProdClient
		shadowClient = origShadowClient
		proxy = origProxy
	}()

	config = &ShadowProxyConfig{
		ProdAddress:   "http://prod.example.com",
		ShadowAddress: "http://shadow.example.com",
		ProxyPort:     8080,
	}
	err := config.Apply()
	assert.NoError(t, err, "failed to apply initial config")

	initialConfigPtr := config
	initialConfigValue := *config
	initialProdClient := prodClient
	initialShadowClient := shadowClient
	initialProxy := proxy

	engine := gin.New()
	engine.PATCH("/proxy/config", handleConfigPatch)

	body := `{"ProdAddress":""}`
	req := httptest.NewRequest(http.MethodPatch, "/proxy/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code, "unexpected status code")

	assert.Same(t, initialConfigPtr, config, "config pointer changed on invalid update")
	assert.Equal(t, initialConfigValue, *config, "config values changed on invalid update")
	assert.Same(t, initialProdClient, prodClient, "prodClient changed on invalid update")
	assert.Same(t, initialShadowClient, shadowClient, "shadowClient changed on invalid update")
	assert.Same(t, initialProxy, proxy, "proxy changed on invalid update")
}
