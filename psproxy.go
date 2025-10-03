package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"presto-shadow-proxy/presto"
	"strings"
	"time"

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func psproxy(cmd *cobra.Command, _ []string) error {
	if err := config.Apply(); err != nil {
		return err
	}

	engine := gin.Default()
	// Config update endpoint (could add auth)
	engine.PATCH("/proxy/config", handleConfigPatch)
	// Config read endpoint
	engine.GET("/proxy/config", func(c *gin.Context) {
		c.JSON(http.StatusOK, config)
	})
	// Custom handling for POST /v1/statement (not proxied directly)
	engine.POST("/v1/statement", gzip.Gzip(gzip.DefaultCompression), handlePrestoStatement)
	// Proxy everything else using NoRoute to avoid wildcard conflicts with explicit routes
	engine.NoRoute(func(c *gin.Context) {
		proxy.ServeHTTP(c.Writer, c.Request)
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", config.ProxyPort),
		Handler: engine,
	}

	// Start server in goroutine
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	// Wait for either server error or context cancellation
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-cmd.Context().Done():
		log.Info().Msg("Shutting down shadow proxy...")
	}

	// Graceful shutdown with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}

	// Drain potential server error after shutdown (not strictly required, but keeps log clean)
	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	default:
	}
	return nil
}

func handleConfigPatch(c *gin.Context) {
	var newConfig ShadowProxyConfig
	if config != nil {
		newConfig = *config
	}

	if err := c.ShouldBindJSON(&newConfig); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid config format"})
		return
	}

	if err := newConfig.Apply(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	config = &newConfig
	c.Status(http.StatusOK)
}

// handlePrestoStatement handles POST /v1/statement requests.
// 1. Reads SQL body with size limit
// 2. Sends query to production cluster
// 3. Responds to client with production results
// 4. Asynchronously mirrors the query to shadow cluster with a pre-minted ID
func handlePrestoStatement(c *gin.Context) {
	// Enforce body size limit
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxStatementBodyBytes)
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		if errors.Is(err, http.ErrBodyReadAfterClose) || strings.Contains(err.Error(), "request body too large") {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "statement body too large"})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		}
		return
	}
	query := string(bodyBytes)

	clientHeaders := c.Request.Header.Clone()
	copyHeader := func(req *http.Request) {
		for k, vals := range clientHeaders {
			// Use Set for headers expected to be singular
			switch k {
			case presto.UserHeader, presto.CatalogHeader, presto.SchemaHeader, presto.TimeZoneHeader, "Content-Type":
				if len(vals) > 0 {
					req.Header.Set(k, vals[0])
				}
			default:
				for _, v := range vals {
					req.Header.Add(k, v)
				}
			}
		}
	}

	qr, resp, err := prodClient.Query(c.Request.Context(), query, copyHeader)
	if err != nil {
		log.Error().Err(err).Msg("production query failed")
	}

	// Copy all response headers from Presto to the client
	for k, v := range resp.Header {
		for _, vv := range v {
			c.Writer.Header().Add(k, vv)
		}
	}
	c.Status(resp.StatusCode)
	if err = json.NewEncoder(c.Writer).Encode(qr); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to encode response"})
	}

	// Shadow execution (optional)
	if config.ShadowAddress != "" {
		slug := ""
		if qr.NextUri != nil {
			if u, err := url.Parse(*qr.NextUri); err != nil {
				log.Error().Err(err).Msg("failed to parse production query slug")
			} else {
				slug = u.Query().Get("slug")
			}
		}
		go func(prodQueryID string) {
			ctx := context.Background()
			shadowID := shadowQueryIDPrefix + prodQueryID
			start := time.Now()
			sqr, _, err := shadowClient.QueryWithPreMintedID(ctx, query, shadowID, slug, copyHeader)
			if err != nil {
				log.Error().Str("shadow_query_id", shadowID).Str("prod_query_id", prodQueryID).
					Err(err).Msg("shadow query failed")
				return
			}
			err = sqr.Drain(ctx, nil)
			if err != nil {
				log.Error().Str("shadow_query_id", shadowID).Str("prod_query_id", prodQueryID).
					Err(err).Msg("failed to drain results for shadow query")
				return
			}
			log.Debug().Str("shadow_query_id", shadowID).Str("prod_query_id", prodQueryID).
				Dur("latency", time.Since(start)).Msg("shadow query completed")
		}(qr.Id)
	}
}

func newPrestoProxy(raw string) (*httputil.ReverseProxy, error) {
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}

	p := httputil.NewSingleHostReverseProxy(u)
	orig := p.Director
	p.Director = func(r *http.Request) {
		orig(r)
		r.Host = u.Host
	}
	p.ErrorHandler = func(rw http.ResponseWriter, r *http.Request, e error) {
		http.Error(rw, "upstream error: "+e.Error(), http.StatusBadGateway)
		log.Error().Err(e).Msg("Upstream error")
	}

	return p, nil
}
