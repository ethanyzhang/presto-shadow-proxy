package main

import (
	"fmt"
	"net/http/httputil"
	"net/url"
	"os"
	"presto-shadow-proxy/presto"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

const (
	shadowQueryIDPrefix     = ""
	maxStatementBodyBytes   = 1 << 20 // 1MB limit for incoming statement body
	gracefulShutdownTimeout = 5 * time.Second
)

var (
	prodClient, shadowClient *presto.Client
	proxy                    *httputil.ReverseProxy
)

type ShadowProxyConfig struct {
	ProdAddress   string `yaml:"prod-address"`
	ShadowAddress string `yaml:"shadow-address"`
	ProxyPort     int    `yaml:"proxy-port"`
}

func (c *ShadowProxyConfig) Apply() error {
	var err error
	if err = c.NormalizeAndValidate(); err != nil {
		return err
	}
	log.Info().Str("prod", c.ProdAddress).Str("shadow", c.ShadowAddress).
		Int("port", c.ProxyPort).Msg("configuration loaded")

	// Build reverse proxy for the rest Presto endpoints
	proxy, err = newPrestoProxy(c.ProdAddress)
	if err != nil {
		return fmt.Errorf("failed to create new presto proxy: %w", err)
	}

	prodClient, err = presto.NewClient(c.ProdAddress, false)
	if err != nil {
		return fmt.Errorf("failed to create production Presto client: %w", err)
	}

	if c.ShadowAddress == "" {
		log.Info().Msg("shadow address not set; shadowing disabled")
	} else {
		shadowClient, err = presto.NewClient(c.ShadowAddress, false)
		if err != nil {
			return fmt.Errorf("failed to create shadow Presto client: %w", err)
		}
	}

	return nil
}

func (c *ShadowProxyConfig) NormalizeAndValidate() error {
	var prodURL, shadowURL *url.URL
	var err error
	c.ProdAddress, prodURL, err = normalizeAddress("prod-address", c.ProdAddress)
	if err != nil {
		return err
	}
	if c.ShadowAddress != "" {
		c.ShadowAddress, shadowURL, err = normalizeAddress("shadow-address", c.ShadowAddress)
		if err != nil {
			return err
		}
		// Ensure they are not pointing to the same host:port (case-insensitive)
		if strings.EqualFold(prodURL.Host, shadowURL.Host) && prodURL.Scheme == shadowURL.Scheme {
			return fmt.Errorf("prod-address and shadow-address must not be the same (%s)", c.ProdAddress)
		}
	}

	if c.ProxyPort <= 0 || c.ProxyPort > 65535 {
		c.ProxyPort = 8080
	}
	return nil
}

func ReadAndDeserializeYaml[T any](filePath string) (*T, error) {
	if filePath == "" {
		return nil, fmt.Errorf("file path is empty")
	}
	if fileBytes, ioErr := os.ReadFile(filePath); ioErr != nil {
		return nil, fmt.Errorf("failed to read file: %w", ioErr)
	} else {
		target := new(T)
		if err := yaml.Unmarshal(fileBytes, target); err != nil {
			return nil, fmt.Errorf("failed to deserialize yaml file: %w", err)
		}
		return target, nil
	}
}

func normalizeAddress(name, addr string) (string, *url.URL, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", nil, fmt.Errorf("%s is required", name)
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	// Remove trailing slash (idempotent) for stable comparison
	addr = strings.TrimRight(addr, "/")
	u, err := url.Parse(addr)
	if err != nil || u.Host == "" {
		return "", nil, fmt.Errorf("invalid %s: %s", name, addr)
	}
	return addr, u, nil
}
