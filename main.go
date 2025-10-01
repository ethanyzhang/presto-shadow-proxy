package main

import (
	"context"
	"io"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "psproxy",
		Short: "Presto Shadow Proxy",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if config, err = ReadAndDeserializeYaml[ShadowProxyConfig](configPath); err != nil {
				return err
			}
			return nil
		},
		RunE: psproxy,
	}
	configPath string
	config     *ShadowProxyConfig
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c",
		"shadow-proxy.yaml", "Path to configuration file")
}

func main() {
	gin.SetMode(gin.ReleaseMode)
	// Redirect Gin's default output to /dev/null to suppress logging
	gin.DefaultWriter = io.Discard

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		log.Fatal().Err(err).Msg("Encountered error")
	}
}
