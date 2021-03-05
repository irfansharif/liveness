package main

import (
	"log"

	"github.com/spf13/cobra"
)

func mustGetStringFlag(cmd *cobra.Command, name string) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	return val
}

func mustGetIntFlag(cmd *cobra.Command, name string) int {
	val, err := cmd.Flags().GetInt(name)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}
	return val
}
