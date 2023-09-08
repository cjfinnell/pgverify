// Package for using go mod to pin specific developer tool versions.

//go:build tools
// +build tools

package tools

import (
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
)
