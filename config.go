package pgverify

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

const (
	TestModeFull = "full"

	TestModeBookend             = "bookend"
	TestModeBookendDefaultLimit = 1000

	TestModeSparse           = "sparse"
	TestModeSparseDefaultMod = 10
)

type Config struct {
	IncludeTables  []string
	ExcludeTables  []string
	IncludeSchemas []string
	ExcludeSchemas []string

	TestModes    []string
	BookendLimit int
	SparseMod    int

	Aliases []string

	Logger *log.Logger
}

// Option interface used for setting optional config properties.
type Option interface {
	apply(*Config)
}

type optionFunc func(*Config)

func (o optionFunc) apply(c *Config) {
	o(c)
}

func NewConfig(opts ...Option) Config {
	c := Config{}
	defaultOpts := []Option{
		WithLogger(log.StandardLogger()),
		WithTests(TestModeFull),
		WithBookendLimit(TestModeBookendDefaultLimit),
		WithSparseMod(TestModeSparseDefaultMod),
	}
	for _, opt := range append(defaultOpts, opts...) {
		opt.apply(&c)
	}
	return c
}

func (c Config) Validate() error {
	for _, mode := range c.TestModes {
		switch mode {
		case TestModeFull:
		case TestModeBookend:
		case TestModeSparse:
		default:
			return fmt.Errorf("invalid strategy: %s", c.TestModes)
		}
	}
	return nil
}

func WithLogger(logger *log.Logger) optionFunc {
	return func(c *Config) {
		c.Logger = logger
	}
}

func ExcludeSchemas(schemas ...string) optionFunc {
	return func(c *Config) {
		c.ExcludeSchemas = schemas
	}
}

func IncludeSchemas(schemas ...string) optionFunc {
	return func(c *Config) {
		c.IncludeSchemas = schemas
	}
}

func ExcludeTables(tables ...string) optionFunc {
	return func(c *Config) {
		c.ExcludeTables = tables
	}
}

func IncludeTables(tables ...string) optionFunc {
	return func(c *Config) {
		c.IncludeTables = tables
	}
}

func WithTests(testModes ...string) optionFunc {
	return func(c *Config) {
		c.TestModes = testModes
	}
}

func WithBookendLimit(limit int) optionFunc {
	return func(c *Config) {
		c.BookendLimit = limit
	}
}

func WithSparseMod(mod int) optionFunc {
	return func(c *Config) {
		c.SparseMod = mod
	}
}

func WithAliases(aliases []string) optionFunc {
	return func(c *Config) {
		c.Aliases = aliases
	}
}
