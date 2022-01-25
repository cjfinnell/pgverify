package pgverify

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

const (
	StrategyFull    = "full"
	StrategyBookend = "bookend"
)

type Config struct {
	IncludeTables  []string
	ExcludeTables  []string
	IncludeSchemas []string
	ExcludeSchemas []string

	Strategy     string
	BookendLimit int

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
		WithStrategy(StrategyFull),
		WithBookendLimit(1000),
	}
	for _, opt := range append(defaultOpts, opts...) {
		opt.apply(&c)
	}
	return c
}

func (c Config) Validate() error {
	switch c.Strategy {
	case StrategyFull:
	case StrategyBookend:
	default:
		return fmt.Errorf("invalid strategy: %s", c.Strategy)
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

func WithStrategy(strategy string) optionFunc {
	return func(c *Config) {
		c.Strategy = strategy
	}
}

func WithBookendLimit(limit int) optionFunc {
	return func(c *Config) {
		c.BookendLimit = limit
	}
}

func WithAliases(aliases []string) optionFunc {
	return func(c *Config) {
		c.Aliases = aliases
	}
}
