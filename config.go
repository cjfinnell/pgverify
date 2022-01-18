package pgverify

import log "github.com/sirupsen/logrus"

type Config struct {
	IncludeTables  []string
	ExcludeTables  []string
	IncludeSchemas []string
	ExcludeSchemas []string

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
	}
	for _, opt := range append(defaultOpts, opts...) {
		opt.apply(&c)
	}
	return c
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
