package dbverify

type Config struct {
	IncludeTables  []string
	ExcludeTables  []string
	IncludeSchemas []string
	ExcludeSchemas []string
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
	for _, opt := range opts {
		opt.apply(&c)
	}
	return c
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
