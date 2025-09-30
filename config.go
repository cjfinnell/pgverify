package pgverify

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

const (
	// TestModeFull is a full test is the default test mode. It is the only test
	// mode that checks all of the rows of a given table, guaranteeing equivalent
	// values between targets.
	TestModeFull = "full"

	// TestModeBookend is a bookend test is similar to the full test mode, but it
	// only checks a certain number of rows from the beginning and end of the table,
	// sorted by primary key.
	TestModeBookend = "bookend"
	// TestModeBookendDefaultLimit is the number of rows checked in the bookend
	// test mode is configurable.
	TestModeBookendDefaultLimit = 1000

	// TestModeSparse is a sparse test checks a deterministic subset of the rows in a table.
	TestModeSparse = "sparse"
	// TestModeSparseDefaultMod is the number of rows checked in the sparse test
	// mode is configurable, equalling approximately 1/mod of the total.
	TestModeSparseDefaultMod = 10

	// TestModeRowCount is a rowcount test simply compares table row counts between targets.
	TestModeRowCount = "rowcount"

	TimestampPrecisionMilliseconds = "milliseconds"
)

// Config represents the configuration for running a verification.
type Config struct {
	// Filters for which schemas and tables to run verification tests on.
	// Exclude overrides Include.
	IncludeTables  []string
	ExcludeTables  []string
	IncludeSchemas []string
	ExcludeSchemas []string
	IncludeColumns []string
	ExcludeColumns []string

	// TestModes is a list of test modes to run, executed in order.
	TestModes []string
	// BookendLimit is the number of rows to include when running a bookend test.
	BookendLimit int
	// SparseMod is used in the sparse test mode to deterministically select a
	// subset of rows, approximately 1/mod of the total.
	SparseMod int
	// HashPrimaryKeys is a flag that determines whether or not to hash the values
	// of primary keys before using them for ordering. This is useful when the
	// primary keys contain TEXT that is sorted differently between engines.
	// May impact performance.
	HashPrimaryKeys bool

	// Aliases is a list of aliases to use for the target databases in reporting
	// output. Is ignored if the number of aliases is not equal to the number of
	// supplied targets.
	Aliases []string

	// TimestampPrecision is the precision level to use when comparing timestamp values.
	TimestampPrecision string

	Logger log.FieldLogger
}

// Option interface used for setting optional config properties.
type Option interface {
	apply(c *Config)
}

type optionFunc func(*Config)

func (o optionFunc) apply(c *Config) {
	o(c)
}

// NewConfig returns a new Config with default values overridden
// by the supplied Options.
func NewConfig(opts ...Option) Config {
	c := Config{}
	defaultOpts := []Option{
		WithLogger(log.StandardLogger()),
		WithTests(TestModeFull),
		WithBookendLimit(TestModeBookendDefaultLimit),
		WithSparseMod(TestModeSparseDefaultMod),
		WithTimestampPrecision(TimestampPrecisionMilliseconds),
	}

	for _, opt := range append(defaultOpts, opts...) {
		opt.apply(&c)
	}

	return c
}

// Validate checks that the configuration contains valid values.
func (c Config) Validate() error {
	for _, mode := range c.TestModes {
		switch mode {
		case TestModeBookend:
		case TestModeFull:
		case TestModeRowCount:
		case TestModeSparse:
		default:
			return fmt.Errorf("invalid strategy: %s", c.TestModes)
		}
	}

	return nil
}

// WithLogger sets the logger configuration.
func WithLogger(logger log.FieldLogger) optionFunc {
	return func(c *Config) {
		c.Logger = logger
	}
}

// ExcludeSchemas sets the exclude schemas configuration.
func ExcludeSchemas(schemas ...string) optionFunc {
	return func(c *Config) {
		c.ExcludeSchemas = schemas
	}
}

// IncludeSchemas sets the include schemas configuration.
func IncludeSchemas(schemas ...string) optionFunc {
	return func(c *Config) {
		c.IncludeSchemas = schemas
	}
}

// ExcludeTables sets the exclude tables configuration.
func ExcludeTables(tables ...string) optionFunc {
	return func(c *Config) {
		c.ExcludeTables = tables
	}
}

// IncludeTables sets the include tables configuration.
func IncludeTables(tables ...string) optionFunc {
	return func(c *Config) {
		c.IncludeTables = tables
	}
}

// ExcludeColumns sets the exclude columns configuration.
func ExcludeColumns(columns ...string) optionFunc {
	return func(c *Config) {
		c.ExcludeColumns = columns
	}
}

// IncludeColumns sets the include columns configuration.
func IncludeColumns(columns ...string) optionFunc {
	return func(c *Config) {
		c.IncludeColumns = columns
	}
}

// WithTests defines the tests to run.
func WithTests(testModes ...string) optionFunc {
	return func(c *Config) {
		c.TestModes = testModes
	}
}

// WithBookendLimit sets the bookend limit configuration used in
// the bookend test mode.
func WithBookendLimit(limit int) optionFunc {
	return func(c *Config) {
		c.BookendLimit = limit
	}
}

// WithSparseMod sets the sparse mod configuration used in
// the sparse test mode.
func WithSparseMod(mod int) optionFunc {
	return func(c *Config) {
		c.SparseMod = mod
	}
}

// WithAliases sets the aliases for the target databases. Is ignored if not equal
// to the number of targets.
func WithAliases(aliases []string) optionFunc {
	return func(c *Config) {
		c.Aliases = aliases
	}
}

// WithTimestampPrecision sets the precision level to use when comparing
// timestamp values. This can be useful for addressing precision differences
// between engines, i.e. millisecond vs. microsecond.
func WithTimestampPrecision(precision string) optionFunc {
	return func(c *Config) {
		c.TimestampPrecision = precision
	}
}

// WithHashPrimaryKeys configures the verifier to hash primary keys before
// ordering results. This is useful when the primary keys contain TEXT that is
// sorted differently between engines.
// May impact performance.
func WithHashPrimaryKeys() optionFunc {
	return func(c *Config) {
		c.HashPrimaryKeys = true
	}
}
