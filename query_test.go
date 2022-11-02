//nolint:testpackage // unit test for internals, *_test pattern not appropriate
package pgverify

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildGetTablesQuery(t *testing.T) {
	for _, tc := range []struct {
		name string

		includeSchemas []string
		excludeSchemas []string
		includeTables  []string
		excludeTables  []string

		expectedQuery string
	}{
		{
			name:          "no filters",
			expectedQuery: "SELECT table_schema, table_name FROM information_schema.tables",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedQuery, buildGetTablesQuery(tc.includeSchemas, tc.excludeSchemas, tc.includeTables, tc.excludeTables))
		})
	}
}

func TestBuildFullHashQuery(t *testing.T) {
	for _, tc := range []struct {
		name string

		config        Config
		schemaName    string
		tableName     string
		primaryColumn column
		columns       []column

		expectedQuery string
	}{
		{
			name:          "happy path",
			config:        Config{TimestampPrecision: TimestampPrecisionMilliseconds},
			schemaName:    "testSchema",
			tableName:     "testTable",
			primaryColumn: column{name: "id", dataType: "uuid", constraints: []string{"PRIMARY KEY", "another constraint"}},
			columns: []column{
				{name: "content", dataType: "text"},
				{name: "when", dataType: "timestamp with time zone"},
			},
			expectedQuery: formatQuery(`
            SELECT md5(string_agg(hash, ''))
            FROM
                (SELECT '' AS grouper, MD5(CONCAT(content::TEXT, extract(epoch from date_trunc('milliseconds', when))::TEXT)) AS hash
                FROM "testSchema"."testTable" ORDER BY id) AS eachrow GROUP BY grouper`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedQuery, buildFullHashQuery(tc.config, tc.schemaName, tc.tableName, tc.primaryColumn, tc.columns))
		})
	}
}
