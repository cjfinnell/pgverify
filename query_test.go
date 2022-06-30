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

		schemaName string
		tableName  string
		columns    map[string]column

		expectedQuery string
	}{
		{
			name:       "happy path",
			schemaName: "testSchema",
			tableName:  "testTable",
			columns: map[string]column{
				"id":      {name: "id", dataType: "uuid", constraints: []string{"this_is_a_pkey", "another constraint"}},
				"content": {name: "content", dataType: "text"},
				"when":    {name: "when", dataType: "timestamp with time zone"},
			},
			expectedQuery: formatQuery(`
				SELECT md5(string_agg(hash, ''))  
				FROM 
					(SELECT '' AS grouper, MD5(CONCAT(content::TEXT, id::TEXT, trunc(extract(epoch from when)::NUMERIC)::TEXT)) AS hash 
					FROM "testSchema"."testTable" ORDER BY 2)
				AS eachrow  GROUP BY grouper`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedQuery, buildFullHashQuery(tc.schemaName, tc.tableName, tc.columns))
		})
	}
}
