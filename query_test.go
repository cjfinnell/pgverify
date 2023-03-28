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

		config                   Config
		schemaName               string
		tableName                string
		columns                  []column
		primaryColumnNamesString string

		expectedQuery string
	}{
		{
			name:       "happy path",
			config:     Config{TimestampPrecision: TimestampPrecisionMilliseconds},
			schemaName: "testSchema",
			tableName:  "testTable",
			columns: []column{
				{name: "id", dataType: "uuid", constraints: []string{"PRIMARY KEY", "another constraint"}},
				{name: "content", dataType: "text"},
				{name: "when", dataType: "timestamp with time zone"},
			},
			primaryColumnNamesString: "id",
			expectedQuery: formatQuery(`
            SELECT md5(string_agg(hash, ''))
            FROM
                (SELECT '' AS grouper, MD5(CONCAT((extract(epoch from date_trunc('milliseconds', when))::DECIMAL * 1000000)::BIGINT::TEXT, content::TEXT, id::TEXT)) AS hash, CONCAT(id::TEXT) as primary_key
                FROM "testSchema"."testTable" ORDER BY CONCAT(id::TEXT)) AS eachrow GROUP BY grouper, primary_key ORDER BY primary_key`),
		},
		{
			name:       "multi-column primary key",
			config:     Config{TimestampPrecision: TimestampPrecisionMilliseconds},
			schemaName: "testSchema",
			tableName:  "testTable",
			columns: []column{
				{name: "id", dataType: "uuid", constraints: []string{"PRIMARY KEY", "another constraint"}},
				{name: "content", dataType: "text", constraints: []string{"PRIMARY KEY"}},
				{name: "when", dataType: "timestamp with time zone"},
			},
			primaryColumnNamesString: "id, content",
			expectedQuery: formatQuery(`
            SELECT md5(string_agg(hash, ''))
            FROM
                (SELECT '' AS grouper, MD5(CONCAT((extract(epoch from date_trunc('milliseconds', when))::DECIMAL * 1000000)::BIGINT::TEXT, content::TEXT, id::TEXT)) AS hash, CONCAT(content::TEXT, id::TEXT) as primary_key
                FROM "testSchema"."testTable" ORDER BY CONCAT(content::TEXT, id::TEXT)) AS eachrow GROUP BY grouper, primary_key ORDER BY primary_key`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedQuery, buildFullHashQuery(tc.config, tc.schemaName, tc.tableName, tc.columns))
		})
	}
}

func TestBuildSparseHashQuery(t *testing.T) {
	for _, tc := range []struct {
		name string

		config        Config
		schemaName    string
		tableName     string
		columns       []column
		expectedQuery string
	}{
		{
			name:       "happy path",
			config:     Config{TimestampPrecision: TimestampPrecisionMilliseconds},
			schemaName: "testSchema",
			tableName:  "testTable",
			columns: []column{
				{name: "id", dataType: "uuid", constraints: []string{"PRIMARY KEY", "another constraint"}},
				{name: "content", dataType: "text"},
				{name: "when", dataType: "timestamp with time zone"},
			},
			expectedQuery: formatQuery(`
            SELECT md5(string_agg(hash, ''))
            FROM
                ( SELECT '' AS grouper, MD5(CONCAT((extract(epoch from date_trunc('milliseconds', when))::DECIMAL * 1000000)::BIGINT::TEXT, content::TEXT, id::TEXT)) AS hash
                FROM "testSchema"."testTable" 
				WHERE id in ( 
					SELECT id FROM "testSchema"."testTable" 
					WHERE ('x' || substr(md5(CONCAT(id::TEXT)),1,16))::bit(64)::bigint % 10 = 0 ) 
					ORDER BY CONCAT(id::TEXT)
				) 
				AS eachrow GROUP BY grouper`),
		},
		{
			name:       "multi-column primary key",
			config:     Config{TimestampPrecision: TimestampPrecisionMilliseconds},
			schemaName: "testSchema",
			tableName:  "testTable",
			columns: []column{
				{name: "id", dataType: "uuid", constraints: []string{"PRIMARY KEY", "another constraint"}},
				{name: "content", dataType: "text", constraints: []string{"PRIMARY KEY"}},
				{name: "when", dataType: "timestamp with time zone"},
			},
			expectedQuery: formatQuery(`
            SELECT md5(string_agg(hash, ''))
            FROM
                ( SELECT '' AS grouper, MD5(CONCAT((extract(epoch from date_trunc('milliseconds', when))::DECIMAL * 1000000)::BIGINT::TEXT, content::TEXT, id::TEXT)) AS hash
                FROM "testSchema"."testTable" 
				WHERE content in ( 
					SELECT content FROM "testSchema"."testTable" 
					WHERE ('x' || substr(md5(CONCAT(content::TEXT, id::TEXT)),1,16))::bit(64)::bigint % 10 = 0
				) AND id in ( 
					SELECT id FROM "testSchema"."testTable" 
					WHERE ('x' || substr(md5(CONCAT(content::TEXT, id::TEXT)),1,16))::bit(64)::bigint % 10 = 0
				) ORDER BY CONCAT(content::TEXT, id::TEXT) )
				AS eachrow GROUP BY grouper`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedQuery, buildSparseHashQuery(tc.config, tc.schemaName, tc.tableName, tc.columns, 10))
		})
	}
}
