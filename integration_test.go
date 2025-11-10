package pgverify_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cjfinnell/pgverify"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/cockroachdb"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var containerNameRegex = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func waitForDBReady(t *testing.T, ctx context.Context, config *pgx.ConnConfig) bool {
	t.Helper()

	connected := false

	for range 30 {
		conn, err := pgx.ConnectConfig(ctx, config)
		if err == nil {
			connected = true

			conn.Close(ctx)

			break
		}

		time.Sleep(2 * time.Second)
	}

	return connected
}

func createContainer(t *testing.T, ctx context.Context, image string) (*pgx.ConnConfig, error) {
	t.Helper()

	containerName := strings.ToLower(containerNameRegex.ReplaceAllString("pgverify-int-test-"+image, "-"))

	switch {
	case strings.HasPrefix(image, "cockroach"):
		cockroachdbContainer, err := cockroachdb.Run(ctx, image, testcontainers.WithName(containerName), cockroachdb.WithInsecure())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(cockroachdbContainer)) })

		return cockroachdbContainer.ConnectionConfig(ctx)
	case strings.HasPrefix(image, "postgres"):
		postgresContainer, err := postgres.Run(ctx, image, testcontainers.WithName(containerName))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(postgresContainer)) })

		connString, err := postgresContainer.ConnectionString(ctx)
		require.NoError(t, err)

		return pgx.ParseConfig(connString)
	default:
		return nil, errors.New("not implemented")
	}
}

func calculateRowCount(columnTypes map[string][]string) int {
	rowCount := 50 // Minimum
	for _, columnType := range columnTypes {
		if rowCount < len(columnType) {
			rowCount = len(columnType)
		}
	}

	return rowCount
}

func TestVerifyData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	for _, tc := range []struct {
		name         string
		psqlVersions []string
		crdbVersions []string
	}{
		{
			name:         "Latest",
			psqlVersions: []string{"latest"},
			crdbVersions: []string{"latest"}, // cockroach cloud
		},
		{
			name: "Full",
			psqlVersions: []string{
				"10",
				"11",
				"12",
				"13",
				"14",
				"15",
				"16",
				"17",
				"18",
			},
			crdbVersions: []string{
				"latest-v22.2",
				"latest-v23.2",
				"latest-v24.3",
				"latest-v25.3",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			dbs := make([]string, 0, len(tc.psqlVersions)+len(tc.crdbVersions))

			for _, tag := range tc.psqlVersions {
				dbs = append(dbs, "postgres:"+tag)
			}

			for _, tag := range tc.crdbVersions {
				dbs = append(dbs, "cockroachdb/cockroach:"+tag)
			}

			columnTypes := map[string][]string{
				"boolean":   {"true", "false"},
				"bytea":     {fmt.Sprintf("'%s'", hex.EncodeToString([]byte("convert this content to bytes")))},
				"bit(1)":    {"'1'", "'0'"},
				"varbit(3)": {"'0'", "'1'", "'101'", "'010'"},

				"bigint[]":         {"'{602213950000000000, -1}'", "'{}'", "ARRAY[]::bigint[]"},
				"integer":          {"0", "123979", "-23974"},
				"double precision": {"69.123987", "-69.123987"},
				"numeric":          {"0", "123.456", "-123.456"},
				"decimal":          {"0", "123.456", "-123.456"},

				"text":                  {`'foo'`, `'bar'`, `''`, `'something that is much longer but without much other complication'`},
				"uuid":                  {fmt.Sprintf("'%s'", uuid.New().String())},
				`character varying(64)`: {`'more string stuff'`},
				"text[]":                {`'{"foo", "bar"}'`, `'{"baz", "qux"}'`, `'{"foo", "bar", "baz", "qux"}'`, `ARRAY[]::text[]`, `'{}'`},

				"jsonb": {`'{}'`, `'{"foo":["bar","baz"]}'`, `'{"foo": "bar"}'`, `'{"foo": "bar", "baz": "qux"}'`, `'{"for sure?": true, "has numbers": 123.456, "this is": ["some", "json", "blob"]}'`},
				"json":  {`'{}'`, `'{"foo":["bar","baz"]}'`, `'{"foo": "bar"}'`, `'{"foo": "bar", "baz": "qux"}'`, `'{"for sure?": true, "has numbers": 123.456, "this is": ["some", "json", "blob"]}'`},

				"date":                        {`'2020-12-31'`},
				"timestamp with time zone":    {`'2020-12-31 23:59:59 -8:00'`, `'2022-06-08 20:03:06.957223+00'`}, // hashes differently for psql/crdb, convert to epoch when hashing
				"timestamp without time zone": {`'2020-12-31 23:59:59'`},
			}
			keys := make([]string, len(columnTypes))
			keysWithTypes := make([]string, len(columnTypes))
			sortedTypes := make([]string, len(columnTypes))
			i := 0

			for k := range columnTypes {
				// Create sanitized column name from type
				cleanName := strings.ReplaceAll("col_"+k, " ", "_")
				for _, char := range "()[]" {
					cleanName = strings.ReplaceAll(cleanName, string(char), "X")
				}

				sortedTypes[i] = k
				keys[i] = cleanName
				keysWithTypes[i] = strings.Join([]string{keys[i], k}, " ")
				i++
			}

			sort.Strings(keys)
			sort.Strings(keysWithTypes)
			sort.Strings(sortedTypes)

			tableNames := []string{"testtable1", "testTABLE_multi_col_2", "testtable3", "test_stringkey_table4", "test_column_names"}
			createTableQueryBase := fmt.Sprintf("( id INT DEFAULT 0 NOT NULL, zid INT DEFAULT 0 NOT NULL, sid TEXT NOT NULL, ignored TIMESTAMP WITH TIME ZONE DEFAULT NOW(), %s);", strings.Join(keysWithTypes, ", "))

			rowCount := calculateRowCount(columnTypes)
			insertDataQueryBase := `(id, zid, sid,` + strings.Join(keys, ", ") + `) VALUES `
			valueClauses := make([]string, 0, rowCount)

			// Modulo-cycle through prefixes to re-create ORDER BY issue
			textPKeyPrefixes := []string{"A", "AA", "a", "aa", "A-A", "a-a"}

			for rowID := range rowCount {
				textPKeyPrefix := textPKeyPrefixes[rowID%len(textPKeyPrefixes)]
				valueClause := fmt.Sprintf("( %d, 0, '%s-%d'", rowID, textPKeyPrefix, rowID)

				var valueClauseSb167 strings.Builder
				for _, columnType := range sortedTypes {
					valueClauseSb167.WriteString(`, ` + columnTypes[columnType][rowID%len(columnTypes[columnType])])
				}

				valueClause += valueClauseSb167.String()

				valueClause += `)`

				valueClauses = append(valueClauses, valueClause)
			}

			// Act
			ctx := t.Context()

			var targets []*pgx.ConnConfig

			var aliases []string

			for _, dbImage := range dbs {
				aliases = append(aliases, dbImage)
				// Create db and connect
				config, err := createContainer(t, ctx, dbImage)
				require.NoError(t, err)
				assert.True(t, waitForDBReady(t, ctx, config))
				conn, err := pgx.ConnectConfig(ctx, config)
				require.NoError(t, err)

				defer conn.Close(ctx)

				// Create and populate tables
				for _, tableName := range tableNames {
					if tableName == "test_column_names" {
						_, err = conn.Exec(ctx, `
				CREATE TABLE test_column_names (
					id INT PRIMARY KEY,
					"default" INT,
					"order" INT
				);`)
						require.NoError(t, err, "Failed to initialize 'test_column_names' table")

						continue // skip populating this table
					}

					createTableQuery := fmt.Sprintf(`CREATE TABLE "%s" %s`, tableName, createTableQueryBase)
					_, err = conn.Exec(ctx, createTableQuery)
					require.NoError(t, err, "Failed to create table %s on %v with query: %s", tableName, dbImage, createTableQuery)

					var pkeyString string

					switch {
					case strings.Contains(tableName, "multi_col"):
						pkeyString = fmt.Sprintf("multi_col_pkey_%s PRIMARY KEY (id, zid)", tableName)
					case strings.Contains(tableName, "stringkey"):
						pkeyString = fmt.Sprintf("text_col_pkey_%s PRIMARY KEY (sid)", tableName)
					default:
						pkeyString = fmt.Sprintf("single_col_pkey_%s PRIMARY KEY (id)", tableName)
					}

					require.NotEmpty(t, pkeyString)

					alterTableQuery := fmt.Sprintf(`ALTER TABLE ONLY "%s" ADD CONSTRAINT %s;`, tableName, pkeyString)
					_, err = conn.Exec(ctx, alterTableQuery)
					require.NoError(t, err, "Failed to add primary key to table %s on %v with query %s", tableName, dbImage, alterTableQuery)

					rand.Shuffle(len(valueClauses), func(i, j int) { valueClauses[i], valueClauses[j] = valueClauses[j], valueClauses[i] })
					insertDataQuery := fmt.Sprintf(`INSERT INTO "%s" %s %s`, tableName, insertDataQueryBase, strings.Join(valueClauses, ", "))
					_, err = conn.Exec(ctx, insertDataQuery)
					require.NoError(t, err, "Failed to insert data to table on %v with query %s", tableName, dbImage, insertDataQuery)
				}

				targets = append(targets, config)
			}

			logger := logrus.New()
			logger.Level = logrus.ErrorLevel

			results, err := pgverify.Verify(
				ctx,
				targets,
				pgverify.WithTests(
					pgverify.TestModeBookend,
					pgverify.TestModeSparse,
					pgverify.TestModeFull,
					pgverify.TestModeRowCount,
				),
				pgverify.WithLogger(logger),
				pgverify.ExcludeSchemas("pg_catalog", "pg_extension", "information_schema", "crdb_internal"),
				pgverify.ExcludeColumns("ignored", "rowid"),
				pgverify.WithAliases(aliases),
				pgverify.WithBookendLimit(5),
				pgverify.WithHashPrimaryKeys(),
			)
			require.NoError(t, err)
			require.NoError(t, results.WriteAsTable(os.Stdout))
		})
	}
}

func TestVerifyDataFail(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Arrange
	ctx := t.Context()

	dbs := []string{
		"postgres:latest",
		"cockroachdb/cockroach:latest", // cockroach cloud
	}

	// Act
	var targets []*pgx.ConnConfig

	var aliases []string

	var conns []*pgx.Conn

	for _, dbImage := range dbs {
		// Create db and connect
		config, err := createContainer(t, ctx, dbImage)
		require.NoError(t, err)
		assert.True(t, waitForDBReady(t, ctx, config))
		conn, err := pgx.ConnectConfig(ctx, config)
		require.NoError(t, err)

		aliases = append(aliases, dbImage)
		conns = append(conns, conn)
		targets = append(targets, config)

		// Create tables, initially all the same
		initTableQuery := `
			create table failtest (id int primary key);
			insert into failtest (id) values (1);
			insert into failtest (id) values (2);
			insert into failtest (id) values (3);
			insert into failtest (id) values (5);
			insert into failtest (id) values (6);
		`
		_, err = conn.Exec(ctx, initTableQuery)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		for _, conn := range conns {
			conn.Close(ctx)
		}
	})

	logger := logrus.New()
	logger.Level = logrus.WarnLevel

	for _, test := range []string{
		pgverify.TestModeFull,
		pgverify.TestModeSparse,
		pgverify.TestModeBookend,
		pgverify.TestModeRowCount,
	} {
		t.Run(test+"/AllSameRowsPass", func(t *testing.T) {
			results, err := pgverify.Verify(
				ctx,
				targets,
				pgverify.WithTests(test),
				pgverify.WithBookendLimit(4),
				pgverify.WithSparseMod(1),
				pgverify.WithLogger(logger),
				pgverify.IncludeSchemas("public"),
				pgverify.WithAliases(aliases),
			)
			require.NoError(t, results.WriteAsTable(os.Stdout))
			require.NoError(t, err)
		})
	}

	// Insert a row in just the first db
	addRowQuery := `
		insert into failtest (id) values (4);
	`
	_, err := conns[0].Exec(ctx, addRowQuery)
	require.NoError(t, err)

	for _, test := range []string{
		pgverify.TestModeFull,
		pgverify.TestModeSparse,
		pgverify.TestModeBookend,
		pgverify.TestModeRowCount,
	} {
		t.Run(test+"/FailAfterInsert", func(t *testing.T) {
			results, err := pgverify.Verify(
				ctx,
				targets,
				pgverify.WithTests(test),
				pgverify.WithBookendLimit(4),
				pgverify.WithSparseMod(1),
				pgverify.WithLogger(logger),
				pgverify.IncludeSchemas("public"),
				pgverify.WithAliases(aliases),
			)
			require.NoError(t, results.WriteAsTable(os.Stdout))
			require.Error(t, err) // should fail
		})
	}
}
