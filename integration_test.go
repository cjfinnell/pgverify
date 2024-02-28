package pgverify_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cjfinnell/pgverify"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	dbUser     = "test"
	dbPassword = "test"
	dbName     = "test"
)

func waitForDBReady(t *testing.T, ctx context.Context, config *pgx.ConnConfig) bool {
	t.Helper()

	connected := false

	for count := 0; count < 30; count++ {
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

func createContainer(t *testing.T, ctx context.Context, image string, port int, env, cmd []string) (string, int, error) {
	t.Helper()

	docker, err := newDockerClient()
	if err != nil {
		return "", 0, err
	}

	hostPort, err := getFreePort()
	if err != nil {
		return "", 0, errors.New("could not determine a free port")
	}

	container, err := docker.runContainer(
		t,
		ctx,
		&containerConfig{
			image: image,
			ports: []*portMapping{
				{
					HostPort:      strconv.Itoa(hostPort),
					ContainerPort: strconv.Itoa(port),
				},
			},
			env: env,
			cmd: cmd,
		})
	if err != nil {
		return "", 0, err
	}

	t.Cleanup(func() {
		if err := docker.removeContainer(t, ctx, container.ID); err != nil {
			t.Errorf("Could not remove container %s: %v", container.ID, err)
		}
	})

	return container.ID, hostPort, nil
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

//nolint:maintidx
func TestVerifyData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Arrange
	ctx := context.Background()

	dbs := []struct {
		image        string
		cmd          []string
		env          []string
		port         int
		userPassword string
		db           string
	}{
		{
			image: "postgres:10",
			cmd:   []string{"postgres"},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", dbName),
				fmt.Sprintf("POSTGRES_USER=%s", dbUser),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword),
			},
			port:         5432,
			userPassword: dbUser + ":" + dbPassword,
			db:           "/" + dbName,
		},
		{
			image: "postgres:11",
			cmd:   []string{"postgres"},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", dbName),
				fmt.Sprintf("POSTGRES_USER=%s", dbUser),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword),
			},
			port:         5432,
			userPassword: dbUser + ":" + dbPassword,
			db:           "/" + dbName,
		},
		{
			image: "postgres:12.6",
			cmd:   []string{"postgres"},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", dbName),
				fmt.Sprintf("POSTGRES_USER=%s", dbUser),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword),
			},
			port:         5432,
			userPassword: dbUser + ":" + dbPassword,
			db:           "/" + dbName,
		},
		{
			image: "postgres:12.11",
			cmd:   []string{"postgres"},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", dbName),
				fmt.Sprintf("POSTGRES_USER=%s", dbUser),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword),
			},
			port:         5432,
			userPassword: dbUser + ":" + dbPassword,
			db:           "/" + dbName,
		},
		{
			image:        "cockroachdb/cockroach:v21.2.0",
			cmd:          []string{"start-single-node", "--insecure"},
			port:         26257,
			userPassword: "root",
		},
		{
			image:        "cockroachdb/cockroach:v21.2.12",
			cmd:          []string{"start-single-node", "--insecure"},
			port:         26257,
			userPassword: "root",
		},
		{
			image:        "cockroachdb/cockroach:v22.2.3",
			cmd:          []string{"start-single-node", "--insecure"},
			port:         26257,
			userPassword: "root",
		},
		{
			image:        "cockroachdb/cockroach:latest", // cockroach cloud
			cmd:          []string{"start-single-node", "--insecure"},
			port:         26257,
			userPassword: "root",
		},
	}

	columnTypes := map[string][]string{
		"boolean":   {"true", "false"},
		"bytea":     {fmt.Sprintf("'%s'", hex.EncodeToString([]byte("convert this content to bytes")))},
		"bit(1)":    {"'1'", "'0'"},
		"varbit(3)": {"'0'", "'1'", "'101'", "'010'"},

		"bigint[]":         {"'{602213950000000000, -1}'"},
		"integer":          {"0", "123979", "-23974"},
		"double precision": {"69.123987", "-69.123987"},
		"numeric":          {"0", "123.456", "-123.456"},
		"decimal":          {"0", "123.456", "-123.456"},

		"text":                  {`'foo'`, `'bar'`, `''`, `'something that is much longer but without much other complication'`},
		"uuid":                  {fmt.Sprintf("'%s'", uuid.New().String())},
		`character varying(64)`: {`'more string stuff'`},

		"jsonb": {`'{}'`, `'{"foo": ["bar", "baz"]}'`, `'{"foo": "bar"}'`, `'{"foo": "bar", "baz": "qux"}'`, `'{"for sure?": true, "has numbers": 123.456, "this is": ["some", "json", "blob"]}'`},
		"json":  {`'{}'`, `'{"foo": ["bar", "baz"]}'`, `'{"foo": "bar"}'`, `'{"foo": "bar", "baz": "qux"}'`, `'{"for sure?": true, "has numbers": 123.456, "this is": ["some", "json", "blob"]}'`},

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
		cleanName := strings.ReplaceAll(fmt.Sprintf("col_%s", k), " ", "_")
		for _, char := range "()[]" {
			cleanName = strings.ReplaceAll(cleanName, string(char), "")
		}

		sortedTypes[i] = k
		keys[i] = cleanName
		keysWithTypes[i] = strings.Join([]string{keys[i], k}, " ")
		i++
	}

	sort.Strings(keys)
	sort.Strings(keysWithTypes)
	sort.Strings(sortedTypes)

	tableNames := []string{"testtable1", "testTABLE_multi_col_2", "testtable3", "test_stringkey_table4"}
	createTableQueryBase := fmt.Sprintf("( id INT DEFAULT 0 NOT NULL, zid INT DEFAULT 0 NOT NULL, sid TEXT NOT NULL, ignored TIMESTAMP WITH TIME ZONE DEFAULT NOW(), %s);", strings.Join(keysWithTypes, ", "))

	rowCount := calculateRowCount(columnTypes)
	insertDataQueryBase := `(id, zid, sid,` + strings.Join(keys, ", ") + `) VALUES `
	valueClauses := make([]string, 0, rowCount)

	// Modulo-cycle through prefixes to re-create ORDER BY issue
	textPKeyPrefixes := []string{"A", "AA", "a", "aa", "A-A", "a-a"}

	for rowID := 0; rowID < rowCount; rowID++ {
		textPKeyPrefix := textPKeyPrefixes[rowID%len(textPKeyPrefixes)]
		valueClause := fmt.Sprintf("( %d, 0, '%s-%d'", rowID, textPKeyPrefix, rowID)

		for _, columnType := range sortedTypes {
			valueClause += `, ` + columnTypes[columnType][rowID%len(columnTypes[columnType])]
		}

		valueClause += `)`

		valueClauses = append(valueClauses, valueClause)
	}

	// Act
	var targets []*pgx.ConnConfig

	var aliases []string

	for _, db := range dbs {
		aliases = append(aliases, db.image)
		// Create db and connect
		_, port, err := createContainer(t, ctx, db.image, db.port, db.env, db.cmd)
		require.NoError(t, err)
		config, err := pgx.ParseConfig(fmt.Sprintf("postgresql://%s@127.0.0.1:%d%s", db.userPassword, port, db.db))
		require.NoError(t, err)
		assert.True(t, waitForDBReady(t, ctx, config))
		conn, err := pgx.ConnectConfig(ctx, config)
		require.NoError(t, err)

		defer conn.Close(ctx)

		// Create and populate tables
		for _, tableName := range tableNames {
			createTableQuery := fmt.Sprintf(`CREATE TABLE "%s" %s`, tableName, createTableQueryBase)
			_, err = conn.Exec(ctx, createTableQuery)
			require.NoError(t, err, "Failed to create table %s on %v with query: %s", tableName, db.image, createTableQuery)

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
			require.NoError(t, err, "Failed to add primary key to table %s on %v with query %s", tableName, db.image, alterTableQuery)

			rand.Shuffle(len(valueClauses), func(i, j int) { valueClauses[i], valueClauses[j] = valueClauses[j], valueClauses[i] })
			insertDataQuery := fmt.Sprintf(`INSERT INTO "%s" %s %s`, tableName, insertDataQueryBase, strings.Join(valueClauses, ", "))
			_, err = conn.Exec(ctx, insertDataQuery)
			require.NoError(t, err, "Failed to insert data to table on %v with query %s", tableName, db.image, insertDataQuery)
		}

		targets = append(targets, config)
	}

	logger := logrus.New()
	logger.Level = logrus.ErrorLevel
	// Test all the different verification strategies
	for i = 0; i < 1; i++ {
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
		results.WriteAsTable(os.Stdout)
	}
}
