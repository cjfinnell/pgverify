package integration

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cjfinnell/pgverify"
	"github.com/google/uuid"
	"github.com/jackc/pgx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	dbUser     = "test"
	dbPassword = "test"
	dbName     = "test"
)

func waitForDBReady(config *pgx.ConnConfig) bool {
	connected := false
	for count := 0; count < 30; count++ {
		conn, err := pgx.Connect(*config)
		if err == nil {
			connected = true
			conn.Close()
			break
		}
		time.Sleep(2 * time.Second)
	}
	return connected
}

func createContainer(t *testing.T, ctx context.Context, image string, port int, env, cmd []string) (string, int, error) {
	docker, err := NewDockerClient()
	if err != nil {
		return "", 0, err
	}

	hostPort, err := getFreePort()
	if err != nil {
		return "", 0, errors.New("could not determine a free port")
	}

	container, err := docker.runContainer(
		ctx,
		&ContainerConfig{
			image: image,
			ports: []*PortMapping{
				{
					HostPort:      fmt.Sprintf("%d", hostPort),
					ContainerPort: fmt.Sprintf("%d", port),
				},
			},
			env: env,
			cmd: cmd,
		})
	if err != nil {
		return "", 0, err
	}

	t.Cleanup(func() {
		if err := docker.removeContainer(ctx, container.ID); err != nil {
			t.Errorf("Could not remove container %s: %v", container.ID, err)
		}
	})

	return container.ID, hostPort, nil
}

func calculateRowCount(columnTypes map[string][]string) int {
	rowCount := 5000 // Minimum
	for _, columnType := range columnTypes {
		if rowCount < len(columnType) {
			rowCount = len(columnType)
		}
	}
	return rowCount
}

func TestVerifyData(t *testing.T) {
	// Arrange
	ctx := context.Background()

	dbs := []struct {
		image string
		cmd   []string
		env   []string
		port  int

		config pgx.ConnConfig
		conn   *pgx.Conn
	}{
		{
			image: "postgres:10",
			cmd:   []string{"postgres"},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", dbName),
				fmt.Sprintf("POSTGRES_USER=%s", dbUser),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword),
			},
			port: 5432,
			config: pgx.ConnConfig{
				User:     dbUser,
				Password: dbPassword,
				Database: dbName,
			},
		},
		{
			image: "postgres:11",
			cmd:   []string{"postgres"},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", dbName),
				fmt.Sprintf("POSTGRES_USER=%s", dbUser),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword),
			},
			port: 5432,
			config: pgx.ConnConfig{
				User:     dbUser,
				Password: dbPassword,
				Database: dbName,
			},
		},
		{
			image: "postgres:12.6",
			cmd:   []string{"postgres"},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", dbName),
				fmt.Sprintf("POSTGRES_USER=%s", dbUser),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", dbPassword),
			},
			port: 5432,
			config: pgx.ConnConfig{
				User:     dbUser,
				Password: dbPassword,
				Database: dbName,
			},
		},
		{
			image: "cockroachdb/cockroach:latest",
			cmd:   []string{"start-single-node", "--insecure"},
			port:  26257,
			config: pgx.ConnConfig{
				User: "root",
			},
		},
	}

	columnTypes := map[string][]string{
		"boolean": {"true", "false"},
		"bytea":   {fmt.Sprintf("'%s'", hex.EncodeToString([]byte("convert this content to bytes")))},

		"bigint[]": {"'{602213950000000000}'"},
		"integer":  {"0", "123979"},

		"text":                  {`'foo'`, `'bar'`, `''`, `'something that is much longer but without much other complication'`},
		"uuid":                  {fmt.Sprintf("'%s'", uuid.New().String())},
		`character varying(64)`: {`'more string stuff'`},

		"date":                        {`'2020-12-31'`},
		"timestamp with time zone":    {`'2020-12-31 23:59:59 -8:00'`}, // hashes differently for psql/crdb, convert to epoch when hashing
		"timestamp without time zone": {`'2020-12-31 23:59:59'`},
	}
	keys := make([]string, len(columnTypes))
	keysWithTypes := make([]string, len(columnTypes))
	sortedTypes := make([]string, len(columnTypes))
	i := 0
	for k := range columnTypes {
		// Create sanitized column name from type
		cleanName := strings.Replace(fmt.Sprintf("col_%s", k), " ", "_", -1)
		for _, char := range "()[]" {
			cleanName = strings.Replace(cleanName, string(char), "", -1)
		}

		sortedTypes[i] = k
		keys[i] = cleanName
		keysWithTypes[i] = strings.Join([]string{keys[i], k}, " ")
		i++
	}
	sort.Strings(keys)
	sort.Strings(keysWithTypes)
	sort.Strings(sortedTypes)

	tableNames := []string{"testtable1", "testtable2", "testtable3"}
	createTableQueryBase := fmt.Sprintf("( id INT PRIMARY KEY, %s);", strings.Join(keysWithTypes, ", "))

	rowCount := calculateRowCount(columnTypes)
	insertDataQueryBase := `(id, ` + strings.Join(keys, ", ") + `) VALUES `
	for rowID := 0; rowID < rowCount; rowID++ {
		if rowID != 0 {
			insertDataQueryBase += ", "
		}
		insertDataQueryBase += `(` + strconv.Itoa(rowID)
		for _, columnType := range sortedTypes {
			insertDataQueryBase += `, ` + columnTypes[columnType][rowID%len(columnTypes[columnType])]
		}
		insertDataQueryBase += `)`
	}

	// Act
	var targets []pgx.ConnConfig
	var aliases []string
	for _, db := range dbs {
		aliases = append(aliases, db.image)
		// Create db and connect
		_, port, err := createContainer(t, ctx, db.image, db.port, db.env, db.cmd)
		assert.NoError(t, err)
		db.config.Host = "127.0.0.1"
		db.config.Port = uint16(port)
		assert.True(t, waitForDBReady(&db.config))
		db.conn, err = pgx.Connect(db.config)
		require.NoError(t, err)
		defer db.conn.Close()

		// Create and populate tables
		for _, tableName := range tableNames {
			createTableQuery := fmt.Sprintf(`CREATE TABLE "%s" %s`, tableName, createTableQueryBase)
			_, err = db.conn.Exec(createTableQuery)
			assert.NoError(t, err, "Failed to create table %s on %v with query: %s", tableName, db.image, createTableQuery)

			insertDataQuery := fmt.Sprintf(`INSERT INTO "%s" %s`, tableName, insertDataQueryBase)
			_, err = db.conn.Exec(insertDataQuery)
			assert.NoError(t, err, "Failed to insert data to table on %v with query %s", tableName, db.image, insertDataQuery)
		}
		targets = append(targets, db.config)
	}

	// Test all the different verification strategies
	results, err := pgverify.Verify(
		targets,
		pgverify.WithTests(
			pgverify.TestModeBookend,
			pgverify.TestModeSparse,
			pgverify.TestModeFull,
		),
		pgverify.ExcludeSchemas("pg_catalog", "pg_extension", "information_schema", "crdb_internal"),
		pgverify.WithAliases(aliases),
	)
	assert.NoError(t, err)
	results.WriteAsTable(os.Stdout)
}
