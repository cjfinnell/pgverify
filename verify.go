package dbverify

import (
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

var (
	// results[table][hash] = [target1, target2, ...]
	finalResults      = make(map[string]map[string][]int)
	finalResultsMutex = &sync.Mutex{}
)

func Verify(targets []pgx.ConnConfig, opts ...Option) error {
	c := NewConfig(opts...)
	return c.Verify(targets)
}

func (c Config) Verify(targets []pgx.ConnConfig) error {
	c.Logger.Infof("Verifying %d targets", len(targets))

	// First check that we can connect to every specified target database.
	conns := make(map[int]*pgx.Conn)
	for i, target := range targets {
		conn, err := pgx.Connect(target)
		if err != nil {
			return err
		}
		defer conn.Close()
		conns[i] = conn
	}

	// Then query each target database in parallel to generate table hashes.
	var doneChannels []chan struct{}
	for i, conn := range conns {
		done := make(chan struct{})
		go c.generateTableHashes(i, conn, done)
		doneChannels = append(doneChannels, done)
	}
	for _, done := range doneChannels {
		<-done
	}

	// Compare final results
	c.Logger.Infof("Final hashes: %v", finalResults)
	var hashErrors []string
	for table, hashes := range finalResults {
		if len(hashes) > 1 {
			hashErrors = append(hashErrors, fmt.Sprintf("table %s has multiple hashes: %v", table, hashes))
		}
		for hash, reportTargets := range hashes {
			if len(targets) != len(reportTargets) {
				hashErrors = append(hashErrors, fmt.Sprintf("table %s hash %s has incorct number of targets: %v", table, hash, reportTargets))
			}
		}
	}

	if len(hashErrors) > 0 {
		return fmt.Errorf("Verification failed with errors: %s", strings.Join(hashErrors, "; "))
	}

	c.Logger.Info("Verification successful")
	return nil
}

func (c Config) generateTableHashes(targetIndex int, conn *pgx.Conn, done chan struct{}) {
	logger := c.Logger.WithField("target", targetIndex)
	schemaTableHashes := make(map[string]map[string]string)

	rows, err := conn.Query(c.buildGetTablesQuery())
	if err != nil {
		logger.WithError(err).Error("Failed to query for tables")
		close(done)
		return
	}
	for rows.Next() {
		var schema, table pgtype.Text
		err := rows.Scan(&schema, &table)
		if err != nil {
			logger.WithError(err).Error("Failed to scan row data for table names")
			close(done)
			return
		}
		if _, ok := schemaTableHashes[schema.String]; !ok {
			schemaTableHashes[schema.String] = make(map[string]string)
		}
		schemaTableHashes[schema.String][table.String] = ""
	}

	for schemaName, tables := range schemaTableHashes {
		for tableName := range tables {
			tableLogger := logger.WithField("table", tableName).WithField("schema", schemaName)
			tableLogger.Info("Computing hash")
			// Grab the column names/types
			query := fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' AND table_schema = '%s'", tableName, schemaName)
			rows, err := conn.Query(query)
			if err != nil {
				tableLogger.WithError(err).WithField("query", query).Error("Failed to query column names, data types")
				continue
			}
			var tableColumns []column
			for rows.Next() {
				var columnName, dataType pgtype.Text
				err := rows.Scan(&columnName, &dataType)
				if err != nil {
					tableLogger.WithError(err).Error("Failed to parse column names, data types from query response")
					continue
				}
				tableColumns = append(tableColumns, column{columnName.String, dataType.String})
			}
			tableLogger.Infof("Found %d columns", len(tableColumns))

			var columnsWithCasting []string
			for _, column := range tableColumns {
				columnsWithCasting = append(columnsWithCasting, column.String())
			}

			var hash pgtype.Text
			query = fmt.Sprintf(`
		SELECT md5(string_agg(hash, ''))
		FROM (SELECT '' AS grouper, MD5(CONCAT(%s)) AS hash FROM "%s"."%s" ORDER BY 2) AS eachrow
		GROUP BY grouper
		`, strings.Join(columnsWithCasting, ", "), schemaName, tableName)
			row := conn.QueryRow(query)
			err = row.Scan(&hash)
			if err != nil {
				switch err {
				case pgx.ErrNoRows:
					tableLogger.Info("No rows found")
					hash.String = "0"
				default:
					tableLogger.WithError(err).WithField("query", query).Error("Failed to compute hash")
					continue
				}
			}
			schemaTableHashes[schemaName][tableName] = hash.String
			tableLogger.Infof("Hash computed: %s", hash.String)
		}
	}

	finalResultsMutex.Lock()
	for schema, tables := range schemaTableHashes {
		for table, hash := range tables {
			tableFullName := fmt.Sprintf("%s.%s", schema, table)
			if _, ok := finalResults[tableFullName]; !ok {
				finalResults[tableFullName] = make(map[string][]int)
			}
			finalResults[tableFullName][hash] = append(finalResults[tableFullName][hash], targetIndex)
		}
	}
	finalResultsMutex.Unlock()
	logger.Info("Table hashes computed")
	close(done)
}

func (c Config) buildGetTablesQuery() string {
	query := "SELECT table_schema, table_name FROM information_schema.tables"
	whereClauses := []string{}

	if len(c.IncludeSchemas) > 0 {
		whereClause := "table_schema IN ("
		for i := 0; i < len(c.IncludeSchemas); i++ {
			whereClause += fmt.Sprintf("'%s'", c.IncludeSchemas[i])
			if i < len(c.IncludeSchemas)-1 {
				whereClause += ", "
			}
		}
		whereClause += ")"
		whereClauses = append(whereClauses, whereClause)
	} else if len(c.ExcludeSchemas) > 0 {
		whereClause := "table_schema NOT IN ("
		for i := 0; i < len(c.ExcludeSchemas); i++ {
			whereClause += fmt.Sprintf("'%s'", c.ExcludeSchemas[i])
			if i < len(c.ExcludeSchemas)-1 {
				whereClause += ", "
			}
		}
		whereClause += ")"
		whereClauses = append(whereClauses, whereClause)
	}

	if len(c.IncludeTables) > 0 {
		whereClause := "table_name IN ("
		for i := 0; i < len(c.IncludeTables); i++ {
			whereClause += fmt.Sprintf("'%s'", c.IncludeTables[i])
			if i < len(c.IncludeTables)-1 {
				whereClause += ", "
			}
		}
		whereClause += ")"
		whereClauses = append(whereClauses, whereClause)
	} else if len(c.ExcludeTables) > 0 {
		whereClause := "table_name NOT IN ("
		for i := 0; i < len(c.ExcludeTables); i++ {
			whereClause += fmt.Sprintf("'%s'", c.ExcludeTables[i])
			if i < len(c.ExcludeTables)-1 {
				whereClause += ", "
			}
		}
		whereClause += ")"
		whereClauses = append(whereClauses, whereClause)
	}

	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	return query
}
