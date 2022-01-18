package dbverify

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	log "github.com/sirupsen/logrus"
)

var (
	// results[table][hash] = [target1, target2, ...]
	finalResults      = make(map[string]map[string][]int)
	finalResultsMutex = &sync.Mutex{}
)

func Verify(targets []pgx.ConnConfig, includeTables, excludeTables, includeSchemas, excludeSchemas []string) error {
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stderr)
	log.Infof("Verifying %d targets", len(targets))

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
		go generateTableHashes(i, conn, includeTables, excludeTables, includeSchemas, excludeSchemas, done)
		doneChannels = append(doneChannels, done)
	}
	for _, done := range doneChannels {
		<-done
	}

	// Compare final results
	log.Infof("Final hashes: %v", finalResults)
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

	log.Info("Verification successful")
	return nil
}

type column struct {
	name     string
	dataType string
}

func generateTableHashes(targetIndex int, conn *pgx.Conn, explicitTables, excludeTables, explicitSchemas, excludeSchemas []string, done chan struct{}) {
	logger := log.WithField("target", targetIndex)
	schemaTableHashes := make(map[string]map[string]string)

	if len(explicitTables) == 0 {
		logger.Info("No explicit tables specified, querying all tables")
		query := "SELECT table_schema, table_name FROM information_schema.tables"
		whereClauses := []string{}

		if len(explicitSchemas) > 0 {
			logger.Info("Explicit schemas specified, filtering tables")
			// Only query these schemas
			whereClause := "table_schema IN ("
			for i := 0; i < len(explicitSchemas); i++ {
				whereClause += fmt.Sprintf("'%s'", explicitSchemas[i])
				if i < len(explicitSchemas)-1 {
					whereClause += ", "
				}
			}
			whereClause += ")"
			whereClauses = append(whereClauses, whereClause)
		} else if len(excludeSchemas) > 0 {
			logger.Info("Excluded schemas specified, filtering tables")
			// Query all but these schemas
			whereClause := "table_schema NOT IN ("
			for i := 0; i < len(excludeSchemas); i++ {
				whereClause += fmt.Sprintf("'%s'", excludeSchemas[i])
				if i < len(excludeSchemas)-1 {
					whereClause += ", "
				}
			}
			whereClause += ")"
			whereClauses = append(whereClauses, whereClause)
		}

		if len(excludeTables) > 0 {
			logger.Info("Excluded tables specified, filtering tables")
			whereClause := "table_name NOT IN ("
			for i := 0; i < len(excludeTables); i++ {
				whereClause += fmt.Sprintf("'%s'", excludeTables[i])
				if i < len(excludeTables)-1 {
					whereClause += ", "
				}
			}
			whereClause += ")"
			whereClauses = append(whereClauses, whereClause)
		}

		if len(whereClauses) > 0 {
			query += " WHERE " + strings.Join(whereClauses, " AND ")
		}
		rows, err := conn.Query(query)
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

			sort.Slice(tableColumns, func(i, j int) bool {
				return tableColumns[i].name < tableColumns[j].name
			})

			var columnsWithCasting []string
			for _, column := range tableColumns {
				columnsWithCasting = append(columnsWithCasting, determineTypeCasting(column))
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

func determineTypeCasting(col column) string {
	switch strings.ToLower(col.dataType) {
	case "timestamp with time zone":
		return fmt.Sprintf("extract(epoch from %s)::TEXT", col.name)
	default:
		return col.name + "::TEXT"
	}
}
