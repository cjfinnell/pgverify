package dbverify

import (
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

var (
	finalResults      = make(Results)
	finalResultsMutex = &sync.Mutex{}
)

func Verify(targets []pgx.ConnConfig, opts ...Option) (Results, error) {
	c := NewConfig(opts...)
	return c.Verify(targets)
}

func (c Config) Verify(targets []pgx.ConnConfig) (Results, error) {
	c.Logger.Infof("Verifying %d targets", len(targets))

	// First check that we can connect to every specified target database.
	conns := make(map[int]*pgx.Conn)
	for i, target := range targets {
		conn, err := pgx.Connect(target)
		if err != nil {
			return finalResults, err
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
		return finalResults, fmt.Errorf("Verification failed with errors: %s", strings.Join(hashErrors, "; "))
	}

	c.Logger.Info("Verification successful")
	return finalResults, nil
}

func (c Config) generateTableHashes(targetIndex int, conn *pgx.Conn, done chan struct{}) {
	logger := c.Logger.WithField("target", targetIndex)
	schemaTableHashes := make(map[string]map[string]string)

	rows, err := conn.Query(buildGetTablesQuery(c.IncludeSchemas, c.ExcludeSchemas, c.IncludeTables, c.ExcludeTables))
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
			rows, err := conn.Query(buildGetColumsQuery(schemaName, tableName))
			if err != nil {
				tableLogger.WithError(err).Error("Failed to query column names, data types")
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

			var hash pgtype.Text
			row := conn.QueryRow(buildGetHashQuery(schemaName, tableName, tableColumns))
			err = row.Scan(&hash)
			if err != nil {
				switch err {
				case pgx.ErrNoRows:
					tableLogger.Info("No rows found")
					hash.String = "0"
				default:
					tableLogger.WithError(err).Error("Failed to compute hash")
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
