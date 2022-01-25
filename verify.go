package pgverify

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

func Verify(targets []pgx.ConnConfig, opts ...Option) (*Results, error) {
	c := NewConfig(opts...)
	return c.Verify(targets)
}

func (c Config) Verify(targets []pgx.ConnConfig) (*Results, error) {
	finalResults := NewResults()

	err := c.Validate()
	if err != nil {
		return finalResults, err
	}
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
		var targetName string
		if len(c.Aliases) == len(targets) {
			targetName = c.Aliases[i]
		} else {
			targetName = targets[i].Host
		}
		done := make(chan struct{})
		go c.generateTableHashes(targetName, conn, finalResults, done)
		doneChannels = append(doneChannels, done)
	}
	for _, done := range doneChannels {
		<-done
	}

	// Compare final results
	var hashErrors []string
	for table, hashes := range finalResults.Hashes {
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

func (c Config) generateTableHashes(targetHost string, conn *pgx.Conn, finalResults *Results, done chan struct{}) {
	logger := c.Logger.WithField("target", targetHost)
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

			var query string
			switch c.Strategy {
			case StrategyFull:
				query = buildFullHashQuery(schemaName, tableName, tableColumns)
			case StrategyBookend:
				query = buildBookendHashQuery(schemaName, tableName, tableColumns, c.BookendLimit)
			}

			row := conn.QueryRow(query)

			var hash pgtype.Text
			err = row.Scan(&hash)
			if err != nil {
				switch err {
				case pgx.ErrNoRows:
					tableLogger.Info("No rows found")
					hash.String = "no rows"
				default:
					tableLogger.WithError(err).Error("Failed to compute hash")
					continue
				}
			}
			schemaTableHashes[schemaName][tableName] = hash.String
			tableLogger.Infof("Hash computed: %s", hash.String)
		}
	}

	finalResults.Mutex.Lock()
	for schema, tables := range schemaTableHashes {
		for table, hash := range tables {
			tableFullName := fmt.Sprintf("%s.%s", schema, table)
			if _, ok := finalResults.Hashes[tableFullName]; !ok {
				finalResults.Hashes[tableFullName] = make(map[string][]string)
			}
			finalResults.Hashes[tableFullName][hash] = append(finalResults.Hashes[tableFullName][hash], targetHost)
		}
	}
	finalResults.Mutex.Unlock()
	logger.Info("Table hashes computed")
	close(done)
}
