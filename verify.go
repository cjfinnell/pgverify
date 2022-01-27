package pgverify

import (
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

// Verify runs all verification tests for the given table, configured by
// the supplied Options.
func Verify(targets []pgx.ConnConfig, opts ...Option) (*Results, error) {
	c := NewConfig(opts...)
	return c.Verify(targets)
}

// Verify runs all verification tests for the given table.
func (c Config) Verify(targets []pgx.ConnConfig) (*Results, error) {
	var finalResults *Results
	err := c.Validate()
	if err != nil {
		return finalResults, err
	}
	c.Logger.Infof("Verifying %d targets", len(targets))

	// First check that we can connect to every specified target database.
	var targetNames = make([]string, len(targets))
	conns := make(map[int]*pgx.Conn)
	for i, target := range targets {
		if len(c.Aliases) == len(targets) {
			targetNames[i] = c.Aliases[i]
		} else {
			targetNames[i] = targets[i].Host
		}
		conn, err := pgx.Connect(target)
		if err != nil {
			return finalResults, err
		}
		defer conn.Close()
		conns[i] = conn
	}
	finalResults = NewResults(targetNames, c.TestModes)

	// Then query each target database in parallel to generate table hashes.
	var doneChannels []chan struct{}
	for i, conn := range conns {
		done := make(chan struct{})
		go c.runTestsOnTarget(targetNames[i], conn, finalResults, done)
		doneChannels = append(doneChannels, done)
	}
	for _, done := range doneChannels {
		<-done
	}

	// Compare final results
	reportErrors := finalResults.CheckForErrors()
	if len(reportErrors) > 0 {
		return finalResults, multierr.Combine(reportErrors...)
	}

	c.Logger.Info("Verification successful")
	return finalResults, nil
}

func (c Config) runTestsOnTarget(targetName string, conn *pgx.Conn, finalResults *Results, done chan struct{}) {
	logger := c.Logger.WithField("target", targetName)

	schemaTableHashes, err := c.fetchTargetTableNames(logger, conn)
	if err != nil {
		logger.WithError(err).Error("failed to fetch target tables")
		close(done)
		return
	}

	schemaTableHashes, err = c.runTestQueriesOnTarget(logger, conn, schemaTableHashes)
	if err != nil {
		logger.WithError(err).Error("failed to run verification tests")
		close(done)
		return
	}

	finalResults.AddResult(targetName, schemaTableHashes)
	logger.Info("Table hashes computed")
	close(done)
}

func (c Config) fetchTargetTableNames(logger *logrus.Entry, conn *pgx.Conn) (SingleResult, error) {
	schemaTableHashes := make(SingleResult)
	rows, err := conn.Query(buildGetTablesQuery(c.IncludeSchemas, c.ExcludeSchemas, c.IncludeTables, c.ExcludeTables))
	if err != nil {
		return schemaTableHashes, errors.Wrap(err, "failed to query for tables")
	}
	for rows.Next() {
		var schema, table pgtype.Text
		err := rows.Scan(&schema, &table)
		if err != nil {
			return schemaTableHashes, errors.Wrap(err, "failed to scan row data for table names")
		}
		if _, ok := schemaTableHashes[schema.String]; !ok {
			schemaTableHashes[schema.String] = make(map[string]map[string]string)
		}
		schemaTableHashes[schema.String][table.String] = make(map[string]string)
		for _, testMode := range c.TestModes {
			schemaTableHashes[schema.String][table.String][testMode] = defaultErrorOutput
		}
	}
	return schemaTableHashes, nil
}

func (c Config) runTestQueriesOnTarget(logger *logrus.Entry, conn *pgx.Conn, schemaTableHashes SingleResult) (SingleResult, error) {
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
				var columnName, dataType, constraintName pgtype.Text
				err := rows.Scan(&columnName, &dataType, &constraintName)
				if err != nil {
					tableLogger.WithError(err).Error("Failed to parse column names, data types from query response")
					continue
				}
				tableColumns = append(tableColumns, column{columnName.String, dataType.String, constraintName.String})
			}
			tableLogger.Infof("Found %d columns", len(tableColumns))

			for _, testMode := range c.TestModes {
				testLogger := tableLogger.WithField("test", testMode)
				var query string
				switch testMode {
				case TestModeFull:
					query = buildFullHashQuery(schemaName, tableName, tableColumns)
				case TestModeBookend:
					query = buildBookendHashQuery(schemaName, tableName, tableColumns, c.BookendLimit)
				case TestModeSparse:
					query = buildSparseHashQuery(schemaName, tableName, tableColumns, c.SparseMod)
				case TestModeRowCount:
					query = buildRowCountQuery(schemaName, tableName)
				}

				row := conn.QueryRow(query)

				var testOutput pgtype.Text
				err = row.Scan(&testOutput)
				if err != nil {
					switch err {
					case pgx.ErrNoRows:
						testLogger.Info("No rows found")
						testOutput.String = "no rows"
					default:
						testLogger.WithError(err).Error("Failed to compute hash")
						continue
					}
				}
				schemaTableHashes[schemaName][tableName][testMode] = testOutput.String
				testLogger.Infof("Hash computed: %s", testOutput.String)
			}
		}
	}
	return schemaTableHashes, nil
}
