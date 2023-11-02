package pgverify

import (
	"context"

	"github.com/jackc/pgx/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"
)

// Verify runs all verification tests for the given table, configured by
// the supplied Options.
func Verify(ctx context.Context, targets []*pgx.ConnConfig, opts ...Option) (*Results, error) {
	c := NewConfig(opts...)

	return c.Verify(ctx, targets)
}

// Verify runs all verification tests for the given table.
func (c Config) Verify(ctx context.Context, targets []*pgx.ConnConfig) (*Results, error) {
	var finalResults *Results

	if err := c.Validate(); err != nil {
		return finalResults, err
	}

	c.Logger.Infof("Verifying %d targets", len(targets))

	// First check that we can connect to every specified target database.
	targetNames := make([]string, len(targets))
	conns := make(map[int]*pgx.Conn)

	for i, target := range targets {
		pgxLoggerFields := logrus.Fields{
			"component": "pgx",
			"host":      targets[i].Host,
			"port":      targets[i].Port,
			"database":  targets[i].Database,
			"user":      targets[i].User,
		}

		if len(c.Aliases) == len(targets) {
			targetNames[i] = c.Aliases[i]
			pgxLoggerFields["alias"] = c.Aliases[i]
		} else {
			targetNames[i] = targets[i].Host
		}

		target.Logger = &pgxLogger{c.Logger.WithFields(pgxLoggerFields)}

		target.LogLevel = pgx.LogLevelError

		conn, err := pgx.ConnectConfig(ctx, target)
		if err != nil {
			return finalResults, err
		}
		defer conn.Close(ctx)
		conns[i] = conn
	}

	finalResults = NewResults(targetNames, c.TestModes)

	// Then query each target database in parallel to generate table hashes.
	var doneChannels []chan struct{}

	for i, conn := range conns {
		done := make(chan struct{})
		go c.runTestsOnTarget(ctx, targetNames[i], conn, finalResults, done)
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

func (c Config) runTestsOnTarget(ctx context.Context, targetName string, conn *pgx.Conn, finalResults *Results, done chan struct{}) {
	logger := c.Logger.WithField("target", targetName)

	schemaTableHashes, err := c.fetchTargetTableNames(ctx, conn)
	if err != nil {
		logger.WithError(err).Error("failed to fetch target tables")
		close(done)

		return
	}

	schemaTableHashes, err = c.runTestQueriesOnTarget(ctx, logger, conn, schemaTableHashes)
	if err != nil {
		logger.WithError(err).Error("failed to run verification tests")
		close(done)

		return
	}

	finalResults.AddResult(targetName, schemaTableHashes)
	logger.Info("Table hashes computed")
	close(done)
}

func (c Config) fetchTargetTableNames(ctx context.Context, conn *pgx.Conn) (SingleResult, error) {
	schemaTableHashes := make(SingleResult)

	rows, err := conn.Query(ctx, buildGetTablesQuery(c.IncludeSchemas, c.ExcludeSchemas, c.IncludeTables, c.ExcludeTables))
	if err != nil {
		return schemaTableHashes, errors.Wrap(err, "failed to query for tables")
	}

	for rows.Next() {
		var schema, table pgtype.Text
		if err := rows.Scan(&schema, &table); err != nil {
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

func (c Config) validColumnTarget(columnName string) bool {
	if len(c.IncludeColumns) == 0 {
		for _, excludedColumn := range c.ExcludeColumns {
			if excludedColumn == columnName {
				return false
			}
		}

		return true
	}

	for _, includedColumn := range c.IncludeColumns {
		if includedColumn == columnName {
			return true
		}
	}

	return false
}

func (c Config) runTestQueriesOnTarget(ctx context.Context, logger *logrus.Entry, conn *pgx.Conn, schemaTableHashes SingleResult) (SingleResult, error) {
	for schemaName, tables := range schemaTableHashes {
		for tableName := range tables {
			tableLogger := logger.WithField("table", tableName).WithField("schema", schemaName)
			tableLogger.Info("Computing hash")

			rows, err := conn.Query(ctx, buildGetColumsQuery(schemaName, tableName))
			if err != nil {
				tableLogger.WithError(err).Error("Failed to query column names, data types")

				continue
			}

			allTableColumns := make(map[string]column)

			for rows.Next() {
				var columnName, dataType, constraintName, constraintType pgtype.Text

				err := rows.Scan(&columnName, &dataType, &constraintName, &constraintType)
				if err != nil {
					tableLogger.WithError(err).Error("Failed to parse column names, data types from query response")

					continue
				}

				existing, ok := allTableColumns[columnName.String]
				if ok {
					existing.constraints = append(existing.constraints, constraintType.String)
					allTableColumns[columnName.String] = existing
				} else {
					allTableColumns[columnName.String] = column{columnName.String, dataType.String, []string{constraintType.String}}
				}
			}

			var tableColumns []column

			var primaryKeyColumnNames []string

			for _, col := range allTableColumns {
				if col.IsPrimaryKey() {
					primaryKeyColumnNames = append(primaryKeyColumnNames, col.name)
				}

				if c.validColumnTarget(col.name) {
					tableColumns = append(tableColumns, col)
				}
			}

			if len(primaryKeyColumnNames) == 0 {
				tableLogger.Error("No primary keys found")

				continue
			}

			tableLogger.WithFields(logrus.Fields{
				"primary_keys": primaryKeyColumnNames,
				"columns":      tableColumns,
			}).Info("Determined columns to hash")

			for _, testMode := range c.TestModes {
				testLogger := tableLogger.WithField("test", testMode)

				var query string

				switch testMode {
				case TestModeFull:
					query = buildFullHashQuery(c, schemaName, tableName, tableColumns)
				case TestModeBookend:
					query = buildBookendHashQuery(c, schemaName, tableName, tableColumns, c.BookendLimit)
				case TestModeSparse:
					query = buildSparseHashQuery(c, schemaName, tableName, tableColumns, c.SparseMod)
				case TestModeRowCount:
					query = buildRowCountQuery(schemaName, tableName)
				}

				testLogger.Debugf("Generated query: %s", query)

				testOutput, err := runTestOnTable(ctx, conn, query)
				if err != nil {
					testLogger.WithError(err).Error("Failed to compute hash")

					continue
				}

				schemaTableHashes[schemaName][tableName][testMode] = testOutput
				testLogger.Infof("Hash computed: %s", testOutput)
			}
		}
	}

	return schemaTableHashes, nil
}

func runTestOnTable(ctx context.Context, conn *pgx.Conn, query string) (string, error) {
	row := conn.QueryRow(ctx, query)

	var testOutput pgtype.Text
	if err := row.Scan(&testOutput); err != nil {
		switch err {
		case pgx.ErrNoRows:
			return "no rows", nil
		default:
			return "", errors.Wrap(err, "failed to scan test output")
		}
	}

	return testOutput.String, nil
}
