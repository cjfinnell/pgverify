package pgverify

import (
	"fmt"
	"strings"
)

// Constructs a query that returns a list of tables with schemas that will be
// used for verification, translating the provided filter configuration to a
// SQL 'WHERE' clause. Exclusions override inclusions.
func buildGetTablesQuery(includeSchemas, excludeSchemas, includeTables, excludeTables []string) string {
	query := "SELECT table_schema, table_name FROM information_schema.tables"
	whereClauses := []string{}

	if len(includeSchemas) > 0 {
		whereClause := "table_schema IN ("
		for i := 0; i < len(includeSchemas); i++ {
			whereClause += fmt.Sprintf("'%s'", includeSchemas[i])
			if i < len(includeSchemas)-1 {
				whereClause += ", "
			}
		}
		whereClause += ")"
		whereClauses = append(whereClauses, whereClause)
	} else if len(excludeSchemas) > 0 {
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

	if len(includeTables) > 0 {
		whereClause := "table_name IN ("
		for i := 0; i < len(includeTables); i++ {
			whereClause += fmt.Sprintf("'%s'", includeTables[i])
			if i < len(includeTables)-1 {
				whereClause += ", "
			}
		}
		whereClause += ")"
		whereClauses = append(whereClauses, whereClause)
	} else if len(excludeTables) > 0 {
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

	return query
}

// Constructs a query that returns a list of columns for the given table,
// including the column name, data type, and constraint.
func buildGetColumsQuery(schemaName, tableName string) string {
	return fmt.Sprintf(`
		SELECT c.column_name, c.data_type, k.constraint_name
		FROM information_schema.columns as c
			LEFT OUTER JOIN information_schema.key_column_usage as k ON (
				c.column_name = k.column_name AND
				c.table_name = k.table_name AND
				c.table_schema = k.table_schema
			)
		WHERE c.table_name = '%s' AND c.table_schema = '%s'
		`, tableName, schemaName)
}

// Constructs a query for test mode full that generates a MD5 hash of each row,
// aggregates those hashes, and outputs a single hash of those hashes.
func buildFullHashQuery(schemaName, tableName string, columns []column) string {
	var columnsWithCasting []string
	for _, column := range columns {
		columnsWithCasting = append(columnsWithCasting, column.CastToText())
	}

	return fmt.Sprintf(`
		SELECT md5(string_agg(hash, ''))
		FROM (SELECT '' AS grouper, MD5(CONCAT(%s)) AS hash FROM "%s"."%s" ORDER BY 2) AS eachrow
		GROUP BY grouper
		`, strings.Join(columnsWithCasting, ", "), schemaName, tableName)
}

// Similar to the full test query, this test differs by first selecting a subset
// of the rows by casting the primary key value to an integer, then bucketing
// based off of that value modulo the configured SparseMod value.
func buildSparseHashQuery(schemaName, tableName string, columns []column, sparseMod int) string {
	var columnsWithCasting []string
	var primaryKey column
	for _, column := range columns {
		columnsWithCasting = append(columnsWithCasting, column.CastToText())
		if column.IsPrimaryKey() {
			primaryKey = column
		}
	}

	return fmt.Sprintf(`
		SELECT md5(string_agg(hash, ''))
		FROM (
			SELECT '' AS grouper, MD5(CONCAT(%s)) AS hash
			FROM "%s"."%s"
			WHERE %s in (
				SELECT %s
				FROM "%s"."%s"
				WHERE ('x' || substr(md5(%s),1,16))::bit(64)::bigint %% %d = 0
			)
			ORDER BY 2
		) AS eachrow
		GROUP BY grouper
		`,
		strings.Join(columnsWithCasting, ", "),
		schemaName, tableName,
		primaryKey.name,
		primaryKey.name,
		schemaName, tableName,
		primaryKey.CastToText(),
		sparseMod)
}

// Like the full test query, but only looks at the first and last N rows for generating hashes.
func buildBookendHashQuery(schemaName, tableName string, columns []column, limit int) string {
	var columnsWithCasting []string
	for _, column := range columns {
		columnsWithCasting = append(columnsWithCasting, column.CastToText())
	}
	allColumnsWithCasting := strings.Join(columnsWithCasting, ", ")

	return fmt.Sprintf(`
		SELECT md5(CONCAT(starthash::TEXT, endhash::TEXT))
		FROM (
			SELECT md5(string_agg(hash, ''))
			FROM (
				SELECT '' AS grouper, MD5(CONCAT(%s)) AS hash
				FROM "%s"."%s"
				ORDER BY 2 ASC
				LIMIT %d
			) AS eachrow
			GROUP BY grouper
		) as starthash, (
			SELECT md5(string_agg(hash, ''))
			FROM (
				SELECT '' AS grouper, MD5(CONCAT(%s)) AS hash
				FROM "%s"."%s"
				ORDER BY 2 DESC
				LIMIT %d
			) AS eachrow
			GROUP BY grouper
		) as endhash
		`, allColumnsWithCasting, schemaName, tableName, limit, allColumnsWithCasting, schemaName, tableName, limit)
}

// A minimal test that simply counts the number of rows.
func buildRowCountQuery(schemaName, tableName string) string {
	return fmt.Sprintf(`SELECT count(*)::TEXT FROM "%s"."%s"`, schemaName, tableName)
}
