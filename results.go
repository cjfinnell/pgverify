package pgverify

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/olekukonko/tablewriter"
)

type Results struct {
	// Results.Hashes[table][hash] = [target1, target2, ...]
	Hashes map[string]map[string][]string
	Mutex  *sync.Mutex
}

// SingleResult represents the verification result from a single target
// result[schema][table] = hash
type SingleResult map[string]map[string]string

func NewResults() *Results {
	return &Results{
		Hashes: make(map[string]map[string][]string),
		Mutex:  &sync.Mutex{},
	}
}

func (r *Results) AddResult(targetName string, schemaTableHashes SingleResult) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	for schema, tables := range schemaTableHashes {
		for table, hash := range tables {
			tableFullName := fmt.Sprintf("%s.%s", schema, table)
			if _, ok := r.Hashes[tableFullName]; !ok {
				r.Hashes[tableFullName] = make(map[string][]string)
			}
			r.Hashes[tableFullName][hash] = append(r.Hashes[tableFullName][hash], targetName)
		}
	}
}

func (r Results) WriteAsTable(writer io.Writer) {
	output := tablewriter.NewWriter(writer)
	output.SetHeader([]string{"Schema.Table", "Hash", "Targets"})

	var rows [][]string
	for table, hashes := range r.Hashes {
		for hash, targets := range hashes {
			sort.Strings(targets)
			for _, target := range targets {
				rows = append(rows, []string{
					table,
					hash,
					target,
				})
			}
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i][0] == rows[j][0] {
			if rows[i][1] == rows[j][1] {
				return rows[i][2] < rows[j][2]
			}
			return rows[i][1] < rows[j][1]
		}
		return rows[i][0] < rows[j][0]
	})
	for _, row := range rows {
		output.Append(row)
	}
	output.SetAutoMergeCellsByColumnIndex([]int{0, 1})
	output.Render()
}
