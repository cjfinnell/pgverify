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
	content     map[string]map[string][]string
	targetNames []string
	mutex       *sync.Mutex
}

// SingleResult represents the verification result from a single target
// result[schema][table] = hash
type SingleResult map[string]map[string]string

func NewResults(targetNames []string) *Results {
	return &Results{
		content:     make(map[string]map[string][]string),
		targetNames: targetNames,
		mutex:       &sync.Mutex{},
	}
}

func (r *Results) AddResult(targetName string, schemaTableHashes SingleResult) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for schema, tables := range schemaTableHashes {
		for table, hash := range tables {
			tableFullName := fmt.Sprintf("%s.%s", schema, table)
			if _, ok := r.content[tableFullName]; !ok {
				r.content[tableFullName] = make(map[string][]string)
			}
			r.content[tableFullName][hash] = append(r.content[tableFullName][hash], targetName)
		}
	}
}

func (r Results) CheckForErrors() []error {
	var errors []error
	for table, hashes := range r.content {
		if len(hashes) > 1 {
			errors = append(errors, fmt.Errorf("table %s has multiple hashes: %v", table, hashes))
		}
		for hash, reportTargets := range hashes {
			if len(r.targetNames) != len(reportTargets) {
				errors = append(errors, fmt.Errorf("table %s hash %s has incorct number of targets: %v", table, hash, reportTargets))
			}
		}
	}
	return errors
}

func (r Results) WriteAsTable(writer io.Writer) {
	output := tablewriter.NewWriter(writer)
	output.SetHeader([]string{"Schema.Table", "Hash", "Targets"})

	var rows [][]string
	for table, hashes := range r.content {
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
	output.SetAutoMergeCellsByColumnIndex([]int{0})
	output.Render()
}
