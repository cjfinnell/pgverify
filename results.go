package pgverify

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/olekukonko/tablewriter"
)

const defaultErrorOutput = "(err)"

type Results struct {
	// Results.content[schema][table][mode][hash/output] = [target1, target2, ...]
	content     map[string]map[string]map[string]map[string][]string
	targetNames []string
	testModes   []string
	mutex       *sync.Mutex
}

// SingleResult represents the verification result from a single target
// result[schema][table][mode] = hash/output
type SingleResult map[string]map[string]map[string]string

func NewResults(targetNames []string, testModes []string) *Results {
	return &Results{
		content:     make(map[string]map[string]map[string]map[string][]string),
		targetNames: targetNames,
		testModes:   testModes,
		mutex:       &sync.Mutex{},
	}
}

func (r *Results) AddResult(targetName string, schemaTableHashes SingleResult) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for schema, tables := range schemaTableHashes {
		if _, ok := r.content[schema]; !ok {
			r.content[schema] = make(map[string]map[string]map[string][]string)
		}
		for table, modes := range tables {
			if _, ok := r.content[schema][table]; !ok {
				r.content[schema][table] = make(map[string]map[string][]string)
			}
			for mode, output := range modes {
				if _, ok := r.content[schema][table][mode]; !ok {
					r.content[schema][table][mode] = make(map[string][]string)
				}
				r.content[schema][table][mode][output] = append(r.content[schema][table][mode][output], targetName)
			}
		}
	}
}

func (r Results) CheckForErrors() []error {
	var errors []error
	for schema, tables := range r.content {
		for table, modes := range tables {
			for mode, outputs := range modes {
				if len(outputs) > 1 {
					errors = append(errors, fmt.Errorf("%s.%s test %s has %d outputs", schema, table, mode, len(outputs)))
					continue
				}
				for output, targets := range outputs {
					if len(targets) != len(r.targetNames) {
						errors = append(errors, fmt.Errorf("%s.%s test %s has %d targets (should be %d)", schema, table, mode, len(targets), len(r.targetNames)))
					}
					if output == defaultErrorOutput {
						errors = append(errors, fmt.Errorf("%s.%s test %s has error output", schema, table, mode))
					}
				}
			}
		}
	}
	return errors
}

func (r Results) WriteAsTable(writer io.Writer) {
	sort.Strings(r.testModes)
	header := []string{"schema", "table"}
	header = append(header, r.testModes...)
	header = append(header, "target")
	output := tablewriter.NewWriter(writer)
	output.SetHeader(header)

	var rows [][]string

	for schema, tables := range r.content {
		for table, modes := range tables {
			// map[target][mode] = output
			combinedModesOutputs := make(map[string]map[string]string)

			for mode, outputs := range modes {
				for output, targets := range outputs {
					for _, target := range targets {
						if _, ok := combinedModesOutputs[target]; !ok {
							combinedModesOutputs[target] = make(map[string]string)
						}
						combinedModesOutputs[target][mode] = output
					}
				}
			}

			for target := range combinedModesOutputs {
				row := []string{schema, table}
				for _, mode := range r.testModes {
					if _, ok := combinedModesOutputs[target][mode]; ok {
						row = append(row, combinedModesOutputs[target][mode])
					} else {
						row = append(row, defaultErrorOutput)
					}
				}
				row = append(row, target)
				rows = append(rows, row)
			}
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		for k := 0; k < len(header); k++ {
			if rows[i][k] != rows[j][k] {
				return rows[i][k] < rows[j][k]
			}
		}
		return false
	})
	for _, row := range rows {
		output.Append(row)
	}
	output.SetAutoMergeCellsByColumnIndex([]int{0, 1})
	output.SetAutoFormatHeaders(false)
	output.Render()
}
