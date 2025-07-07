package pgverify

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/olekukonko/tablewriter"
)

const defaultErrorOutput = "(err)"

// Results stores the results from tests run in a verification. It is accessed
// from the per-target goroutines and is designed to be thread-safe.
type Results struct {
	// Names of each target to use in the generated output.
	targetNames []string
	// List of test modes run in the verification.
	testModes []string

	// The code data store of test results, stored in map tree with the schema:
	//   content[schema][table][mode][test output] = [targetName1, ...]
	content map[string]map[string]map[string]map[string][]string

	// Mutex to protect access to Results.content
	mutex *sync.Mutex
}

// NewResults creates a new Results object, configured with the output-formatted
// names of the targets and list of test modes ran.
func NewResults(targetNames []string, testModes []string) *Results {
	return &Results{
		content:     make(map[string]map[string]map[string]map[string][]string),
		targetNames: targetNames,
		testModes:   testModes,
		mutex:       &sync.Mutex{},
	}
}

// SingleResult represents the verification result from a single target, with the schema:
// SingleResult[schema][table][mode] = test output.
type SingleResult map[string]map[string]map[string]string

// AddResult adds a SingleResult from a test on a specific target to the Results object.
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

// CheckForErrors checks for and returns a list of any errors found by comparing test outputs.
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

// WriteAsTable writes the results as a table to the given io.Writer.
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
		for k := range header {
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
