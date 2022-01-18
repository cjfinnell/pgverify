package dbverify

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/olekukonko/tablewriter"
)

// results[table][hash] = [target1, target2, ...]
type Results map[string]map[string][]int

func (r Results) WriteAsTable(writer io.Writer) {
	output := tablewriter.NewWriter(writer)
	output.SetHeader([]string{"Schema.Table", "Hash", "Targets"})

	var rows [][]string
	for table, hashes := range r {
		for hash, targets := range hashes {
			sort.Ints(targets)
			rows = append(rows, []string{
				table,
				hash,
				strings.Trim(strings.Join(strings.Fields(fmt.Sprint(targets)), ","), "[]"),
			})
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
	output.Render()
}
