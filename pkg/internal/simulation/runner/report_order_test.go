package runner

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReportOrder_ListsEveryScenario keeps the CI comment's fixed row order complete: a
// scenario missing from the list would otherwise land at the end of its table.
func TestReportOrder_ListsEveryScenario(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../../../.github/scripts/simulation-order.json")
	require.NoError(t, err)
	var order []string
	require.NoError(t, json.Unmarshal(data, &order))

	all, err := profileScenarios("all", true, defaultRunID)
	require.NoError(t, err)
	var names []string
	for _, sc := range all {
		names = append(names, sc.name)
		require.Contains(t, order, sc.name)
	}
	for _, name := range order {
		require.Contains(t, names, name, "the order lists a scenario that does not exist")
	}
	require.Len(t, order, len(names))

	// The two sides of a threshold pair stay next to each other, below first.
	for i, name := range order {
		if strings.Contains(name, " just above ") {
			require.Equal(t, strings.Replace(name, " just above ", " just below ", 1), order[i-1])
		}
	}
	require.False(t, slices.Contains(order, ""))
}
