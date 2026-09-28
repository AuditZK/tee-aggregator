package connector

import (
	"slices"
	"sort"
	"strings"
	"sync"
)

// cashflowNotes holds the markers the last GetCashflows raised. Each call
// replaces them, so a problem that has gone away stops being reported.
type cashflowNotes struct {
	mu    sync.Mutex
	marks []string
}

func (n *cashflowNotes) set(marks []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.marks = marks
}

func (n *cashflowNotes) get() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.marks)
}

// unpricedAssets collects the assets a cashflow was dropped for because no
// price could value it.
type unpricedAssets map[string]bool

func (u unpricedAssets) add(asset string) {
	if a := markerAsset(asset); a != "" {
		u[a] = true
	}
}

// markers names each asset as `<exchange>_cashflow_unpriced:<ASSET>`. The
// frontend keeps `_unpriced:` markers out of the user's banner: the user can
// do nothing about a missing price, while support can see which flow was left
// out of the returns.
func (u unpricedAssets) markers(exchange string) []string {
	out := make([]string, 0, len(u))
	for a := range u {
		out = append(out, exchange+"_cashflow_unpriced:"+a)
	}
	sort.Strings(out)
	return out
}

// markerAsset keeps a ticker to what the frontend accepts after the colon
// (2 to 15 letters or digits); anything else would read as a key-scope fault.
func markerAsset(asset string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(asset) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	s := b.String()
	if len(s) > 15 {
		s = s[:15]
	}
	if len(s) < 2 {
		return ""
	}
	return s
}
