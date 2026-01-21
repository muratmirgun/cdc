package writer

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

func TestPostgresProtocolSelection(t *testing.T) {
	// Property 15: Batch Write Protocol Selection
	// Validates: Requirements 7.4
	properties := gopter.NewProperties(nil)

	properties.Property("protocol selection threshold is 100", prop.ForAll(
		func(size int) bool {
			if size < 0 || size > 500 {
				return true
			}
			records := make([]PlayerRecord, size)
			w := &PGWriter{}
			usesCopy := w.ShouldUseCopy(records)
			if size >= 100 {
				return usesCopy
			}
			return !usesCopy
		},
		gen.IntRange(0, 500),
	))

	properties.TestingRun(t, gopter.ConsoleReporter(false))
}
