package godb

/*
 TableStats represents statistics (e.g., histograms) about base tables in a
 query.
*/

// Interface for statistics that are maintained for a table.
type Stats interface {
	EstimateScanCost() float64
	EstimateCardinality(selectivity float64) int
	EstimateSelectivity(field string, op BoolOp, value DBValue) (float64, error)
}

type TableStats struct {
	basePages  int
	baseTups   int
	histograms map[string]any
	tupleDesc  *TupleDesc
}

// The default cost to read a page from disk. This value can be adjusted to
// accommodate different storage devices.
const CostPerPage = 1000

// Number of bins for histograms. Feel free to increase this value over 100,
// though our tests assume that you have at least 100 bins in your histograms.
const NumHistBins = 100

// Dummy Implementations to implement the interface
func (ts *TableStats) EstimateScanCost() float64 {
	return 0
}
func (ts *TableStats) EstimateCardinality(selectivity float64) int {
	return 0
}
func (ts *TableStats) EstimateSelectivity(field string, op BoolOp, value DBValue) (float64, error) {
	return 0.5, nil
}