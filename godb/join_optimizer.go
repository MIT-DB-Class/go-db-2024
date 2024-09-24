package godb

// Estimate the cost of a join j given the cardinalities (card1, card2) and
// estimated costs (cost1, cost2) of the left and right sides of the join,
// respectively.
//
// The cost of the join should be calculated based on the join algorithm (or
// algorithms) that you implemented for Lab 2. It should be a function of the
// amount of data that must be read over the course of the query, as well as the
// number of CPU opertions performed by your join. Assume that the cost of a
// single predicate application is roughly 1.
func EstimateJoinCost(card1 int, card2 int, cost1 float64, cost2 float64) float64 {
	// Dummy implementation. Do not worry about this.
	return -1.0
}

// Estimate the cardinality of the result of a join between two tables, given
// the join operator, primary key information, and table statistics.
func EstimateJoinCardinality(t1card int, t2card int) int {
	// Dummy implementation. Do not worry about this.
	return -1
}

type TableInfo struct {
	name  string  // Name of the table
	stats Stats   // Statistics for the table; may be nil if no stats are available
	sel   float64 // Selectivity of the filters on the table
}

// A JoinNode represents a join between two tables.
type JoinNode struct {
	leftTable TableInfo
	leftField string

	rightTable TableInfo
	rightField string
}

// Given a list of joins, table statistics, and selectivities, return the best
// order in which to join the tables.
//
// selectivity is a map from table aliases to the selectivity of the filters on
// that table. Note that LogicalJoinNodes contain LogicalSelectNodes that define
// tables to join. Inside a LogicalSelectNode, there is both a table name
// (table) and an alias. We may apply different filters to the same base table
// but with different aliases, so the selectivity map contains selectivities for
// a particular alias, not for a base table.
func OrderJoins(joins []*JoinNode) ([]*JoinNode, error) {
	// Dummy implementation. Do not worry about this.
	return joins, nil
}

