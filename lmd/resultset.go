package main

// ResultSet is a list of result rows
type ResultSet [][]interface{}

// ResultSetStats contains a result from a stats query
type ResultSetStats map[string][]*Filter
