package util

import "strings"

// Define a type that can be used by the flag library to collect an array of strings.
type arrayClients []string

// Convert array to a string
func (a *arrayClients) String() string {
	return strings.Join(*a, ",")
}

// Add a string
func (a *arrayClients) Set(v string) error {
	*a = append(*a, v)
	return nil
}
