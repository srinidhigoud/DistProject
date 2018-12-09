package util

import "strings"

// Define a type that can be used by the flag library to collect an array of strings.
type ArrayClients []string

// Convert array to a string
func (a *ArrayClients) String() string {
	return strings.Join(*a, ",")
}

// Add a string
func (a *ArrayClients) Set(v string) error {
	*a = append(*a, v)
	return nil
}
