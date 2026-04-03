package main

import (
	"fmt"
	"os"
	"plugin"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: pluginprobe <path-to-plugin.so>\n")
		os.Exit(2)
	}

	path := os.Args[1]
	p, err := plugin.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin.Open failed: %v\n", err)
		os.Exit(1)
	}

	sym, err := p.Lookup("GetName")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Lookup(GetName) failed: %v\n", err)
		os.Exit(1)
	}

	getName, ok := sym.(func() string)
	if !ok {
		fmt.Fprintf(os.Stderr, "GetName has unexpected type: %T\n", sym)
		os.Exit(1)
	}

	fmt.Println(getName())
}
