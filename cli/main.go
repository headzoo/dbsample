package main

import (
	"fmt"
	"github.com/headzoo/dbsampler"
	"os"
)

func main() {
	if err := dbsampler.Dump(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
