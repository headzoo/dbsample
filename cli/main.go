package main

import (
	"fmt"
	"os"
	"github.com/headzoo/dbsample"
)

func main() {
	if err := dbsample.Dump(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
