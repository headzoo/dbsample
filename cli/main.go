package main

import (
	"fmt"
	"github.com/headzoo/dbsample"
	"os"
)

func main() {
	if err := dbsample.Dump(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
