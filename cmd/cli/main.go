package main

import (
	"fmt"
	"os"

	"github.com/sgatu/kxtract/internal/config"
)

func main() {
	reqCfg, err := config.GetRequest()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if reqCfg.Args.DryRun {
		dryRun(reqCfg)
	}
}

func dryRun(r *config.Request) {
	fmt.Println("Dry Run")
	fmt.Println("--- Loaded configuration ---")
	r.PrettyPrint()
	fmt.Println()
	os.Exit(0)
}
