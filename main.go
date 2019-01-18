package main

import (
	"log"
	"matheus/3dhubs/internals/importer"
	"os"
)

func main() {
	if len(os.Args) <= 1 {
		log.Println("Please provide 1 argument: 'import' or 'metrics'")
		os.Exit(0)
	}
	switch os.Args[1] {
	case "import":
		importer.Import()
	case "metrics":
		importer.Metrics()
	}
}
