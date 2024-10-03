package main

import (
	"golang.org/x/tools/go/analysis/multichecker"

	"gitlab.myteksi.net/dbops/Redis/internal/customvet/checks/setval"
)

func main() {
	multichecker.Main(
		setval.Analyzer,
	)
}
