package setval_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"gitlab.myteksi.net/dbops/Redis/internal/customvet/checks/setval"
)

func Test(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, setval.Analyzer, "a")
}
