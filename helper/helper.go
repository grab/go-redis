package helper

import "gitlab.myteksi.net/dbops/Redis/v9/internal/util"

func ParseFloat(s string) (float64, error) {
	return util.ParseStringToFloat(s)
}

func MustParseFloat(s string) float64 {
	return util.MustParseFloat(s)
}
