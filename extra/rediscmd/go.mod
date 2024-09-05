module github.com/redis/go-redis/extra/rediscmd/v9

go 1.15

replace gitlab.myteksi.net/dbops/Redis/v9 => ../..

require (
	github.com/bsm/ginkgo/v2 v2.12.0
	github.com/bsm/gomega v1.27.10
	gitlab.myteksi.net/dbops/Redis/v9 v9.5.0
)
