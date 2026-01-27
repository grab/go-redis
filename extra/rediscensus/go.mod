module gitlab.myteksi.net/dbops/Redis/extra/rediscensus/v9

go 1.19

replace gitlab.myteksi.net/dbops/Redis/v9 => ../..

replace gitlab.myteksi.net/dbops/Redis/extra/rediscmd/v9 => ../rediscmd

require (
	gitlab.myteksi.net/dbops/Redis/extra/rediscmd/v9 v9.17.3
	gitlab.myteksi.net/dbops/Redis/v9 v9.17.3
	go.opencensus.io v0.24.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
)
