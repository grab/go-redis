module gitlab.myteksi.net/dbops/Redis/extra/rediscensus/v8

go 1.15

replace gitlab.myteksi.net/dbops/Redis/v8 => ../..

replace gitlab.myteksi.net/dbops/Redis/extra/rediscmd/v8 => ../rediscmd

require (
	gitlab.myteksi.net/dbops/Redis/extra/rediscmd/v8 v8.11.5-grab1.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	gitlab.myteksi.net/dbops/Redis/v8 v8.11.5-grab1.0
	go.opencensus.io v0.23.0
)
