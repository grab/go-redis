module github.com/redis/go-redis/extra/rediscensus/v9

go 1.15

replace gitlab.myteksi.net/dbops/Redis/v9 => ../..

replace github.com/redis/go-redis/extra/rediscmd/v9 => ../rediscmd

require (
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.5.0
	gitlab.myteksi.net/dbops/Redis/v9 v9.5.0
	go.opencensus.io v0.24.0
)
