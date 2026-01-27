module gitlab.myteksi.net/dbops/Redis/extra/redisotel/v9

go 1.19

replace gitlab.myteksi.net/dbops/Redis/v9 => ../..

replace gitlab.myteksi.net/dbops/Redis/extra/rediscmd/v9 => ../rediscmd

require (
	gitlab.myteksi.net/dbops/Redis/extra/rediscmd/v9 v9.17.3
	gitlab.myteksi.net/dbops/Redis/v9 v9.17.3
	go.opentelemetry.io/otel v1.22.0
	go.opentelemetry.io/otel/metric v1.22.0
	go.opentelemetry.io/otel/sdk v1.22.0
	go.opentelemetry.io/otel/trace v1.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	golang.org/x/sys v0.16.0 // indirect
)
