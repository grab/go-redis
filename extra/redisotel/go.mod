module gitlab.myteksi.net/dbops/Redis/extra/redisotel/v8

go 1.15

replace gitlab.myteksi.net/dbops/Redis/v8 => ../..

replace gitlab.myteksi.net/dbops/Redis/extra/rediscmd/v8 => ../rediscmd

require (
	gitlab.myteksi.net/dbops/Redis/extra/rediscmd/v8 v8.0.0-00010101000000-000000000000
	gitlab.myteksi.net/dbops/Redis/v8 v8.11.5-grab1.0
	go.opentelemetry.io/otel v1.5.0
	go.opentelemetry.io/otel/sdk v1.4.1
	go.opentelemetry.io/otel/trace v1.5.0
)
