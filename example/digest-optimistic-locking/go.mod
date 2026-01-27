module gitlab.myteksi.net/dbops/Redis/example/digest-optimistic-locking

go 1.18

replace gitlab.myteksi.net/dbops/Redis/v9 => ../..

require (
	gitlab.myteksi.net/dbops/Redis/v9 v9.17.3
	github.com/zeebo/xxh3 v1.0.2
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
)
