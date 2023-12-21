package pool_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grab/redis/v8/internal/pool"
)

type dynamicPoolGetPutBenchmark struct {
	poolSize int
}

func (bm dynamicPoolGetPutBenchmark) String() string {
	return fmt.Sprintf("pool=%d", bm.poolSize)
}

func BenchmarkDynamicPoolGetPut(b *testing.B) {
	ctx := context.Background()
	benchmarks := []dynamicPoolGetPutBenchmark{
		{1},
		{2},
		{8},
		{32},
		{64},
		{128},
	}
	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			connPool := pool.NewDynamicConnPool(&pool.Options{
				Dialer:             dummyDialer,
				PoolSize:           bm.poolSize,
				MaxIdleConns:       bm.poolSize,
				PoolTimeout:        time.Second,
				IdleTimeout:        time.Hour,
				IdleCheckFrequency: time.Hour,
			})

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cn, err := connPool.Get(ctx)
					if err != nil {
						b.Fatal(err)
					}
					connPool.Put(ctx, cn)
				}
			})
		})
	}
}

type dynamicPoolGetRemoveBenchmark struct {
	poolSize int
}

func (bm dynamicPoolGetRemoveBenchmark) String() string {
	return fmt.Sprintf("pool=%d", bm.poolSize)
}

func BenchmarkDynamicPoolGetRemove(b *testing.B) {
	ctx := context.Background()
	benchmarks := []dynamicPoolGetRemoveBenchmark{
		{1},
		{2},
		{8},
		{32},
		{64},
		{128},
	}

	for _, bm := range benchmarks {
		b.Run(bm.String(), func(b *testing.B) {
			connPool := pool.NewDynamicConnPool(&pool.Options{
				Dialer:             dummyDialer,
				PoolSize:           bm.poolSize,
				MaxIdleConns:       bm.poolSize,
				PoolTimeout:        time.Second,
				IdleTimeout:        time.Hour,
				IdleCheckFrequency: time.Hour,
			})

			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					cn, err := connPool.Get(ctx)
					if err != nil {
						b.Fatal(err)
					}
					connPool.Remove(ctx, cn, nil)
				}
			})
		})
	}
}
