package pool_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-redis/redis/v8/internal/pool"
)

func assertPoolStats(pooler pool.DynamicPooler, hits, misses, timeouts, totals, idles, stales uint32) {
	Expect(pooler.Stats()).To(Equal(&pool.Stats{
		Hits:       hits,
		Misses:     misses,
		Timeouts:   timeouts,
		TotalConns: totals,
		IdleConns:  idles,
		StaleConns: stales,
	}))
}

var _ = Describe("ConnPool", func() {
	ctx := context.Background()
	var connPool pool.DynamicPooler

	BeforeEach(func() {
		connPool = pool.NewDynamicConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           10,
			MaxIdleConns:       10,
			PoolTimeout:        time.Hour,
			IdleTimeout:        time.Millisecond,
			IdleCheckFrequency: time.Millisecond,
		})
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("should create idle connections then safe close", func() {
		const minIdleConns = 10

		var (
			wg         sync.WaitGroup
			closedChan = make(chan struct{})
		)
		wg.Add(minIdleConns)
		connPool = pool.NewDynamicConnPool(&pool.Options{
			Dialer: func(ctx context.Context) (net.Conn, error) {
				wg.Done()
				<-closedChan
				return &net.TCPConn{}, nil
			},
			PoolSize:           10,
			MinIdleConns:       minIdleConns,
			MaxIdleConns:       10,
			PoolTimeout:        time.Hour,
			IdleTimeout:        time.Millisecond,
			IdleCheckFrequency: time.Millisecond * 500,
		})
		wg.Wait()

		// MinIdleConns should optimistically increase poolSize though actual conn creation is pending
		assertPoolStats(connPool, 0, 0, 0, 10, 10, 0)

		// no error since no idle conn queued yet during pool close and stats remains
		Expect(connPool.Close()).NotTo(HaveOccurred())
		assertPoolStats(connPool, 0, 0, 0, 10, 10, 0)

		close(closedChan) // release the channel to make all dail success

		// wait for 5ms and all conn put back idle list should fail and decrease pool size
		time.Sleep(time.Millisecond * 5)
		assertPoolStats(connPool, 0, 0, 0, 0, 0, 0)
	})

	It("should unblock client when conn is removed", func() {
		// Reserve one connection.
		cn, err := connPool.Get(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Reserve all other connections.
		var cns []*pool.Conn
		for i := 0; i < 9; i++ {
			cn, err := connPool.Get(ctx)
			Expect(err).NotTo(HaveOccurred())
			cns = append(cns, cn)
		}

		started := make(chan bool, 1)
		done := make(chan bool, 1)
		go func() {
			defer GinkgoRecover()

			started <- true
			_, err := connPool.Get(ctx)
			Expect(err).NotTo(HaveOccurred())
			done <- true

			connPool.Put(ctx, cn)
		}()
		<-started

		// Check that Get is blocked.
		select {
		case <-done:
			Fail("Get is not blocked")
		case <-time.After(time.Millisecond * 5):
			// ok
		}

		connPool.Remove(ctx, cn, nil)

		// Check that Get is unblocked.
		select {
		case <-done:
			// ok
		case <-time.After(time.Millisecond * 5):
			Fail("Get is not unblocked")
		}

		for _, cn := range cns {
			connPool.Put(ctx, cn)
		}
	})
})

var _ = Describe("MinIdleConns", func() {
	const poolSize, poolTimeout = 100, 100 * time.Millisecond
	ctx := context.Background()
	var minIdleConns int
	var connPool pool.DynamicPooler

	newConnPool := func() pool.DynamicPooler {
		connPool := pool.NewDynamicConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           poolSize,
			MinIdleConns:       minIdleConns,
			MaxIdleConns:       poolSize,
			PoolTimeout:        poolTimeout,
			IdleTimeout:        -1,
			IdleCheckFrequency: -1,
		})

		Eventually(func() int {
			return connPool.Len()
		}).Should(Equal(minIdleConns))
		return connPool
	}

	assert := func() {
		It("has idle connections when created", func() {
			Expect(connPool.Len()).To(Equal(minIdleConns))
			Expect(connPool.IdleLen()).To(Equal(minIdleConns))
		})

		Context("after Get", func() {
			var cn *pool.Conn

			BeforeEach(func() {
				var err error

				cn, err = connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())

				Eventually(func() int {
					return connPool.Len()
				}).Should(Equal(minIdleConns + 1))
			})

			It("has idle connections", func() {
				Expect(connPool.Len()).To(Equal(minIdleConns + 1))
				Expect(connPool.IdleLen()).To(Equal(minIdleConns))
			})

			Context("after Remove", func() {
				BeforeEach(func() {
					connPool.Remove(ctx, cn, nil)
				})

				It("has idle connections", func() {
					Expect(connPool.Len()).To(Equal(minIdleConns))
					Expect(connPool.IdleLen()).To(Equal(minIdleConns))
				})
			})
		})

		Describe("Get does not exceed pool size", func() {
			var mu sync.RWMutex
			var cns []*pool.Conn

			BeforeEach(func() {
				cns = make([]*pool.Conn, 0)

				perform(poolSize, func(_ int) {
					defer GinkgoRecover()

					cn, err := connPool.Get(ctx)
					Expect(err).NotTo(HaveOccurred())
					mu.Lock()
					cns = append(cns, cn)
					mu.Unlock()
				})

				Eventually(func() int {
					return connPool.Len()
				}).Should(BeNumerically(">=", poolSize))
			})

			It("Get is blocked", func() {
				var err error
				done := make(chan struct{})
				go func() {
					_, err = connPool.Get(ctx)
					close(done)
				}()

				// Get should be blocked until poolTimeout.
				select {
				case <-done:
					Fail("Get is not blocked")
				case <-time.After(time.Millisecond * 5):
					// ok
				}

				select {
				case <-done:
					Expect(err).To(Equal(pool.ErrPoolTimeout))
				case <-time.After(poolTimeout + time.Millisecond*5):
					Fail("Get is not unblocked")
				}
			})

			Context("after Put", func() {
				BeforeEach(func() {
					perform(len(cns), func(i int) {
						mu.RLock()
						connPool.Put(ctx, cns[i])
						mu.RUnlock()
					})

					Eventually(func() int {
						return connPool.Len()
					}).Should(Equal(poolSize))
				})

				It("pool.Len is back to normal", func() {
					Expect(connPool.Len()).To(Equal(poolSize))
					Expect(connPool.IdleLen()).To(Equal(poolSize))
				})
			})

			Context("after Remove", func() {
				BeforeEach(func() {
					perform(len(cns), func(i int) {
						mu.RLock()
						connPool.Remove(ctx, cns[i], nil)
						mu.RUnlock()
					})

					Eventually(func() int {
						return connPool.Len()
					}).Should(Equal(minIdleConns))
				})

				It("has idle connections", func() {
					Expect(connPool.Len()).To(Equal(minIdleConns))
					Expect(connPool.IdleLen()).To(Equal(minIdleConns))
				})
			})
		})

		Context("after Reap", func() {
			idleTimeout := time.Millisecond * 100
			BeforeEach(func() {
				connPool.SetIdleTimeout(idleTimeout)
				time.Sleep(idleTimeout + time.Millisecond*5) // After timeout
				n, err := connPool.ReapStaleConns()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(minIdleConns))
			})

			It("has idle connections", func() {
				Expect(connPool.Len()).To(Equal(minIdleConns))
				Expect(connPool.IdleLen()).To(Equal(minIdleConns))
			})
		})
	}

	Context("minIdleConns = 1", func() {
		BeforeEach(func() {
			minIdleConns = 1
			connPool = newConnPool()
		})

		AfterEach(func() {
			connPool.Close()
		})

		assert()
	})

	Context("minIdleConns = 32", func() {
		BeforeEach(func() {
			minIdleConns = 32
			connPool = newConnPool()
			time.Sleep(time.Millisecond * 5) // wait for 5ms to let idle connections add to pool
		})

		AfterEach(func() {
			connPool.Close()
		})

		assert()
	})
})

var _ = Describe("conns reaper", func() {
	const idleTimeout = time.Minute
	const maxAge = time.Hour

	ctx := context.Background()
	var connPool pool.DynamicPooler
	var conns, staleConns, closedConns []*pool.Conn

	assert := func(typ string) {
		BeforeEach(func() {
			closedConns = nil
			connPool = pool.NewDynamicConnPool(&pool.Options{
				Dialer:             dummyDialer,
				PoolSize:           10,
				MaxIdleConns:       10,
				IdleTimeout:        idleTimeout,
				MaxConnAge:         maxAge,
				PoolTimeout:        time.Second,
				IdleCheckFrequency: time.Hour,
				OnClose: func(cn *pool.Conn) error {
					closedConns = append(closedConns, cn)
					return nil
				},
			})

			conns = nil

			// add stale connections
			staleConns = nil
			for i := 0; i < 3; i++ {
				cn, err := connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				switch typ {
				case "idle":
					cn.SetUsedAt(time.Now().Add(-2 * idleTimeout))
				case "aged":
					cn.SetCreatedAt(time.Now().Add(-2 * maxAge))
				}
				conns = append(conns, cn)
				staleConns = append(staleConns, cn)
			}

			// add fresh connections
			for i := 0; i < 3; i++ {
				cn, err := connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				conns = append(conns, cn)
			}

			for _, cn := range conns {
				connPool.Put(ctx, cn)
			}

			Expect(connPool.Len()).To(Equal(6))
			Expect(connPool.IdleLen()).To(Equal(6))

			connPool.ReapStaleConns()
			Expect(connPool.Len()).To(Equal(3))
			Expect(connPool.IdleLen()).To(Equal(3))
		})

		AfterEach(func() {
			_ = connPool.Close()
			Expect(connPool.Len()).To(Equal(0))
			Expect(connPool.IdleLen()).To(Equal(0))
			Expect(len(closedConns)).To(Equal(len(conns)))
			Expect(closedConns).To(ConsistOf(conns))
		})

		It("reaps stale connections", func() {
			Expect(connPool.Len()).To(Equal(3))
			Expect(connPool.IdleLen()).To(Equal(3))
		})

		It("does not reap fresh connections", func() {
			connPool.ReapStaleConns()
			Expect(connPool.Len()).To(Equal(3))
			Expect(connPool.IdleLen()).To(Equal(3))
		})

		It("stale connections are closed", func() {
			Expect(len(closedConns)).To(Equal(len(staleConns)))
			Expect(closedConns).To(ConsistOf(staleConns))
		})

		It("pool is functional", func() {
			for j := 0; j < 3; j++ {
				var freeCns []*pool.Conn
				for i := 0; i < 3; i++ {
					cn, err := connPool.Get(ctx)
					Expect(err).NotTo(HaveOccurred())
					Expect(cn).NotTo(BeNil())
					freeCns = append(freeCns, cn)
				}

				Expect(connPool.Len()).To(Equal(3))
				Expect(connPool.IdleLen()).To(Equal(0))

				cn, err := connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cn).NotTo(BeNil())
				conns = append(conns, cn)

				Expect(connPool.Len()).To(Equal(4))
				Expect(connPool.IdleLen()).To(Equal(0))

				connPool.Remove(ctx, cn, nil)

				Expect(connPool.Len()).To(Equal(3))
				Expect(connPool.IdleLen()).To(Equal(0))

				for _, cn := range freeCns {
					connPool.Put(ctx, cn)
				}

				Expect(connPool.Len()).To(Equal(3))
				Expect(connPool.IdleLen()).To(Equal(3))
			}
		})
	}

	assert("idle")
	assert("aged")
})

var _ = Describe("race", func() {
	ctx := context.Background()
	var connPool pool.DynamicPooler
	var C, N int

	BeforeEach(func() {
		C, N = 10, 1000
		if testing.Short() {
			C = 4
			N = 100
		}
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("does not happen on Get, Put, and Remove", func() {
		connPool = pool.NewDynamicConnPool(&pool.Options{
			Dialer:             dummyDialer,
			PoolSize:           10,
			MaxIdleConns:       10,
			PoolTimeout:        time.Minute,
			IdleTimeout:        time.Millisecond,
			IdleCheckFrequency: time.Millisecond,
		})

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					connPool.Put(ctx, cn)
				}
			}
		}, func(id int) {
			for i := 0; i < N; i++ {
				cn, err := connPool.Get(ctx)
				Expect(err).NotTo(HaveOccurred())
				if err == nil {
					connPool.Remove(ctx, cn, nil)
				}
			}
		})
	})
})

func getConnsExpectNoErr(pooler pool.DynamicPooler, connCount int) []*pool.Conn {
	cns := make([]*pool.Conn, connCount)
	perform(connCount, func(idx int) {
		cn, err := pooler.Get(context.Background())
		Expect(err).NotTo(HaveOccurred())
		cns[idx] = cn
	})
	return cns
}

func putConns(pooler pool.DynamicPooler, cns []*pool.Conn) {
	for _, cn := range cns {
		pooler.Put(context.Background(), cn)
	}
}

var _ = Describe("request timeout", func() {
	var connPool pool.DynamicPooler

	ctx := context.Background()
	const poolSize = 10
	const poolTimeout, shortTimeout, longTimeout = time.Millisecond * 10, time.Millisecond * 5, time.Millisecond * 15

	BeforeEach(func() {
		connPool = pool.NewDynamicConnPool(&pool.Options{
			Dialer:       dummyDialer,
			PoolSize:     poolSize,
			MaxIdleConns: poolSize,
			PoolTimeout:  poolTimeout,
		})
	})

	AfterEach(func() {
		connPool.Close()
	})

	It("request timeout", func() {
		getConnsExpectNoErr(connPool, poolSize) // use all connections

		longCtx, longCancel := context.WithTimeout(ctx, longTimeout)
		defer longCancel()
		_, longErr := connPool.Get(longCtx)
		Expect(longErr).To(Equal(pool.ErrPoolTimeout))

		shortCtx, shortCancel := context.WithTimeout(ctx, shortTimeout)
		defer shortCancel()
		_, shortErr := connPool.Get(shortCtx)
		Expect(shortErr).To(Equal(shortCtx.Err()))
	})

})

// check event happens roughly at the given event time with acceptable given deviation
// i.e, it matches beforeFn right before event time and afterFn right after event time.
func checkEvent(eventTime time.Duration, deviation time.Duration, beforeFn func(), afterFn func()) chan bool {
	waitCh := make(chan bool)
	time.AfterFunc(eventTime-deviation, func() {
		beforeFn()
	})
	time.AfterFunc(eventTime+deviation, func() {
		afterFn()
		close(waitCh)
	})

	return waitCh
}

func assertStalesConn(reaper pool.Reaper, staleCount int) {
	count, err := reaper.ReapStaleConns()
	Expect(count).To(Equal(staleCount))
	Expect(err).NotTo(HaveOccurred())
}

// use 1 connection and update usedAt timestamp
func getPutOneConn(pooler pool.DynamicPooler) *pool.Conn {
	ctx := context.Background()
	cn, err := pooler.Get(ctx)
	Expect(err).NotTo(HaveOccurred())
	pooler.Put(ctx, cn)
	cn.SetUsedAt(time.Now())
	return cn
}

var _ = Describe("dynamic update", func() {
	ctx := context.Background()
	var connPool pool.DynamicPooler

	AfterEach(func() {
		if connPool != nil {
			_ = connPool.Close()
		}
	})

	Context("pool changes", func() {
		const poolSize, minIdleConns, maxIdleConns = 10, 2, 4 // please ensure poolSize >= minIdleConns * 2 + 2
		const poolTimeout = time.Millisecond * 50

		BeforeEach(func() {
			connPool = pool.NewDynamicConnPool(&pool.Options{
				Dialer:       dummyDialer,
				PoolSize:     poolSize,
				MinIdleConns: minIdleConns,
				MaxIdleConns: maxIdleConns,
				PoolTimeout:  poolTimeout,
			})
		})

		It("PoolSize increases", func() {
			getConnsExpectNoErr(connPool, poolSize) // use all connections

			Expect(connPool.Len()).To(Equal(poolSize))
			done := make(chan struct{})
			go func() {
				_, _ = connPool.Get(ctx)
				close(done)
			}()

			select {
			case <-done:
				Fail("should be blocked")
			case <-time.After(time.Millisecond * 5):
			}

			connPool.SetPoolSize(poolSize + 10)

			select {
			case <-done:
			case <-time.After(time.Millisecond * 5):
				Fail("should be unblocked after pool size increase")
			}

			getConnsExpectNoErr(connPool, 9) // use the rest 9 connections

			Expect(connPool.Len()).To(Equal(poolSize + 10))
		})

		It("PoolSize decreases", func() {
			Expect(connPool.Len()).To(Equal(minIdleConns))
			Expect(connPool.IdleLen()).To(Equal(minIdleConns))

			time.Sleep(time.Millisecond * 5) // wait for all idle connection creation stable
			connPool.SetPoolSize(minIdleConns - 1)

			Expect(connPool.Len()).To(Equal(minIdleConns - 1))
			Expect(connPool.IdleLen()).To(Equal(minIdleConns - 1))

			getConnsExpectNoErr(connPool, minIdleConns-1) // use all connections

			// all get request should hit and use up all idle connections
			assertPoolStats(connPool, minIdleConns-1, 0, 0, minIdleConns-1, 0, 0)

			done := make(chan struct{})
			go func() {
				_, _ = connPool.Get(ctx)
				close(done)
			}()

			select {
			case <-done:
				Fail("should be blocked")
			case <-time.After(time.Millisecond * 5):
			}
		})

		It("MinIdleConns increases", func() {
			Expect(connPool.IdleLen()).To(Equal(minIdleConns))

			connPool.SetMinIdleConns(minIdleConns + 1)

			Expect(connPool.IdleLen()).To(Equal(minIdleConns + 1))

			time.Sleep(time.Millisecond * 5) // wait for 5ms for idle conn to be created

			getConnsExpectNoErr(connPool, minIdleConns+1) // use all connections

			time.Sleep(time.Millisecond * 5) // wait for 5ms for idle conn to be created

			// all get request should hit and new idle connections created
			Expect(int(connPool.Stats().Hits)).To(Equal(minIdleConns + 1))
			Expect(connPool.IdleLen()).To(Equal(minIdleConns + 1))
		})

		It("MinIdleConns should not overflow", func() {
			Expect(connPool.IdleLen()).To(Equal(minIdleConns))

			connPool.SetMinIdleConns(maxIdleConns + 1)

			Expect(connPool.IdleLen()).To(Equal(maxIdleConns)) // min idle conn should not overflow maxIdleConns
		})

		It("MinIdleConns decreases", func() {
			Expect(connPool.IdleLen()).To(Equal(minIdleConns))

			connPool.SetMinIdleConns(minIdleConns - 1)

			Expect(connPool.IdleLen()).To(Equal(minIdleConns)) // idle conn should not be removed when idleConnSize shrinks

			time.Sleep(time.Millisecond * 5) // wait for 5ms for idle conn to be created

			getConnsExpectNoErr(connPool, minIdleConns) // use all connections

			time.Sleep(time.Millisecond * 5) // wait for 5ms for idle conn to be created

			// all get request should hit and new idle connections created
			Expect(int(connPool.Stats().Hits)).To(Equal(minIdleConns))
			Expect(connPool.IdleLen()).To(Equal(minIdleConns - 1))
		})

		It("MaxIdleConns increases", func() {
			cns := getConnsExpectNoErr(connPool, poolSize)
			putConns(connPool, cns)

			Expect(connPool.IdleLen()).To(Equal(maxIdleConns)) // conn put back should be bound by MaxIdleConns

			connPool.SetMaxIdleConns(maxIdleConns + 1)

			cns = getConnsExpectNoErr(connPool, poolSize)
			putConns(connPool, cns)
			Expect(connPool.IdleLen()).To(Equal(maxIdleConns + 1)) // bound should increase after MaxIdleConns increases
		})

		It("MaxIdleConns decreases", func() {
			cns := getConnsExpectNoErr(connPool, poolSize)
			putConns(connPool, cns)

			Expect(connPool.IdleLen()).To(Equal(maxIdleConns))

			connPool.SetMaxIdleConns(maxIdleConns - 1)

			Expect(connPool.IdleLen()).To(Equal(maxIdleConns - 1)) // idle conn should not be removed when MaxIdleConns shrinks
		})

		It("PoolTimeout changes", func() {
			getConnsExpectNoErr(connPool, poolSize) // use all connections

			assert := func(poolTimeout time.Duration) {
				connPool.SetPoolTimeout(poolTimeout)

				var err error
				waitCh := checkEvent(poolTimeout, time.Millisecond*5, func() {
					defer GinkgoRecover()
					Expect(err).NotTo(HaveOccurred())
				}, func() {
					defer GinkgoRecover()
					Expect(err).To(Equal(pool.ErrPoolTimeout))
				})
				_, err = connPool.Get(ctx)
				<-waitCh
			}

			assert(poolTimeout)
			assert(poolTimeout / 2)
			assert(poolTimeout * 2)
		})
	})

	Context("MaxConnAge changes", func() {
		const maxConnAge = time.Millisecond * 50

		BeforeEach(func() {
			connPool = pool.NewDynamicConnPool(&pool.Options{
				Dialer:       dummyDialer,
				PoolSize:     2,
				MaxIdleConns: 2,
				MaxConnAge:   maxConnAge,
			})
			getPutOneConn(connPool) // create one conn
		})

		assert := func(maxConnAge time.Duration) {
			It("check", func() {
				connPool.SetMaxConnAge(maxConnAge)

				waitCh := checkEvent(maxConnAge, time.Millisecond*5, func() {
					defer GinkgoRecover()
					assertStalesConn(connPool, 0)
				}, func() {
					defer GinkgoRecover()
					assertStalesConn(connPool, 1)
				})
				time.AfterFunc(maxConnAge/2, func() {
					getPutOneConn(connPool) // MaxConnAge stale check should not be impacted by conn last used time
				})

				<-waitCh
			})
		}

		assert(maxConnAge)
		assert(maxConnAge / 2)
		assert(maxConnAge * 2)
	})

	Context("IdleTimeout changes", func() {
		BeforeEach(func() {
			connPool = pool.NewDynamicConnPool(&pool.Options{
				Dialer:       dummyDialer,
				PoolSize:     2,
				MaxIdleConns: 2,
				IdleTimeout:  time.Second,
			})
		})

		// need to ensure idleTimeout >= 1 second since usedAt is at unit of 1 second
		assert := func(idleTimeout time.Duration) {
			It("check", func() {
				connPool.SetIdleTimeout(idleTimeout)
				getPutOneConn(connPool) // create one conn

				time.Sleep(time.Millisecond * 50)

				cn := getPutOneConn(connPool) // Idle stale check is impacted by conn last used time
				expiredAfter := cn.UsedAt().Add(idleTimeout).Sub(time.Now())

				if expiredAfter < 0 { // already expired, should reap directly
					assertStalesConn(connPool, 1)
				} else { // reap around expiry time
					waitCh := checkEvent(expiredAfter, time.Millisecond*5, func() {
						defer GinkgoRecover()
						assertStalesConn(connPool, 0)
					}, func() {
						defer GinkgoRecover()
						assertStalesConn(connPool, 1)
					})
					<-waitCh
				}
			})
		}

		assert(time.Millisecond * 500)
		assert(time.Millisecond * 1000)
		assert(time.Millisecond * 1500)
	})

	It("IdleCheckFrequency changes", func() {
		minIdleCheckFrequency := time.Second // this should be the value used in actual implementation
		idleTimeout := time.Millisecond * 50
		connPool = pool.NewDynamicConnPool(&pool.Options{
			Dialer:       dummyDialer,
			PoolSize:     2,
			MaxIdleConns: 2,
			IdleTimeout:  idleTimeout,
		})
		defer connPool.Close()
		getPutOneConn(connPool) // create one conn
		Expect(connPool.IdleLen()).To(Equal(1))

		// originally there is no idle check, so idle len remains even if connection is expired.
		time.Sleep(idleTimeout + time.Millisecond*5)
		Expect(connPool.IdleLen()).To(Equal(1))

		// increase idleCheckFrequency to larger than minIdleCheckFrequency, shall wait for next check to remove
		connPool.SetIdleCheckFrequency(minIdleCheckFrequency + time.Millisecond*50)
		<-checkEvent(minIdleCheckFrequency+time.Millisecond*50, time.Millisecond*5, func() {
			defer GinkgoRecover()
			Expect(connPool.IdleLen()).To(Equal(1))
		}, func() {
			defer GinkgoRecover()
			Expect(connPool.IdleLen()).To(Equal(0))
		})

		getPutOneConn(connPool) // create one conn
		Expect(connPool.IdleLen()).To(Equal(1))
		connPool.SetIdleCheckFrequency(0) // disable the check, shall no longer remove
		time.Sleep(minIdleCheckFrequency + time.Millisecond*5)
		Expect(connPool.IdleLen()).To(Equal(1))
	})
})

var _ = Describe("NewConn with poolSize changes", func() {
	It("NewConn for PubSub", func() {
		var closed []*pool.Conn
		const poolSize = 10

		connPool := pool.NewDynamicConnPool(&pool.Options{
			Dialer: dummyDialer,
			OnClose: func(conn *pool.Conn) error {
				closed = append(closed, conn)
				return nil
			},
			PoolSize:     poolSize,
			MaxIdleConns: poolSize,
		})
		defer connPool.Close()

		cns := getConnsExpectNoErr(connPool, poolSize)

		_, err := connPool.Get(context.Background())
		Expect(err).To(Equal(pool.ErrPoolTimeout)) // Get should be blocked by poolSize constraint

		// NewConn should not be blocked by poolSize constraint
		newConn1, err1 := connPool.NewConn(context.Background())
		newConn2, err2 := connPool.NewConn(context.Background())
		Expect(err1).NotTo(HaveOccurred())
		Expect(err2).NotTo(HaveOccurred())

		Expect(connPool.Len()).To(Equal(poolSize + 2)) // poolSize should overflow
		Expect(len(closed)).To(Equal(0))               // ongoing connections should not be closed

		// after all cns release, should expect 2 are closed due to overflow and the rest are put in idle list
		putConns(connPool, cns)
		Expect(connPool.Len()).To(Equal(poolSize))
		Expect(connPool.IdleLen()).To(Equal(poolSize - 2))
		Expect(len(closed)).To(Equal(2))
		Expect(cns).To(ContainElements(closed))

		// pool size shrink should close all idle connections but not those NewConn
		connPool.SetPoolSize(1)
		Expect(connPool.Len()).To(Equal(2))
		Expect(connPool.IdleLen()).To(Equal(0))
		Expect(len(closed)).To(Equal(poolSize))
		Expect(closed).To(ConsistOf(cns))

		_, err = connPool.Get(context.Background())
		Expect(err).To(Equal(pool.ErrPoolTimeout)) // Get should still be blocked

		_ = connPool.CloseConn(newConn1)
		_ = connPool.CloseConn(newConn2)
		Expect(closed).To(ContainElements(newConn1, newConn2)) // conn created via NewConn should invoke OnClose as well
		Expect(connPool.Len()).To(Equal(0))

		_, err = connPool.Get(context.Background())
		Expect(err).NotTo(HaveOccurred()) // Get should not be blocked since pool is not full
	})
})
