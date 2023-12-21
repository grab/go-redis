package redis_test

import (
	"fmt"
	"net"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/grab/redis/v8"
)

// ------------------------------------------------------------------------------
var _ = Describe("Dynamic Client", func() {
	var client *redis.Client

	Context("retry", func() {
		timePingRequest := func() time.Duration {
			now := time.Now()
			err := client.Ping(ctx).Err()
			Expect(err).To(HaveOccurred())
			return time.Since(now)
		}

		const normalRetries, normalMinBackoff, normalMaxBackoff = 3, time.Millisecond * 8, time.Millisecond * 512

		BeforeEach(func() {
			client = redis.NewDynamicClient(&redis.Options{
				Addr:            ":1234",
				MaxRetries:      normalRetries,
				MinRetryBackoff: normalMinBackoff,
				MaxRetryBackoff: normalMaxBackoff,
			})
		})

		AfterEach(func() {
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("maxRetries update", func() {
			elapseNormalRetry := timePingRequest()

			client.SetMaxRetries(-1)
			elapseNoRetries := timePingRequest()
			Expect(elapseNormalRetry).To(BeNumerically(">", elapseNoRetries, 10*time.Millisecond))

			client.SetMaxRetries(normalRetries * 2)
			elapseMoreRetries := timePingRequest()
			Expect(elapseMoreRetries).To(BeNumerically(">", elapseNormalRetry, 10*time.Millisecond))
		})

		It("retries backoff update", func() {
			elapseNormalRetry := timePingRequest()

			client.SetMinRetryBackoff(normalMinBackoff / 2)
			client.SetMaxRetryBackoff(normalMaxBackoff / 2)
			elapseLessRetryBackoff := timePingRequest()
			Expect(elapseNormalRetry).To(BeNumerically(">", elapseLessRetryBackoff, 10*time.Millisecond))

			client.SetMinRetryBackoff(normalMinBackoff * 2)
			client.SetMaxRetryBackoff(normalMaxBackoff * 2)
			elapseMoreRetryBackoff := timePingRequest()
			Expect(elapseMoreRetryBackoff).To(BeNumerically(">", elapseNormalRetry, 10*time.Millisecond))
		})
	})

	Context("timeout", func() {
		BeforeEach(func() {
			opt := redisOptions()
			opt.MaxIdleConns = -1 // need to ensure MaxIdleConn = -1 to force use of new connection
			client = redis.NewDynamicClient(opt)
		})

		AfterEach(func() {
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		checkTimeoutError := func(err error, shouldTimeout bool) {
			if shouldTimeout {
				Expect(err).To(HaveOccurred())
				Expect(err.(net.Error).Timeout()).To(BeTrue())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
		}

		testTimeout := func(shouldTimeout bool) {
			// Ping timeouts
			err := client.Ping(ctx).Err()
			checkTimeoutError(err, shouldTimeout)

			// Pipeline timeouts
			_, err = client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Ping(ctx)
				return nil
			})
			checkTimeoutError(err, shouldTimeout)

			// Subscribe timeouts
			if client.Options().WriteTimeout != 0 {
				pubsub := client.Subscribe(ctx)
				defer pubsub.Close()

				err = pubsub.Subscribe(ctx, "_")
				checkTimeoutError(err, shouldTimeout)
			}

			// Tx timeouts
			err = client.Watch(ctx, func(tx *redis.Tx) error {
				return tx.Ping(ctx).Err()
			})
			checkTimeoutError(err, shouldTimeout)

			// Tx Pipeline timeouts
			err = client.Watch(ctx, func(tx *redis.Tx) error {
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Ping(ctx)
					return nil
				})
				return err
			})
			checkTimeoutError(err, shouldTimeout)
		}

		It("read timeout update", func() {
			testTimeout(false)
			client.SetReadTimeout(time.Nanosecond)
			client.SetWriteTimeout(-1)
			testTimeout(true)
		})

		It("write timeout update", func() {
			testTimeout(false)
			client.SetReadTimeout(-1)
			client.SetWriteTimeout(time.Nanosecond)
			testTimeout(true)
		})

		It("dail timeout update", func() {
			testTimeout(false)
			client.SetDialTimeout(time.Nanosecond)
			testTimeout(true)
		})
	})

	It("limiter update", func() {
		opt := redisOptions()
		client = redis.NewDynamicClient(opt)
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
		client.SetLimiter(&errorLimiter{})
		Expect(client.Ping(ctx).Err()).To(Equal(limiterError))
		limiter := &normalLimiter{}
		client.SetLimiter(limiter)
		err := client.Conn(ctx).Get(ctx, "this-key-does-not-exist").Err()
		Expect(err).To(Equal(redis.Nil))
		Expect(limiter.count).To(Equal(1)) // should call execute wrapper
		Expect(limiter.errors).To(ContainElement(redis.Nil))
	})
})

var limiterError = fmt.Errorf("limiter error")

type errorLimiter struct {
}

func (l *errorLimiter) Execute(f func() error) error {
	return nil
}

func (*errorLimiter) Allow() error {
	return limiterError
}

func (*errorLimiter) ReportResult(result error) {
	// do nothing
}

type normalLimiter struct {
	count  int
	errors []error
}

func (l *normalLimiter) Execute(f func() error) error {
	l.count++
	return f()
}

func (*normalLimiter) Allow() error {
	return nil
}

func (l *normalLimiter) ReportResult(result error) {
	l.errors = append(l.errors, result)
}
