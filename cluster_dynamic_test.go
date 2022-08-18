package redis_test

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Dynamic ClusterClient", func() {
	var client *redis.ClusterClient

	Context("retry", func() {
		timePingRequest := func() time.Duration {
			now := time.Now()
			err := client.Ping(ctx).Err()
			Expect(err).To(HaveOccurred())
			return time.Since(now)
		}

		const normalRedirects, normalMinBackoff, normalMaxBackoff = 3, time.Millisecond * 8, time.Millisecond * 512

		BeforeEach(func() {
			client = redis.NewDynamicClusterClient(&redis.ClusterOptions{
				Addrs:           []string{":1234"},
				MaxRedirects:    normalRedirects,
				MinRetryBackoff: normalMinBackoff,
				MaxRetryBackoff: normalMaxBackoff,
				ClusterSlots: func(ctx context.Context) ([]redis.ClusterSlot, error) {
					return []redis.ClusterSlot{{
						Start: 0,
						End:   16383,
						Nodes: []redis.ClusterNode{{
							Addr: ":1234",
						},
						}}}, nil
				},
			})
		})

		AfterEach(func() {
			Expect(client.Close()).NotTo(HaveOccurred())
		})

		It("maxRedirects update", func() {
			elapseNormalRedirect := timePingRequest()

			client.SetMaxRedirects(-1)
			elapseNoRedirects := timePingRequest()
			Expect(elapseNormalRedirect).To(BeNumerically(">", elapseNoRedirects, 10*time.Millisecond))

			client.SetMaxRedirects(normalRedirects * 2)
			elapseMoreRedirects := timePingRequest()
			Expect(elapseMoreRedirects).To(BeNumerically(">", elapseNormalRedirect, 10*time.Millisecond))

			log.Println("redirect elapsed", elapseNoRedirects, elapseNormalRedirect, elapseMoreRedirects)
		})

		It("retries backoff update", func() {
			elapseNormalRedirect := timePingRequest()

			client.SetMinRetryBackoff(normalMinBackoff / 2)
			client.SetMaxRetryBackoff(normalMaxBackoff / 2)
			elapseLessRetryBackoff := timePingRequest()

			client.SetMinRetryBackoff(normalMinBackoff * 2)
			client.SetMaxRetryBackoff(normalMaxBackoff * 2)
			elapseMoreRedirectBackoff := timePingRequest()

			log.Println("backoff elapsed", elapseLessRetryBackoff, elapseNormalRedirect, elapseMoreRedirectBackoff)
			Expect(elapseNormalRedirect).To(BeNumerically(">", elapseLessRetryBackoff, 10*time.Millisecond))
			Expect(elapseMoreRedirectBackoff).To(BeNumerically(">", elapseNormalRedirect, 10*time.Millisecond))
		})
	})
})
