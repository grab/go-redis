package redis

import (
	"crypto/tls"
	"runtime"
	"time"

	"github.com/grab/redis/v8/internal"
)

// NewDynamicClusterClient is similar to NewClusterClient, but it supports dynamic connection pool management
// in exiting clients if NewClient option is not specific in ClusterOptions
func NewDynamicClusterClient(opt *ClusterOptions) *ClusterClient {
	if opt.NewClient == nil {
		opt.NewClient = NewDynamicClient
	}

	return NewClusterClient(opt)
}

func (c *ClusterClient) SetMaxRedirects(maxRedirects int) {
	if maxRedirects == -1 {
		maxRedirects = 0
	} else if maxRedirects == 0 {
		maxRedirects = 3
	}

	c.opt.MaxRedirects = maxRedirects
}

func (c *ClusterClient) SetReadOnly(readOnly bool) {
	c.opt.ReadOnly = readOnly
}

func (c *ClusterClient) SetRouteByLatency(routeByLatency bool) {
	if routeByLatency {
		c.opt.ReadOnly = true
		for _, node := range c.nodes.nodes {
			go node.updateLatency()
		}
	}
	c.opt.RouteByLatency = routeByLatency
}

func (c *ClusterClient) SetRouteRandomly(routeRandomly bool) {
	if routeRandomly {
		c.opt.ReadOnly = true
	}
	c.opt.RouteRandomly = routeRandomly
}

func (c *ClusterClient) SetUsername(username string) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetUsername(username)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetUsername failed: %s", err)
		return
	}

	c.opt.Username = username
}

func (c *ClusterClient) SetPassword(password string) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetPassword(password)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetPassword failed: %s", err)
		return
	}

	c.opt.Password = password
}

func (c *ClusterClient) SetMaxRetries(maxRetries int) {
	if maxRetries == 0 {
		maxRetries = -1
	}

	if err := c.applyUpdateFn(func(client *Client) {
		client.SetMaxRetries(maxRetries)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetMaxRetries failed: %s", err)
		return
	}

	c.opt.MaxRetries = maxRetries
}

func (c *ClusterClient) SetMinRetryBackoff(minRetryBackoff time.Duration) {
	if minRetryBackoff == -1 {
		minRetryBackoff = 0
	} else if minRetryBackoff == 0 {
		minRetryBackoff = 8 * time.Millisecond
	}

	if err := c.applyUpdateFn(func(client *Client) {
		client.SetMinRetryBackoff(minRetryBackoff)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetMinRetryBackoff failed: %s", err)
		return
	}

	c.opt.MinRetryBackoff = minRetryBackoff
}

func (c *ClusterClient) SetMaxRetryBackoff(maxRetryBackoff time.Duration) {
	if maxRetryBackoff == -1 {
		maxRetryBackoff = 0
	} else if maxRetryBackoff == 0 {
		maxRetryBackoff = 512 * time.Millisecond
	}

	if err := c.applyUpdateFn(func(client *Client) {
		client.SetMaxRetryBackoff(maxRetryBackoff)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetMaxRetryBackoff failed: %s", err)
		return
	}

	c.opt.MaxRetryBackoff = maxRetryBackoff
}

func (c *ClusterClient) SetDialTimeout(dialTimeout time.Duration) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetDialTimeout(dialTimeout)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetDialTimeout failed: %s", err)
		return
	}

	c.opt.DialTimeout = dialTimeout
}

func (c *ClusterClient) SetReadTimeout(readTimeout time.Duration) {
	if readTimeout == -1 {
		readTimeout = 0
	} else if readTimeout == 0 {
		readTimeout = 3 * time.Second
	}

	if err := c.applyUpdateFn(func(client *Client) {
		client.SetReadTimeout(readTimeout)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetReadTimeout failed: %s", err)
		return
	}

	c.opt.ReadTimeout = readTimeout
}

func (c *ClusterClient) SetWriteTimeout(writeTimeout time.Duration) {
	if writeTimeout == -1 {
		writeTimeout = 0
	} else if writeTimeout == 0 {
		writeTimeout = c.opt.ReadTimeout
	}

	if err := c.applyUpdateFn(func(client *Client) {
		client.SetWriteTimeout(writeTimeout)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetWriteTimeout failed: %s", err)
		return
	}

	c.opt.WriteTimeout = writeTimeout
}

func (c *ClusterClient) SetPoolFIFO(poolFIFO bool) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetPoolFIFO(poolFIFO)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetPoolFIFO failed: %s", err)
		return
	}

	c.opt.PoolFIFO = poolFIFO
}

func (c *ClusterClient) SetPoolSize(poolSize int) {
	if poolSize == 0 {
		poolSize = 5 * runtime.GOMAXPROCS(0)
	}

	if err := c.applyUpdateFn(func(client *Client) {
		client.SetPoolSize(poolSize)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetPoolSize failed: %s", err)
		return
	}

	c.opt.PoolSize = poolSize
}

func (c *ClusterClient) SetMinIdleConns(minIdleConns int) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetMinIdleConns(minIdleConns)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetMinIdleConns failed: %s", err)
		return
	}

	c.opt.MinIdleConns = minIdleConns
}

func (c *ClusterClient) SetMaxIdleConns(maxIdleConns int) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetMaxIdleConns(maxIdleConns)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetMaxIdleConns failed: %s", err)
		return
	}

	c.opt.MaxIdleConns = maxIdleConns
}

func (c *ClusterClient) SetMaxConnAge(maxConnAge time.Duration) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetMaxConnAge(maxConnAge)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetMaxConnAge failed: %s", err)
		return
	}

	c.opt.MaxConnAge = maxConnAge
}

func (c *ClusterClient) SetPoolTimeout(poolTimeout time.Duration) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetPoolTimeout(poolTimeout)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetPoolTimeout failed: %s", err)
		return
	}

	c.opt.PoolTimeout = poolTimeout
}

func (c *ClusterClient) SetIdleTimeout(idleTimeout time.Duration) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetIdleTimeout(idleTimeout)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetIdleTimeout failed: %s", err)
		return
	}

	c.opt.IdleTimeout = idleTimeout
}

func (c *ClusterClient) SetIdleCheckFrequency(idleCheckFrequency time.Duration) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetIdleCheckFrequency(idleCheckFrequency)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetIdleCheckFrequency failed: %s", err)
		return
	}

	c.opt.IdleCheckFrequency = idleCheckFrequency
}

func (c *ClusterClient) SetTLSConfig(tlsConfig *tls.Config) {
	if err := c.applyUpdateFn(func(client *Client) {
		client.SetTLSConfig(tlsConfig)
	}); err != nil {
		internal.Logger.Printf(c.Context(), "SetTLSConfig failed: %s", err)
		return
	}

	c.opt.TLSConfig = tlsConfig
}

func (c *ClusterClient) applyUpdateFn(fn func(client *Client)) error {
	nodes, err := c.nodes.All()
	if err != nil {
		return err
	}

	for _, node := range nodes {
		fn(node.Client)
	}
	return nil
}
