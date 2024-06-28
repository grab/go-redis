package redis

import (
	"context"
	"crypto/tls"
	"net"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
)

// NewDynamicClient is similar to NewClient, but it uses a dynamic pool instead to provide dynamic connection management
func NewDynamicClient(opt *Options) *Client {
	opt.init()

	c := Client{
		baseClient: &baseClient{
			opt: opt,
		},
	}
	c.init()
	c.connPool = newDynamicConnPool(opt)

	return &c
}

func (c *Client) SetUsername(username string) {
	c.opt.Username = username
}

func (c *Client) SetPassword(password string) {
	c.opt.Password = password
}

func (c *Client) SetMaxRetries(maxRetries int) {
	if maxRetries == -1 {
		maxRetries = 0
	} else if maxRetries == 0 {
		maxRetries = 3
	}

	c.opt.MaxRetries = maxRetries
}

func (c *Client) SetMinRetryBackoff(minRetryBackoff time.Duration) {
	if minRetryBackoff == -1 {
		minRetryBackoff = 0
	} else if minRetryBackoff == 0 {
		minRetryBackoff = 8 * time.Millisecond
	}

	c.opt.MinRetryBackoff = minRetryBackoff
}

func (c *Client) SetMaxRetryBackoff(maxRetryBackoff time.Duration) {
	if maxRetryBackoff == -1 {
		maxRetryBackoff = 0
	} else if maxRetryBackoff == 0 {
		maxRetryBackoff = 512 * time.Millisecond
	}

	c.opt.MaxRetryBackoff = maxRetryBackoff
}

func (c *Client) SetDialTimeout(dialTimeout time.Duration) {
	if dialTimeout == 0 {
		dialTimeout = 5 * time.Second
	}

	c.opt.DialTimeout = dialTimeout
}

func (c *Client) SetReadTimeout(readTimeout time.Duration) {
	if readTimeout == -1 {
		readTimeout = 0
	} else if readTimeout == 0 {
		readTimeout = 3 * time.Second
	}

	c.opt.ReadTimeout = readTimeout
}

func (c *Client) SetWriteTimeout(writeTimeout time.Duration) {
	if writeTimeout == -1 {
		writeTimeout = 0
	} else if writeTimeout == 0 {
		writeTimeout = c.opt.ReadTimeout
	}

	c.opt.WriteTimeout = writeTimeout
}

func (c *Client) SetPoolFIFO(poolFIFO bool) {
	connPool, ok := c.connPool.(pool.DynamicPooler)
	if !ok {
		internal.Logger.Printf(context.TODO(), "SetPoolFIFO failed for non-dynamic client %s", c.getAddr())
		return
	}

	c.opt.PoolFIFO = poolFIFO
	connPool.SetPoolFIFO(poolFIFO)
}

func (c *Client) SetPoolSize(poolSize int) {
	connPool, ok := c.connPool.(pool.DynamicPooler)
	if !ok {
		internal.Logger.Printf(context.TODO(), "SetPoolSize failed for non-dynamic client %s", c.getAddr())
		return
	}

	if poolSize == 0 {
		poolSize = 10 * runtime.GOMAXPROCS(0)
	}

	c.opt.PoolSize = poolSize
	connPool.SetPoolSize(poolSize)
}

func (c *Client) SetMinIdleConns(minIdleConns int) {
	connPool, ok := c.connPool.(pool.DynamicPooler)
	if !ok {
		internal.Logger.Printf(context.TODO(), "SetMinIdleConns failed for non-dynamic client %s", c.getAddr())
		return
	}

	c.opt.MinIdleConns = minIdleConns
	connPool.SetMinIdleConns(minIdleConns)
}

func (c *Client) SetMaxIdleConns(maxIdleConns int) {
	connPool, ok := c.connPool.(pool.DynamicPooler)
	if !ok {
		internal.Logger.Printf(context.TODO(), "SetMaxIdleConns failed for non-dynamic client %s", c.getAddr())
		return
	}

	if maxIdleConns == -1 {
		maxIdleConns = 0
	} else if maxIdleConns == 0 {
		maxIdleConns = c.opt.PoolSize
	}

	c.opt.MaxIdleConns = maxIdleConns
	connPool.SetMaxIdleConns(maxIdleConns)
}

func (c *Client) SetConnMaxLifetime(ConnMaxLifetime time.Duration) {
	connPool, ok := c.connPool.(pool.DynamicPooler)
	if !ok {
		internal.Logger.Printf(context.TODO(), "SetConnMaxLifetime failed for non-dynamic client %s", c.getAddr())
		return
	}

	c.opt.ConnMaxLifetime = ConnMaxLifetime
	connPool.SetConnMaxLifetime(ConnMaxLifetime)
}

func (c *Client) SetPoolTimeout(poolTimeout time.Duration) {
	connPool, ok := c.connPool.(pool.DynamicPooler)
	if !ok {
		internal.Logger.Printf(context.TODO(), "SetPoolTimeout failed for non-dynamic client %s", c.getAddr())
		return
	}

	if poolTimeout == 0 {
		poolTimeout = c.opt.ReadTimeout + time.Second
	}

	c.opt.PoolTimeout = poolTimeout
	connPool.SetPoolTimeout(poolTimeout)
}

func (c *Client) SetConnMaxIdleTime(ConnMaxIdleTime time.Duration) {
	connPool, ok := c.connPool.(pool.DynamicPooler)
	if !ok {
		internal.Logger.Printf(context.TODO(), "SetConnMaxIdleTime failed for non-dynamic client %s", c.getAddr())
		return
	}

	if ConnMaxIdleTime == 0 {
		ConnMaxIdleTime = 5 * time.Minute
	}

	c.opt.ConnMaxIdleTime = ConnMaxIdleTime
	connPool.SetConnMaxIdleTime(ConnMaxIdleTime)
}

func (c *Client) SetTLSConfig(tlsConfig *tls.Config) {
	c.opt.TLSConfig = tlsConfig
}

func (c *Client) SetLimiter(limiter Limiter) {
	c.opt.Limiter = limiter
}

func newDynamicConnPool(opt *Options) *pool.DynamicConnPool {
	return pool.NewDynamicConnPool(&pool.Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			return opt.Dialer(ctx, opt.Network, opt.Addr)
		},
		PoolFIFO:        opt.PoolFIFO,
		PoolSize:        opt.PoolSize,
		MinIdleConns:    opt.MinIdleConns,
		MaxIdleConns:    opt.MaxIdleConns,
		ConnMaxLifetime: opt.ConnMaxLifetime,
		PoolTimeout:     opt.PoolTimeout,
		ConnMaxIdleTime: opt.ConnMaxIdleTime,
	})
}
