package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.myteksi.net/dbops/Redis/v9/internal"
)

const (
	// connReqsQueueSize: buffered chan size of connection opener, this value should be larger than the maximum typical
	// value used for poolSize, otherwise it might block ALL calls in Pool until pending connection request is satisfied
	connReqsQueueSize = 1000000
)

type Setter interface {
	SetPoolFIFO(poolFIFO bool)
	SetPoolSize(poolSize int)
	SetMinIdleConns(minIdleConns int)
	SetMaxIdleConns(maxIdleConns int)
	SetConnMaxLifetime(ConnMaxLifetime time.Duration)
	SetConnMaxIdleTime(ConnMaxIdleTime time.Duration)
	SetPoolTimeout(poolTimeout time.Duration)
}

type DynamicPooler interface {
	Pooler
	Setter
}

type DynamicConnPool struct {
	cfg *Options

	dialErrorsNum uint32 // atomic

	lastDialError atomic.Value

	connsMu      sync.Mutex
	idleConns    []*Conn
	poolSize     int
	idleConnsLen int

	stats Stats

	_closed  uint32 // atomic
	closedCh chan struct{}
	reaperCh chan struct{}

	connReqs chan ctxConnChan
}

var _ DynamicPooler = (*DynamicConnPool)(nil)

func NewDynamicConnPool(opt *Options) *DynamicConnPool {
	p := &DynamicConnPool{
		cfg:      opt,
		closedCh: make(chan struct{}),
		connReqs: make(chan ctxConnChan, connReqsQueueSize),
	}

	p.connsMu.Lock()
	p.checkMinIdleConnsLocked()
	p.connsMu.Unlock()

	go p.handleConnReqs()

	return p
}

// NewConn always creates a new connection w/o blocking by PoolSize constraint, so it might cause poolSize overflow
// temporarily but future connection release might be discarded to bring the poolSize back within threshold.
func (p *DynamicConnPool) NewConn(ctx context.Context) (*Conn, error) {
	cn, err := p.dialConn(ctx)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// no more new dedicated connection should be open after pool closed
	if p.closed() {
		_ = cn.Close()
		return nil, ErrClosed
	}

	p.poolSize++
	return cn, nil
}

// CloseConn closes a normal connection w/o putting it back to idle list
func (p *DynamicConnPool) CloseConn(cn *Conn) error {
	p.connsMu.Lock()
	p.poolSize--
	p.checkMinIdleConnsLocked()
	p.connsMu.Unlock()

	return p.closeConn(cn)
}

// Get returns existed connection from the pool or creates a new one.
func (p *DynamicConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if p.isCtxExpired(ctx) {
		return nil, ctx.Err()
	}

	cn, err := p.getFromIdlePool()
	if cn != nil || err != nil {
		return cn, err
	}

	if p.poolHasSpace() {
		return p.createNewConn(ctx)
	}

	return p.waitForConn(ctx)
}

func (p *DynamicConnPool) getFromIdlePool() (*Conn, error) {
	for {
		p.connsMu.Lock()
		cn, err := p.popIdleLocked()
		p.connsMu.Unlock()

		if err != nil {
			return nil, err
		}

		if cn == nil {
			break
		}

		if !p.isHealthyConn(cn) {
			_ = p.CloseConn(cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)
	return nil, nil
}

func (p *DynamicConnPool) poolHasSpace() bool {
	p.connsMu.Lock()
	hasSpace := p.poolSize < p.cfg.PoolSize
	p.connsMu.Unlock()
	return hasSpace
}

func (p *DynamicConnPool) createNewConn(ctx context.Context) (*Conn, error) {
	p.connsMu.Lock()
	hasSpace := p.poolSize < p.cfg.PoolSize
	if hasSpace {
		p.poolSize++
	}
	p.connsMu.Unlock()

	if !hasSpace {
		return nil, errors.New("connection pool is full")
	}

	cn, err := p.dialConn(ctx)

	if err != nil {
		p.connsMu.Lock()
		p.poolSize--
		p.connsMu.Unlock()
	}
	return cn, err
}

func (p *DynamicConnPool) waitForConn(ctx context.Context) (*Conn, error) {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req := ctxConnChan{
		ctx:      childCtx,
		connChan: make(chan connReq, 1),
	}

	p.connReqs <- req

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.cfg.PoolTimeout)

	select {
	case <-ctx.Done():
		p.stopPutBackTimer(timer)
		p.checkPutConnAfterTimeout(req)
		return nil, ctx.Err()
	case connChan, ok := <-req.connChan:
		p.stopPutBackTimer(timer)
		// underlying connChan is closed, this happens during pool close, return ErrClosed immediately
		if !ok {
			return nil, ErrClosed
		}

		return connChan.conn, connChan.err
	case <-timer.C:
		cancel()
		timers.Put(timer)
		p.checkPutConnAfterTimeout(req)
		atomic.AddUint32(&p.stats.Timeouts, 1)

		return nil, ErrPoolTimeout
	}
}

// Put try to put a connection back to idle list, discard if poolSize is already full.
func (p *DynamicConnPool) Put(ctx context.Context, cn *Conn) {
	if cn.rd.Buffered() > 0 {
		internal.Logger.Printf(ctx, "Conn has unread data")
		p.Remove(ctx, cn, BadConnError{})
		return
	}

	p.putConn(cn, nil)
}

// Remove closes a faulty connection
func (p *DynamicConnPool) Remove(ctx context.Context, cn *Conn, reason error) {
	p.connsMu.Lock()
	p.poolSize--
	p.checkMinIdleConnsLocked()
	p.connsMu.Unlock()

	_ = p.closeConn(cn)
}

// Len returns total number of open connections.
func (p *DynamicConnPool) Len() int {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	return p.poolSize
}

// IdleLen returns number of idle connections.
func (p *DynamicConnPool) IdleLen() int {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	return p.idleConnsLen
}

// Stats returns statistic about connection pool.
func (p *DynamicConnPool) Stats() *Stats {
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns: uint32(p.Len()),
		IdleConns:  uint32(p.IdleLen()),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

// Close closes the pool and clean up all idle connections, no more new Get is permitted after pool closed, but ongoing
// process will continue to run with new connection creation
func (p *DynamicConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}
	close(p.closedCh)

	p.connsMu.Lock()
	// clean up idle connections, ongoing request shall rely on new connection creation.
	conns := p.idleConns
	p.idleConns = nil
	p.idleConnsLen -= len(conns)
	p.poolSize -= len(conns)
	p.connsMu.Unlock()

	var firstErr error
	for _, cn := range conns {
		err := p.closeConn(cn)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// SetPoolFIFO sets the pool idle connection retrieval order, next pop idle connection operation use the updated config.
func (p *DynamicConnPool) SetPoolFIFO(poolFIFO bool) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.cfg.PoolFIFO = poolFIFO
}

// SetPoolSize sets the maximum number of open connections could be used.
//
// If PoolSize increases, no action required, next connReq attempt will check against the updated config.
//
// If PoolSize decreases, if poolSize overflows, remove extra connections from idle connection pool.
// Note that it might happen that poolSize still overflows after all idle connections are removed.
// So released connection from ongoing requests or addIdleConn process should check against the updated config
// before putting back to idle list to ensure eventual satisfaction of the shrunken PoolSize
func (p *DynamicConnPool) SetPoolSize(poolSize int) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.cfg.PoolSize = poolSize
	removeCount := p.poolSize - p.cfg.PoolSize
	p.removeIdleConns(removeCount)
}

// SetMinIdleConns sets the minimum number of connections in the idle connection pool.
func (p *DynamicConnPool) SetMinIdleConns(minIdleConns int) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.cfg.MinIdleConns = minIdleConns
	p.checkMinIdleConnsLocked()
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
//
// If MaxIdleConns increases, no action required, next conn release/idle conn creation will check against the updated config.
//
// If MaxIdleConns decreases, if idle connections overflows, remove extra connections from idle connection pool.
func (p *DynamicConnPool) SetMaxIdleConns(maxIdleConns int) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.cfg.MaxIdleConns = maxIdleConns
	removeCount := len(p.idleConns) - maxIdleConns
	p.removeIdleConns(removeCount)
}

// SetConnMaxLifetime  sets the connection age at which a connection should be treated as stale.
func (p *DynamicConnPool) SetConnMaxLifetime(ConnMaxLifetime time.Duration) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.cfg.ConnMaxLifetime = ConnMaxLifetime
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle in the idle connection pool.
func (p *DynamicConnPool) SetConnMaxIdleTime(ConnMaxIdleTime time.Duration) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.cfg.ConnMaxIdleTime = ConnMaxIdleTime
}

// SetPoolTimeout sets the PoolTimeout config, no action required, next waiting request will use the updated config.
func (p *DynamicConnPool) SetPoolTimeout(poolTimeout time.Duration) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.cfg.PoolTimeout = poolTimeout
}

func (p *DynamicConnPool) handleConnReqs() {
	for req := range p.connReqs {

		// keep retry until either ctx cancel, a non-stale idle conn is found or a new conn creation is possible
		for {
			if p.isCtxExpired(req.ctx) {
				break
			}

			cn, err := p.getFromIdlePool()
			// either conn is getting from idle list, created or there is error during new connection creation
			if cn == nil || err != nil {
				if p.poolHasSpace() {
					cn, err = p.createNewConn(req.ctx)
				}
			}

			if cn != nil || err != nil {
				req.connChan <- connReq{
					conn: cn,
					err:  err,
				}
				break
			}

			time.Sleep(time.Millisecond)
		}
	}
}

func (p *DynamicConnPool) popIdleLocked() (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	n := len(p.idleConns)
	if n == 0 {
		return nil, nil
	}

	var cn *Conn
	if p.cfg.PoolFIFO {
		cn = p.idleConns[0]
		p.idleConns = p.idleConns[1:]
	} else {
		cn = p.idleConns[n-1]
		p.idleConns = p.idleConns[:n-1]
	}
	p.idleConnsLen--
	p.checkMinIdleConnsLocked()
	return cn, nil
}

func (p *DynamicConnPool) checkMinIdleConnsLocked() {
	if p.cfg.MinIdleConns == 0 {
		return
	}
	for p.poolSize < p.cfg.PoolSize && p.idleConnsLen < p.cfg.MinIdleConns && p.idleConnsLen < p.cfg.MaxIdleConns {
		p.poolSize++
		p.idleConnsLen++
		go p.addIdleConn()
	}
}

func (p *DynamicConnPool) addIdleConn() {
	cn, err := p.dialConn(context.TODO())
	if err != nil {
		p.poolSize--
		p.idleConnsLen--
		return
	}

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// Don't add new idle connection if pool is closed or poolSize/idleConnsLen overflows
	if p.closed() || p.poolSize > p.cfg.PoolSize || p.idleConnsLen > p.cfg.MaxIdleConns {
		_ = cn.Close()
		p.poolSize--
		p.idleConnsLen--
		return
	}

	p.idleConns = append(p.idleConns, cn)
}

func (p *DynamicConnPool) removeIdleConns(removeCount int) {
	if removeCount <= 0 {
		return
	}

	if maxRemove := len(p.idleConns); removeCount > maxRemove {
		removeCount = maxRemove
	}

	// remove extra connections from idle connection pool, prefer remove older connections
	closing := p.idleConns[:removeCount]
	p.idleConns = p.idleConns[removeCount:]
	p.poolSize -= len(closing)
	p.idleConnsLen -= len(closing)
	for _, cn := range closing {
		_ = p.closeConn(cn)
	}
}

func (p *DynamicConnPool) dialConn(ctx context.Context) (*Conn, error) {
	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.cfg.PoolSize) {
		return nil, p.getLastDialError()
	}

	netConn, err := p.cfg.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.cfg.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	cn := NewConn(netConn)
	return cn, nil
}

func (p *DynamicConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.cfg.Dialer(context.Background())
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (p *DynamicConnPool) setLastDialError(err error) {
	p.lastDialError.Store(err)
}

func (p *DynamicConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(error)
	return err
}

func (p *DynamicConnPool) stopPutBackTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timers.Put(timer)
}

// checkPutConnAfterTimeout: conn could be emitted right before timeout, put back the connection in this case
func (p *DynamicConnPool) checkPutConnAfterTimeout(req ctxConnChan) {
	select {
	default:
	case connChan, ok := <-req.connChan:
		if ok && connChan.conn != nil {
			p.putConn(connChan.conn, connChan.err)
		}
	}
}

func (p *DynamicConnPool) putConn(cn *Conn, err error) {
	// bad connection, should not re-use the connection
	if err != nil {
		return
	}

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// we might need to close surplus connection when PoolSize shrinks or DB is closed
	if p.closed() || p.poolSize > p.cfg.PoolSize || len(p.idleConns) >= p.cfg.MaxIdleConns {
		p.poolSize--
		_ = p.closeConn(cn)
		return
	}

	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
}

func (p *DynamicConnPool) closeConn(cn *Conn) error {
	return cn.Close()
}

func (p *DynamicConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *DynamicConnPool) isHealthyConn(cn *Conn) bool {
	now := time.Now()

	if p.cfg.ConnMaxLifetime > 0 && now.Sub(cn.createdAt) >= p.cfg.ConnMaxLifetime {
		return false
	}
	if p.cfg.ConnMaxIdleTime > 0 && now.Sub(cn.UsedAt()) >= p.cfg.ConnMaxIdleTime {
		return false
	}

	if connCheck(cn.netConn) != nil {
		return false
	}

	cn.SetUsedAt(now)
	return true
}

func (p *DynamicConnPool) isCtxExpired(ctx context.Context) bool {
	select {
	default:
		return false
	case <-ctx.Done():
		return true
	}
}

type connReq struct {
	conn *Conn
	err  error
}

type ctxConnChan struct {
	ctx      context.Context
	connChan chan connReq
}
