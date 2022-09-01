package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
)

const (
	// connReqsQueueSize: buffered chan size of connection opener, this value should be larger than the maximum typical
	// value used for poolSize, otherwise it might block ALL calls in Pool until pending connection request is satisfied
	connReqsQueueSize = 1000000

	minIdleCheckFrequency = time.Second
)

type Reaper interface {
	ReapStaleConns() (int, error)
}

type Setter interface {
	SetPoolFIFO(poolFIFO bool)
	SetPoolSize(poolSize int)
	SetMinIdleConns(minIdleConns int)
	SetMaxIdleConns(maxIdleConns int)
	SetMaxConnAge(maxConnAge time.Duration)
	SetPoolTimeout(poolTimeout time.Duration)
	SetIdleTimeout(idleTimeout time.Duration)
	SetIdleCheckFrequency(idleCheckFrequency time.Duration)
}

type DynamicPooler interface {
	Pooler
	Setter
	Reaper
}

type DynamicConnPool struct {
	opt *Options

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
		opt:      opt,
		closedCh: make(chan struct{}),
		connReqs: make(chan ctxConnChan, connReqsQueueSize),
	}

	p.connsMu.Lock()
	p.checkMinIdleConnsLocked()
	p.resetReaperLocked()
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
	// no more Get operation after pool closed, but ongoing request will continue to run
	if p.closed() {
		return nil, ErrClosed
	}

	if p.isCtxExpired(ctx) {
		return nil, ctx.Err()
	}

	cn, err := p.getNonStaleConnOrNewConn(ctx, true)
	// either conn is getting from idle list, created or there is error during new connection creation
	if cn != nil || err != nil {
		return cn, err
	}

	// no non-stale conn or new conn available, create a connection request and wait for it
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	req := ctxConnChan{
		ctx:      childCtx,
		connChan: make(chan connReq, 1),
	}

	p.connReqs <- req

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)

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
	p.opt.PoolFIFO = poolFIFO
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
	p.opt.PoolSize = poolSize
	removeCount := p.poolSize - p.opt.PoolSize
	p.removeIdleConns(removeCount)
}

// SetMinIdleConns sets the minimum number of connections in the idle connection pool.
func (p *DynamicConnPool) SetMinIdleConns(minIdleConns int) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.opt.MinIdleConns = minIdleConns
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
	p.opt.MaxIdleConns = maxIdleConns
	removeCount := len(p.idleConns) - maxIdleConns
	p.removeIdleConns(removeCount)
}

// SetMaxConnAge sets the connection age at which a connection should be treated as stale.
func (p *DynamicConnPool) SetMaxConnAge(maxConnAge time.Duration) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.opt.MaxConnAge = maxConnAge
	p.resetReaperLocked() // check if reaper job need to be turned on/off after changes
}

// SetPoolTimeout sets the PoolTimeout config, no action required, next waiting request will use the updated config.
func (p *DynamicConnPool) SetPoolTimeout(poolTimeout time.Duration) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.opt.PoolTimeout = poolTimeout
}

// SetIdleTimeout sets amount of idle time after which a connection should be treated as stale.
func (p *DynamicConnPool) SetIdleTimeout(idleTimeout time.Duration) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	p.opt.IdleTimeout = idleTimeout
	p.resetReaperLocked() // check if reaper job need to be turned on/off after changes
}

func (p *DynamicConnPool) SetIdleCheckFrequency(idleCheckFrequency time.Duration) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	if idleCheckFrequency > 0 && idleCheckFrequency < minIdleCheckFrequency {
		idleCheckFrequency = minIdleCheckFrequency
	}

	p.opt.IdleCheckFrequency = idleCheckFrequency
	p.resetReaperLocked() // check if reaper job need to be turned on/off after changes
}

func (p *DynamicConnPool) handleConnReqs() {
	for req := range p.connReqs {

		// keep retry until either ctx cancel, a non-stale idle conn is found or a new conn creation is possible
		for {
			if p.isCtxExpired(req.ctx) {
				break
			}

			cn, err := p.getNonStaleConnOrNewConn(req.ctx, false)
			// either conn is getting from idle list, created or there is error during new connection creation
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

// getNonStaleConnOrNewConn: attempt to get a connection to satisfy request
// If there is non-stale connection in the idle list, pop the first one to return while close the stale connections.
// If there is still vacancy to create new connection, try to create a new connection and return the response.
// Otherwise, return nil, nil to indicate the request still need to wait for connection release.
func (p *DynamicConnPool) getNonStaleConnOrNewConn(ctx context.Context, isDirectGet bool) (*Conn, error) {
	p.connsMu.Lock()
	// first try to get from idle conn list
	if cn := p.getNonStaleConnLocked(); cn != nil {
		if isDirectGet {
			atomic.AddUint32(&p.stats.Hits, 1)
		}
		p.connsMu.Unlock()
		return cn, nil
	}

	if isDirectGet {
		atomic.AddUint32(&p.stats.Misses, 1)
	}

	// no more vacancy to create new connection, return nil, nil
	if p.poolSize >= p.opt.PoolSize {
		p.connsMu.Unlock()
		return nil, nil
	}

	// still has vacancy to create new connection
	p.poolSize++
	p.connsMu.Unlock()

	cn, err := p.dialConn(ctx)
	if err != nil {
		p.connsMu.Lock()
		p.poolSize--
		p.connsMu.Unlock()
	}
	return cn, err
}

func (p *DynamicConnPool) getNonStaleConnLocked() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	defer p.checkMinIdleConnsLocked()

	// try get a non-stale idle conn
	for {
		cn := p.popIdleLocked()
		if cn == nil {
			return nil
		}

		if p.shouldCheckStaleConn() && p.isStaleConn(cn) {
			p.poolSize--
			_ = p.closeConn(cn)
			atomic.AddUint32(&p.stats.StaleConns, uint32(1))
			continue
		}

		return cn
	}
}

func (p *DynamicConnPool) popIdleLocked() *Conn {
	n := len(p.idleConns)
	if n == 0 {
		return nil
	}

	var cn *Conn
	if p.opt.PoolFIFO {
		cn = p.idleConns[0]
		p.idleConns = p.idleConns[1:]
	} else {
		cn = p.idleConns[n-1]
		p.idleConns = p.idleConns[:n-1]
	}
	p.idleConnsLen--
	return cn
}

func (p *DynamicConnPool) checkMinIdleConnsLocked() {
	if p.opt.MinIdleConns == 0 {
		return
	}
	for p.poolSize < p.opt.PoolSize && p.idleConnsLen < p.opt.MinIdleConns && p.idleConnsLen < p.opt.MaxIdleConns {
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
	if p.closed() || p.poolSize > p.opt.PoolSize || p.idleConnsLen > p.opt.MaxIdleConns {
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
	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	netConn, err := p.opt.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
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

		conn, err := p.opt.Dialer(context.Background())
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
	if p.closed() || p.poolSize > p.opt.PoolSize || len(p.idleConns) >= p.opt.MaxIdleConns {
		p.poolSize--
		_ = p.closeConn(cn)
		return
	}

	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
}

func (p *DynamicConnPool) closeConn(cn *Conn) error {
	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(cn)
	}
	return cn.Close()
}

func (p *DynamicConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

// resetReaperLocked update the reaper after related configuration changed
func (p *DynamicConnPool) resetReaperLocked() {
	if p.opt.IdleCheckFrequency > 0 && p.shouldCheckStaleConn() { // should enable reaper
		if p.reaperCh == nil { // no reaper job run yet, start new reaper job
			p.reaperCh = make(chan struct{}, 1)
			go p.reaper()
		} else { // trigger reaper reset to apply changes immediately
			p.reaperCh <- struct{}{}
		}
	} else { // should disable reaper
		if p.reaperCh != nil {
			close(p.reaperCh)
			p.reaperCh = nil
		}
	}
}

func (p *DynamicConnPool) reaper() {
	ticker := time.NewTicker(p.opt.IdleCheckFrequency)
	defer ticker.Stop()

	for {
		select {
		case _, ok := <-p.reaperCh:
			if !ok {
				return
			}
			ticker.Reset(p.opt.IdleCheckFrequency)
		case <-ticker.C:
			p.ReapStaleConns()
		case <-p.closedCh:
			p.reaperCh = nil
			return
		}
	}
}

func (p *DynamicConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()

		if cn == nil {
			break
		}
		n++
	}
	atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	return n, nil
}

func (p *DynamicConnPool) reapStaleConn() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	cn := p.idleConns[0]
	if !p.isStaleConn(cn) {
		return nil
	}

	p.idleConns = p.idleConns[1:]
	p.idleConnsLen--
	p.poolSize--
	_ = p.closeConn(cn)

	return cn
}

func (p *DynamicConnPool) shouldCheckStaleConn() bool {
	return p.opt.IdleTimeout > 0 || p.opt.MaxConnAge > 0
}

func (p *DynamicConnPool) isStaleConn(cn *Conn) bool {
	now := time.Now()
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		return true
	}
	if p.opt.MaxConnAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxConnAge {
		return true
	}

	return false
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
