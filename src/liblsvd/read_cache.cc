/*
 * file:        read_cache.cc
 * description: implementation of read cache
 *              the read cache is:
 *                       * 1. indexed by obj/offset[*], not LBA
 *                       * 2. stores aligned 64KB blocks
 *                       * [*] offset is in units of 64KB blocks
 * author:      Peter Desnoyers, Northeastern University
 *              Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#include <unistd.h>
#include <stdint.h>
#include <string.h>

#include <shared_mutex>
#include <mutex>
#include <thread>
#include <atomic>
#include <cassert>


#include <queue>
#include <map>
#include <stack>
#include <vector>

#include <random>
#include <algorithm>		// std::min

#include "lsvd_types.h"
#include "backend.h"

#include "smartiov.h"
#include "extent.h"
#include "misc_cache.h"

#include "request.h"
#include "journal.h"
#include "config.h"
#include "translate.h"
#include "nvme.h"

#include "read_cache.h"
#include "objname.h"

extern void do_log(const char *, ...);

class read_cache_impl : public read_cache {
    
    std::mutex m;
    std::map<extmap::obj_offset,int> map;

    j_read_super       *super;
    extmap::obj_offset *flat_map;

    extmap::objmap     *obj_map;
    std::shared_mutex  *obj_lock;
    
    translate          *be;
    backend            *io;
    nvme               *ssd;
    
    friend class rcache_req;
    
    int               unit_sectors;
    std::vector<int>  free_blks;
    bool              map_dirty = false;


    // new idea for hit rate - require that sum(backend reads) is no
    // more than 2 * sum(read sectors) (or 3x?), using 64bit counters 
    //
    struct {
	int64_t       user = 1000; // hack to get test4/test_2_fakemap to work
	int64_t       backend = 0;
    } hit_stats;
    int               outstanding_writes = 0; // NVMe writes to cache
    int               maxbufs = 100;
    
    thread_pool<int> misc_threads; // eviction thread, for now
    bool             nothreads = false;	// for debug

    /* if map[obj,offset] = n:
     *   in_use[n] - not eligible for eviction
     *   written[n] - safe to read from cache
     *   buffer[n] - in-memory data for block n
     *   pending[n] - continuations to invoke when buffer[n] becomes valid
     * buf_loc - FIFO queue of {n | buffer[n] != NULL}
     */
    sized_vector<std::atomic<int>>   in_use;
    sized_vector<char>               written; // can't use vector<bool> here
    sized_vector<char*>              buffer;
    sized_vector<std::vector<request*>> pending;
    std::queue<int>    buf_loc;

    /* possible CLOCK implementation - queue holds <block,ojb/offset> 
     * pairs so that we can evict blocks without having to remove them 
     * from the CLOCK queue
     */
    sized_vector<char> a_bit;
#if 0
    sized_vector<int>  block_version;
    std::queue<std::pair<int,extmap::obj_offset>> clock_queue;
#endif
    
    /* evict 'n' blocks - random replacement
     */
    void evict(int n);
    void evict_thread(thread_pool<int> *p);
    char *get_cacheline_buf(int n);

public:
    read_cache_impl(uint32_t blkno, int _fd, bool nt,
		    translate *_be, extmap::objmap *map,
		    std::shared_mutex *m, backend *_io);
    ~read_cache_impl();
    
    std::tuple<size_t,size_t,request*> async_readv(size_t offset,
						   smartiov *iov);

    void write_map(void);
};

/* factory function so we can hide implementation
 */
read_cache *make_read_cache(uint32_t blkno, int _fd, bool nt, translate *_be,
			    extmap::objmap *map, std::shared_mutex *m,
			    backend *_io) {
    return new read_cache_impl(blkno, _fd, nt, _be, map, m, _io);
}

/* constructor - allocate, read the superblock and map, start threads
 */
read_cache_impl::read_cache_impl(uint32_t blkno, int fd_, bool nt,
				 translate *be_, extmap::objmap *omap,
				 std::shared_mutex *maplock,
				 backend *io_) : misc_threads(&m) {
    obj_map = omap;
    obj_lock = maplock;
    be = be_;
    io = io_;
    nothreads = nt;

    const char *name = "read_cache_cb";
    ssd = make_nvme(fd_, name);
    
    char *buf = (char*)aligned_alloc(512, 4096);
    if (ssd->read(buf, 4096, 4096L*blkno) < 0)
	throw("read cache superblock");
    super = (j_read_super*)buf;

    unit_sectors = super->unit_size;

    in_use.init(super->units);
    written.init(super->units);
    buffer.init(super->units);
    pending.init(super->units);
    a_bit.init(super->units);

    flat_map = (extmap::obj_offset*)aligned_alloc(512, super->map_blocks*4096);
    if (ssd->read((char*)flat_map, super->map_blocks*4096,
		  super->map_start*4096L) < 0)
	throw("read flatmap");

    for (int i = 0; i < super->units; i++) {
	if (flat_map[i].obj != 0) {
	    map[flat_map[i]] = i;
	    written[i] = true;
	}
	else 
	    free_blks.push_back(i);
    }

    map_dirty = false;

    misc_threads.pool.push(std::thread(&read_cache_impl::evict_thread,
				       this, &misc_threads));
}

read_cache_impl::~read_cache_impl() {
    misc_threads.stop();	// before we free anything threads might touch
    delete ssd;
	
    free((void*)flat_map);
    for (auto i = 0; i < super->units; i++)
	if (buffer[i] != NULL)
	    free(buffer[i]);
    free((void*)super);
}

#if 0
static std::random_device rd; // to initialize RNG
static std::mt19937 rng(rd());
#else
static std::mt19937 rng(17);      // for deterministic testing
#endif

/* evict 'n' blocks from cache, using random eviction
 */
void read_cache_impl::evict(int n) {
    // assert(!m.try_lock());       // m must be locked
    std::uniform_int_distribution<int> uni(0,super->units - 1);
    for (int i = 0; i < n; i++) {
	int j = uni(rng);
	while (flat_map[j].obj == 0 || in_use[j] > 0)
	    j = uni(rng);
	auto oo = flat_map[j];
	flat_map[j] = (extmap::obj_offset){0, 0};
	map.erase(oo);
	free_blks.push_back(j);
    }
}

void read_cache_impl::evict_thread(thread_pool<int> *p) {
    auto wait_time = std::chrono::milliseconds(500);
    auto t0 = std::chrono::system_clock::now();
    auto timeout = std::chrono::seconds(2);

    std::unique_lock<std::mutex> lk(m);

    while (p->running) {
	p->cv.wait_for(lk, wait_time);
	if (!p->running)
	    return;

	int n = 0;
	if ((int)free_blks.size() < super->units / 16)
	    n = super->units / 4 - free_blks.size();
	if (n)
	    evict(n);

	if (!map_dirty)     // free list didn't change
	    continue;

	/* write the map (a) immediately if we evict something, or 
	 * (b) occasionally if the map is dirty
	 */
	auto t = std::chrono::system_clock::now();
	if (n > 0 || (t - t0) > timeout) {
	    size_t bytes = 4096 * super->map_blocks;
	    auto buf = (char*)aligned_alloc(512, bytes);
	    memcpy(buf, flat_map, bytes);

	    if (ssd->write(buf, bytes, 4096L * super->map_start) < 0)
		throw("write flatmap");

	    free(buf);
	    t0 = t;
	    map_dirty = false;
	}
    }
}

/* state machine for block obj,offset can be represented by the tuple:
 *  map=n - i.e. exists(n) | map[obj,offset] = n
 *  in_use[n] - 0 / >0
 *  written[n] - n/a, F, T
 *  buffer[n] - n/a, NULL, <p>
 *  pending[n] - n/a, [], [...]
 *
 * if not cached                          -> {!map, n/a}
 * first read will:
 *   - add to map
 *   - increment in_use
 *   - launch read                        -> {map=n, >0, F, NULL, []}
 * following reads will 
 *   queue request to copy from buffer[n] -> {map=n, >0, F, NULL, [..]}
 * read complete will:
 *   - set buffer[n]
 *   - notify requests from pending[n]
 *   - launch write                       -> {map=n, >0, F, <p>, []}
 * write complete will:
 *   - set written[n] to true             -> {map=n, >0, T, <p>, []}
 * eviction of buffer will:
 *   - decr in_use
 *   - remove buffer                      -> {map=n, 0, T, NULL, []}
 * further reads will temporarily increment in_use
 * eviction will remove from map:         -> {!map, n/a}
 */


/* limit the number of block buffers, using FIFO replacement via
 * the @buf_loc queue. Note that this is only called from async_readv
 * with the read cache mutex locked
 *
 * TODO: double-check written[n] and recycle if it isn't?
 */
char *read_cache_impl::get_cacheline_buf(int n) {
    char *buf = NULL;
    int len = super->unit_size * 512;
    if (buf_loc.size() < (size_t)maxbufs) 
	buf = (char*)aligned_alloc(512, len);
    else {
	for (int i = 0; i < 20; i++) {
	    int j = buf_loc.front();
	    buf_loc.pop();
	    if (buffer[j] != NULL && written[j]) {
		buf = buffer[j];
		buffer[j] = NULL;
		in_use[j]--;
		break;
	    }
	    buf_loc.push(j);
	}
	if (buf == NULL) {
	    do_log("rcache bufs: %d\n", (int)buf_loc.size()+1);
	    buf = (char*)aligned_alloc(512, len);
	}
    }
    buf_loc.push(n);
    memset(buf, 0, len);
    return buf;
}

enum req_type {
    RCACHE_NONE = 0,

    // we found an in-memory copy of the cache block,
    // complete immediately
    RCACHE_LOCAL_BUFFER = 1,

    // waiting for read from block already in NVME cache
    RCACHE_SSD_READ = 2,

    // to be queued, waiting for run()
    RCACHE_PENDING_QUEUE = 3,

    // waiting for someone else to read my cache block:
    RCACHE_QUEUED = 4,

    // waiting for backend read of full cache block
    RCACHE_BACKEND_WAIT = 5,

    // waiting for block write to cache (async)
    RCACHE_BLOCK_WRITE = 6,

    // cache readaround - waiting for backend
    RCACHE_DIRECT_READ = 7,

    RCACHE_DONE = 8
};

class rcache_req : public request {
    request *parent = NULL;
    read_cache_impl *rci;

    friend class read_cache_impl;

    enum req_type state = RCACHE_NONE;
    bool   released = false;
    request *sub_req = NULL;	// cache or backend request

    int      n = -1;		// cache block number
    extmap::obj_offset unit;	// map key
    
    sector_t blk_offset = -1;	// see async_readv comments

    smartiov iovs;
    
    off_t nvme_offset;
    off_t buf_offset;
    char *_buf = NULL;
    char objname[128];

    std::mutex m;
    
public:
    rcache_req(read_cache_impl *rcache_) : rci(rcache_) {}
    ~rcache_req() {}

    void run(request *parent);
    void notify(request *child);
    void release();

    void wait() {}
};    

void rcache_req::release() {
    released = true;
    if (state == RCACHE_DONE)
	delete this;
}

void rcache_req::notify(request *child) {
    std::unique_lock lk(m);
    bool notify_parent = false;
    enum req_type next_state = state;

    if (child != NULL)
	child->release();

    /* direct read from nvme, line 375
     */
    if (state == RCACHE_SSD_READ) {
	rci->in_use[n]--;
	next_state = RCACHE_DONE;
	notify_parent = true;
    }
    /* completion of a pending prior read
     */
    else if (state == RCACHE_QUEUED) {
	iovs.copy_in(rci->buffer[n] + blk_offset*512);
	notify_parent = true;
	next_state = RCACHE_DONE;
    }
    /* cache block read completion
     */
    else if (state == RCACHE_BACKEND_WAIT) {
	iovs.copy_in(_buf + buf_offset);

	std::unique_lock lk(rci->m);
	rci->buffer[n] = _buf;
	lk.unlock();

	/* once buffer[n] is set, pending[n] will not be modified
	 * by any other requests and we can safely access and clear it
	 */
	notify_parent = true;
	for (auto const & p : rci->pending[n]) 
	    p->notify(NULL);	// other requests waiting on read
	rci->pending[n].clear();

	sub_req = rci->ssd->make_write_request(_buf, rci->unit_sectors*512L,
					       nvme_offset);
	next_state = RCACHE_BLOCK_WRITE;
	sub_req->run(this);
    }
    else if (state == RCACHE_BLOCK_WRITE) {
	std::unique_lock lk(rci->m);
	rci->written[n] = true;
	rci->flat_map[n] = unit;
	rci->map_dirty = true;
	rci->outstanding_writes--;
	assert(rci->outstanding_writes >= 0);
	next_state = RCACHE_DONE;
    }
    else if (state == RCACHE_DIRECT_READ) {
	notify_parent = true;
	next_state = RCACHE_DONE;
    }
    else
	assert(false);
    
    if (notify_parent && parent != NULL) 
	parent->notify(this);

    if (next_state == RCACHE_DONE && released) {
	lk.unlock();
	delete this;
	return;
    }
    else
	state = next_state;
}

void rcache_req::run(request *parent_) {
    parent = parent_;
    if (state == RCACHE_QUEUED) {
	/* nothing */
    }
    else if (state == RCACHE_PENDING_QUEUE) {
	std::unique_lock lk(rci->m);
	if (rci->buffer[n] != NULL) {
	    iovs.copy_in(rci->buffer[n] + blk_offset*512);
	    parent->notify(this);
	    state = RCACHE_DONE;
	    if (released)
		delete this;
	}
	else {
	    rci->pending[n].push_back(this);
	    state = RCACHE_QUEUED;
	}
    }
    else if (state == RCACHE_LOCAL_BUFFER) {
	parent->notify(this);
	state = RCACHE_DONE;
	if (released)
	    delete this;
    }
    else if (state == RCACHE_SSD_READ ||
	     state == RCACHE_BACKEND_WAIT ||
	     state == RCACHE_DIRECT_READ) {
	sub_req->run(this);
    }
    else
	assert(false);
}

std::tuple<size_t,size_t,request*>
read_cache_impl::async_readv(size_t offset, smartiov *iov) {
    size_t len = iov->bytes();
    sector_t base = offset/512, sectors = len/512, limit = base+sectors;
    sector_t read_sectors = -1, skip_sectors = -1;
    extmap::obj_offset oo = {0, 0};
    
    std::shared_lock lk(*obj_lock);
    auto it = obj_map->lookup(base);
    if (it == obj_map->end() || it->base() >= limit) {
	skip_sectors = sectors;
	read_sectors = 0;
    }
    else {
	auto [_base, _limit, _ptr] = it->vals(base, limit);
	skip_sectors = (_base - base);
	read_sectors = (_limit - _base);
	oo = _ptr;
    }
    lk.unlock();

    if (read_sectors == 0)
	return std::make_tuple(skip_sectors*512L, 0, (request*)NULL);
    
    /* handle the small probability that it's for an object
     * currently being GC'ed
     */
    if (!be->check_object_ready(oo.obj))
	be->wait_object_ready(oo.obj);

    auto r = new rcache_req(this);
    
    /*
     * diagram to help explain stuff:
     *
     *   base                                     limit
     *    [------------ sectors --------------------]
     *                 [----- map entry -----]
     *
     *    |----------->| skip_sectors
     *                 |-------------------->| read_sectors
     *
     * (a) if the unit ends before the map entry:
     * [------- unit --------------------]
     *  -------------->| blk_offset
     *  -------------------------------->| blk_top_offset (update read_sectors)
     *
     * (b) if it ends after the map entry:
     *          [------- unit --------------------]
     *           ----->| blk_offset
     *           --------------------------->| blk_top_offset
     */

    extmap::obj_offset unit = r->unit = {oo.obj, oo.offset / unit_sectors};
    sector_t blk_base = r->unit.offset * unit_sectors;
    sector_t blk_offset = oo.offset % unit_sectors;
    sector_t blk_top_offset = std::min((int)(blk_offset+read_sectors),
				       round_up(blk_offset+1,unit_sectors));
    int n = -1;			// cache block number

    /* trim read_sectors - (a) above
     */
    read_sectors = (blk_top_offset - blk_offset);

    size_t skip_len = 512L * skip_sectors;
    size_t read_len = 512L * read_sectors;

    /* Hold the read cache instance lock through the rest of the
     * method. The code is complicated, but doesn't do any I/O. 
     * TODO: see about dropping the lock for buffer copy???
     */
    std::unique_lock lk2(m);
    bool in_cache = false;
    auto it2 = map.find(unit);
    if (it2 != map.end()) {
	r->n = n = it2->second;
	in_cache = true;
    }

    /* protection against random reads - read-around when hit rate is too low
     */
    bool use_cache = free_blks.size() > 0 &&
	hit_stats.user * 3 > hit_stats.backend * 2;
    if (outstanding_writes > maxbufs - 10)
	use_cache = false;
    if (free_blks.size() < 5)
	misc_threads.cv.notify_one();
    
    if (in_cache) {
	sector_t blk_in_ssd = super->base*8 + n*unit_sectors,
	    start = blk_in_ssd + blk_offset;

	a_bit[n] = true;
	hit_stats.user += read_sectors;

	if (buffer[n] != NULL) {
	    smartiov _iov = iov->slice(skip_len, skip_len + read_len);
	    _iov.copy_in(buffer[n] + blk_offset*512L);
	    r->state = RCACHE_LOCAL_BUFFER;
	}
	else if (written[n]) {
	    in_use[n]++;
	    smartiov _iov = iov->slice(skip_len, skip_len + read_len);
	    r->sub_req = ssd->make_read_request(&_iov, 512L*start);
	    r->state = RCACHE_SSD_READ;
	}
	else {              // prior read is pending
	    r->state = RCACHE_PENDING_QUEUE;
	    r->iovs = iov->slice(skip_len, skip_len + read_len);
	    r->blk_offset = blk_offset;
	}
    }
    else if (use_cache) {
	/* assign a location in cache before we start reading (and while we're
	 * still holding the lock)
	 */
	r->n = n = free_blks.back();
	free_blks.pop_back();
	written[n] = false;
	assert(buffer[n] == NULL);
	in_use[n]++;
	auto _buf = get_cacheline_buf(n);
	memset(_buf, 0, 64*1024);

	/* point the map to that location, so that further requests to the 
	 * same cache block will get queued. 
	 * (since buffer[n] == NULL and !written[n])
	 */
	map[unit] = n;

	hit_stats.backend += unit_sectors;
	//sector_t sectors = std::min((sector_t)(read_len/512),
	//			    blk_top_offset - blk_offset);
	hit_stats.user += read_sectors;

	r->nvme_offset = (super->base*8 + n*unit_sectors) * 512L;
	r->buf_offset = blk_offset * 512L;

	r->_buf = _buf;
	r->state = RCACHE_BACKEND_WAIT;
	r->iovs = iov->slice(skip_len, skip_len+read_len);
	
	objname name(be->prefix(unit.obj), unit.obj);
	strcpy(r->objname, name.c_str());
	r->sub_req = io->make_read_req(r->objname, 512L*blk_base,
				       _buf, 512L*unit_sectors);
	outstanding_writes++;	// bound # of write bufs
    }
    else {
	hit_stats.user += read_sectors;
	hit_stats.backend += read_sectors;

	objname name(be->prefix(oo.obj), oo.obj);
	auto tmp = iov->slice(skip_len, skip_len + read_len);
	auto [_iov, _niov] = tmp.c_iov();
	r->sub_req = io->make_read_req(name.c_str(), 512L*oo.offset,
				       _iov, _niov);
	r->state = RCACHE_DIRECT_READ;
    }
    return std::make_tuple(skip_len, read_len, r);
}

void read_cache_impl::write_map(void) {
    if (ssd->write(flat_map, 4096 * super->map_blocks,
                   4096L * super->map_start) < 0)
        throw("write flatmap");
}
