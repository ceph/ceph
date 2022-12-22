/*
 * file:        translate.cc
 * description: core translation layer - implementation
 * 
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#include <unistd.h>
#include <sys/uio.h>
#include <string.h>

#include <uuid/uuid.h>
#include <zlib.h>

#include <vector>
#include <mutex>
#include <condition_variable>
#include <shared_mutex>
#include <atomic>

#include <stack>
#include <map>

#include <algorithm>

#include <thread>

#include "extent.h"
#include "lsvd_types.h"
#include "request.h"
#include "objects.h"
#include "objname.h"
#include "config.h"
#include "translate.h"

#include "backend.h"
#include "smartiov.h"
#include "misc_cache.h"


void do_log(const char*, ...);
void fp_log(const char*, ...);
extern void log_time(uint64_t loc, uint64_t val); // debug

/* ----------- Object translation layer -------------- */

class batch {
public:
    std::vector<data_map> entries;
    char  *_buf = NULL;		// allocation start
    char  *buf = NULL;		// data goes here
    size_t len = 0;		// current data size
    size_t max;			// done when len hits here
    int    seq = 0;		// sequence number for backend
    uint64_t cache_seq = 0;

    batch(size_t bytes){
	int room = 16*1024*sizeof(data_map) + 512;
	_buf = (char*)malloc(bytes + room);
	buf = _buf + room;
	max = bytes;
    }
    ~batch(){
	free(_buf);
    }
    void append(uint64_t lba, smartiov *iov) {
	auto bytes = iov->bytes();
	entries.push_back((data_map){lba, bytes/512});
	char *ptr = buf + len;
	iov->copy_out(ptr);
	len += bytes;
    }
};

class translate_impl : public translate {
    /* lock ordering: lock m before *map_lock
     */
    std::mutex         m;	// for things in this instance
    extmap::objmap    *map;	// shared object map
    std::shared_mutex *map_lock; // locks the object map
    lsvd_config       *cfg;

    std::atomic<int>   seq;
    uint64_t           ckpt_cache_seq = 0; // from last data object
    
    friend class translate_req;
    batch *b = NULL;
    
    /* info on live data objects - all sizes in sectors 
     * checkpoints are tracked in @checkpoints, and in the superblock
     */
    struct obj_info {
	int hdr;		// sectors
	int data;		// sectors
	int live;		// sectors
    };
    std::map<int,obj_info> object_info;

    std::vector<uint32_t> checkpoints;
    
    /* tracking completions for flush()
     */
    int       next_compln = 1;
    int       last_sent = 0;	// most recent data object
    std::atomic<int> outstanding_writes = 0; // wait_for_room
    
    std::set<int> pending_writes;
    std::condition_variable cv;
    bool stopped = false;	// stop GC from writing

    /* various constant state
     */
    struct clone {
	char prefix[128];
	int  last_seq;
	int  first_seq = 0;
    };
    std::vector<clone> clone_list;
    char               super_name[128];

    /* superblock has two sections: [obj_hdr] [super_hdr]
     */
    char      *super_buf = NULL;
    obj_hdr   *super_h = NULL;
    super_hdr *super_sh = NULL;
    size_t     super_len;


    thread_pool<batch*> *workers;
    thread_pool<int>    *misc_threads; // so we can stop ckpt, gc first

    /* for triggering GC
     */
    sector_t total_sectors = 0;
    sector_t total_live_sectors = 0;
    int gc_cycles = 0;
    int gc_sectors_read = 0;
    int gc_sectors_written = 0;
    int gc_deleted = 0;

    /* for shutdown
     */
    bool gc_running = false;
    std::condition_variable gc_cv;
    void stop_gc(void);
    
    object_reader *parser;
    
    /* maybe use insertion sorted vector?
     *  https://stackoverflow.com/questions/15843525/how-do-you-insert-the-value-in-a-sorted-vector
     */

    void write_checkpoint(int seq);
    batch *check_batch_room(size_t len);
    void process_batch(batch *b);
    void worker_thread(thread_pool<batch*> *p);
    
    sector_t make_gc_hdr(char *buf, uint32_t seq, sector_t sectors,
			 data_map *extents, int n_extents);

    void do_gc(bool *running);
    void gc_thread(thread_pool<int> *p);
    void flush_thread(thread_pool<int> *p);

    backend *objstore;

    /* all objects seq<next_compln have been completed
     */
    void seq_record(int _seq) {
	std::unique_lock lk(m);
	if (_seq > last_sent) {
	    last_sent = _seq;
	    do_log("last_sent2 : %d\n", last_sent);
	}
	pending_writes.insert(_seq);
    }

    void seq_complete(int _seq) {
	std::unique_lock lk(m);
	pending_writes.erase(_seq);
	auto it = pending_writes.begin();
	if (it == pending_writes.end())
	    next_compln = last_sent+1;
	else
	    next_compln = *it;
	do_log("next_compln = %d\n", next_compln);
	cv.notify_all();
    }

public:
    translate_impl(backend *_io, lsvd_config *cfg_,
		   extmap::objmap *map, std::shared_mutex *m);
    ~translate_impl();

    ssize_t init(const char *name, bool timedflush);
    void shutdown(void);

    int flush(void);            /* write out current batch */
    int checkpoint(void);       /* flush, then write checkpoint */

    ssize_t writev(uint64_t cache_seq, size_t offset, iovec *iov, int iovcnt);
    void wait_for_room(void);
    ssize_t readv(size_t offset, iovec *iov, int iovcnt);
    bool check_object_ready(int obj);
    void wait_object_ready(int obj);
    void start_gc(void);
    
    const char *prefix(int seq);
    
};

const char *translate_impl::prefix(int seq) {
    if (clone_list.size() == 0 || seq > clone_list.front().last_seq)
	return super_name;
    for (auto const & c : clone_list)
	if (seq >= c.first_seq)
	    return c.prefix;
    assert(false);
}

translate_impl::translate_impl(backend *_io, lsvd_config *cfg_,
			       extmap::objmap *map_, std::shared_mutex *m_) {
    misc_threads = new thread_pool<int>(&m);
    workers = new thread_pool<batch*>(&m);
    objstore = _io;
    parser = new object_reader(objstore);
    map = map_;
    map_lock = m_;
    cfg = cfg_;
}

translate *make_translate(backend *_io, lsvd_config *cfg,
			  extmap::objmap *map, std::shared_mutex *m) {
    return (translate*) new translate_impl(_io, cfg, map, m);
}

translate_impl::~translate_impl() {
    stopped = true;
    cv.notify_all();
    if (b) 
	delete b;
    delete parser;
    if (super_buf)
	free(super_buf);
}

ssize_t translate_impl::init(const char *prefix_, bool timedflush) {
    std::vector<uint32_t>    ckpts;
    std::vector<clone_info*> clones;
    std::vector<snap_info*>  snaps;

    /* note prefix = superblock name
     */
    strcpy(super_name, prefix_);

    auto [_buf, bytes] = parser->read_super(super_name, ckpts, clones,
					    snaps, uuid);
    if (bytes < 0)
	return bytes;
    
    int n_ckpts = ckpts.size();
    do_log("read super crc %0x8 ckpts %d\n", (uint32_t)crc32(0, (const unsigned char*)_buf, 4096));

    for (auto c : ckpts) 
	do_log("got ckpt from super (total %d): %d\n", n_ckpts, c);
    
    super_buf = _buf;
    super_h = (obj_hdr*)super_buf;
    super_len = super_h->hdr_sectors * 512;
    super_sh = (super_hdr*)(super_h+1);

    memcpy(&uuid, super_h->vol_uuid, sizeof(uuid));

    b = new batch(cfg->batch_size);
    seq = 1;			// empty volume case
    
    /* is this a clone?
     */
    if (super_sh->clones_len > 0) {
	char buf[4096];
	auto ci = (clone_info*)(_buf + super_sh->clones_offset);
	auto obj_name = (char*)(ci+1);
	while (true) {
	    if (objstore->read_object(obj_name, buf, sizeof(buf), 0) < 0)
		return -1;
	    auto _h = (obj_hdr*)buf;
	    auto _sh = (super_hdr*)(_h+1);
	    if (_h->magic != LSVD_MAGIC || _h->type != LSVD_SUPER) {
		printf("clone: bad magic\n");
		return -1;
	    }
	    if (memcmp(_h->vol_uuid, ci->vol_uuid, sizeof(uuid_t)) != 0) {
		printf("clone: bad UUID\n");
		return -1;
	    }
	    clone c;
	    strcpy(c.prefix, obj_name);
	    c.last_seq = ci->last_seq;
	    if (clone_list.size() > 0)
		clone_list.back().first_seq = ci->last_seq + 1;
	    clone_list.push_back(c);

	    if (_sh->clones_len == 0)
		break;
	    ci = (clone_info*)(buf + _sh->clones_offset);
	    obj_name = (char*)(ci + 1);
	}
    }
    
    /* read in the last checkpoint, then roll forward from there;
     */
    int last_ckpt = -1;
    if (ckpts.size() > 0) {
	std::vector<ckpt_obj> objects;
	std::vector<deferred_delete> deletes;
	std::vector<ckpt_mapentry> entries;

	/* hmm, we should never have checkpoints listed in the
	 * super that aren't persisted on the backend, should we?
	 */
	while (n_ckpts > 0) {
	    int c = ckpts[n_ckpts-1];
	    objname name(prefix(c), c);
	    do_log("reading ckpt %s\n", name.c_str());
	    if (parser->read_checkpoint(name.c_str(), max_cache_seq,
					ckpts, objects,
					deletes, entries) >= 0) {
		last_ckpt = c;
		break;
	    }
	    do_log("chkpt skip %d\n", c);
	    n_ckpts--;
	}
	if (last_ckpt == -1)
	    return -1;

	for (int i = 0; i < n_ckpts; i++) {
	    do_log("chkpt from super: %d\n", ckpts[i]);
	    checkpoints.push_back(ckpts[i]); // so we can delete them later
	}

	for (auto o : objects) {
	    object_info[o.seq] = (obj_info){.hdr = (int)o.hdr_sectors,
					    .data = (int)o.data_sectors,
					    .live = (int)o.live_sectors};
	    total_sectors += o.data_sectors;
	    total_live_sectors += o.live_sectors;
	}
	for (auto m : entries) {
	    map->update(m.lba, m.lba + m.len,
			    (extmap::obj_offset){.obj = m.obj,
				    .offset = m.offset});
	}
	seq = next_compln = last_ckpt + 1;
    }

    /* roll forward
     */
    for (; ; seq++) {
	std::vector<obj_cleaned> cleaned;
	std::vector<data_map>    entries;
	obj_hdr h; obj_data_hdr dh;

	objname name(prefix(seq), seq);
	if (parser->read_data_hdr(name.c_str(), h, dh, cleaned, entries) < 0)
	    break;
	if (h.type == LSVD_CKPT) {
	    do_log("ckpt from roll-forward: %d\n", seq.load());
	    checkpoints.push_back(seq);
	    continue;
	}

	do_log("roll %d\n", seq.load());
	assert(h.type == LSVD_DATA);
	object_info[seq] = (obj_info){.hdr = (int)h.hdr_sectors,
				      .data = (int)h.data_sectors,
				      .live = (int)h.data_sectors};
	total_sectors += h.data_sectors;
	total_live_sectors += h.data_sectors;
	if (dh.cache_seq)	// skip GC writes
	    max_cache_seq = dh.cache_seq;
	
	int offset = 0, hdr_len = h.hdr_sectors;
	std::vector<extmap::lba2obj> deleted;
	for (auto m : entries) {
	    extmap::obj_offset oo = {seq, offset + hdr_len};
	    map->update(m.lba, m.lba + m.len, oo, &deleted);
	    offset += m.len;
	}
	for (auto d : deleted) {
	    auto [base, limit, ptr] = d.vals();
	    object_info[ptr.obj].live -= (limit - base);
	    assert(object_info[ptr.obj].live >= 0);
	    total_live_sectors -= (limit - base);
	}
    }
    next_compln = seq;
    last_sent = next_compln - 1;
    do_log("set next_compln %d\n", next_compln);

    /* delete any potential "dangling" objects.
     */
    for (int i = 1; i < 32; i++) {
	objname name(prefix(i+seq), i + seq);
	objstore->delete_object(name.c_str());
    }

    workers->pool.push(std::thread(&translate_impl::worker_thread,
				   this, workers));
    if (timedflush)
	misc_threads->pool.push(std::thread(&translate_impl::flush_thread,
					    this, misc_threads));
    return bytes;
}

void translate_impl::start_gc(void) {
    misc_threads->pool.push(std::thread(&translate_impl::gc_thread,
				       this, misc_threads));
}
    
void translate_impl::shutdown(void) {
}

/* ----------- parsing and serializing various objects -------------*/

/* read object header
 *  fast: just read first 4KB
 *  !fast: read first 4KB, resize and read again if >4KB
 */


/* create header for a GC object
 */
sector_t translate_impl::make_gc_hdr(char *buf, uint32_t _seq, sector_t sectors,
				     data_map *extents, int n_extents) {
    auto h = (obj_hdr*)buf;
    auto dh = (obj_data_hdr*)(h+1);
    uint32_t o1 = sizeof(*h) + sizeof(*dh),
	l1 = n_extents * sizeof(data_map),
	hdr_bytes = o1 + l1;
    sector_t hdr_sectors = div_round_up(hdr_bytes, 512);

    *h = (obj_hdr){.magic = LSVD_MAGIC, .version = 1, .vol_uuid = {0},
		   .type = LSVD_DATA, .seq = _seq,
		   .hdr_sectors = (uint32_t)hdr_sectors,
		   .data_sectors = (uint32_t)sectors, .crc = 0};
    memcpy(h->vol_uuid, &uuid, sizeof(uuid_t));

    *dh = (obj_data_hdr){.cache_seq = 0,
			 .objs_cleaned_offset = 0, .objs_cleaned_len = 0,
			 .data_map_offset = o1, .data_map_len = l1};

    data_map *dm = (data_map*)(dh+1);
    for (int i = 0; i < n_extents; i++)
	*dm++ = extents[i];

    assert(hdr_bytes == ((char*)dm - buf));
    memset(buf + hdr_bytes, 0, 512*hdr_sectors - hdr_bytes); // valgrind
    h->crc = (uint32_t)crc32(0, (const unsigned char*)buf, 512*hdr_sectors);
    
    return hdr_sectors;
}

/* ----------- data transfer logic -------------*/

// call with len=0 to force a new batch
batch *translate_impl::check_batch_room(size_t len) {
    batch *full = NULL;
    if ((b->len + len > b->max) || (len == 0 && b->len > 0)) {
	b->seq = last_sent = seq++;
	do_log("last_sent = %d\n", last_sent);
	full = b;
	b = new batch(cfg->batch_size);
    }
    return full;
}

/* NOTE: offset is in bytes
 */
ssize_t translate_impl::writev(uint64_t cache_seq, size_t offset,
			       iovec *iov, int iovcnt) {
    smartiov siov(iov, iovcnt);
    size_t len = siov.bytes();

    std::unique_lock lk(m);
    auto old_batch = check_batch_room(len);
    if (b->cache_seq == 0) {	// lowest sequence number
	b->cache_seq = cache_seq;
	if (ckpt_cache_seq < cache_seq)
	    ckpt_cache_seq = cache_seq;
    }

    b->append(offset / 512, &siov);
    if (old_batch != NULL)
	workers->put_locked(old_batch);

    return len;
}

void translate_impl::wait_for_room(void) {
    std::unique_lock lk(m);
    while (outstanding_writes > cfg->xlate_window)
	cv.wait(lk);
}

/* GC (like normal write) updates the map before it writes an object,
 * but we can't assume the data is in cache, so we might get writes
 * for objects that haven't committed yet.
 * since this almost never blocks, do an unlocked check before the
 * locked one.
 */
bool translate_impl::check_object_ready(int obj) {
    return obj < (int)next_compln;
}
void translate_impl::wait_object_ready(int obj) {
    std::unique_lock lk(m);
    while (obj >= next_compln)
	cv.wait(lk);
}

class translate_req : public request {
    uint32_t seq;
    translate_impl *tx;
    std::mutex      m;
    bool            waiting;
    bool            done = false;
    std::condition_variable cv;
    
    friend class translate_impl;
    
    /* various things that we might need to free
     */
    std::vector<char*> to_free;
    batch *b = NULL;
    
public:
    translate_req(uint32_t seq_, translate_impl *tx_, bool waiting_) {
	seq = seq_;
	tx = tx_;
	waiting = waiting_;
    }
    ~translate_req(){}

    void wait(void) {
	std::unique_lock lk(m);
	while (!done)
	    cv.wait(lk);
	delete this;		// always call wait
    }

    void notify(request *child) {
	if (child)
	    child->release();
	tx->seq_complete(seq);
	for (auto ptr : to_free)
	    free(ptr);
	if (b) 
	    delete b;

	std::unique_lock lk(tx->m);
	if (--tx->outstanding_writes < tx->cfg->xlate_window)
	    tx->cv.notify_all();

	if (!waiting) 
	    delete this;
	else {
	    done = true;
	    cv.notify_one();
	}
    }

    void run(request *parent) {} // unused
    void release(void) {}	// unused
};

void translate_impl::worker_thread(thread_pool<batch*> *p) {
    pthread_setname_np(pthread_self(), "batch_writer");
    while (p->running) {
	std::unique_lock lk(m);
	batch *b;
	if (!p->get_locked(lk, b)) 
           return;

	int _seq = checkpoints.size() ? checkpoints.back() : 0;
	lk.unlock();

	process_batch(b);

	if (p->running && b->seq - _seq > cfg->ckpt_interval && !gc_running)
	    write_checkpoint(seq++);
    }
}

void translate_impl::process_batch(batch *b) {

    /* TODO: coalesce writes: 
     *   bufmap m
     *   for e in entries
     *      m.insert(lba, len, ptr)
     *   for e in m:
     *      (lba, iovec) -> output
     */

    /* make the following updates:
     * - object_info - hdrlen, total/live data sectors
     * - map - LBA to obj/offset map
     * - object_info, totals - adjust for new garbage
     */
    size_t hdr_bytes = obj_hdr_len(b->entries.size());
    int hdr_sectors = div_round_up(hdr_bytes, 512);
    char *hdr_ptr = b->buf - hdr_sectors*512;

    // deadlock risk - locks m, can't call with map_lock held
    seq_record(b->seq);

    std::unique_lock obj_w_lock(*map_lock);
    obj_info oi = {.hdr = hdr_sectors, .data = (int)b->len/512,
		   .live = (int)b->len/512};
    object_info[b->seq] = oi;

    /* note that we update the map before the object is written,
     * and count on the write cache preventing any reads until
     * it's persisted. TODO: verify this
     */
    sector_t sector_offset = hdr_sectors;
    std::vector<extmap::lba2obj> deleted;

    for (auto e : b->entries) {
	extmap::obj_offset oo = {b->seq, sector_offset};
	map->update(e.lba, e.lba+e.len, oo, &deleted);
	sector_offset += e.len;
    }

    for (auto d : deleted) {
	auto [base, limit, ptr] = d.vals();
	if (object_info.find(ptr.obj) == object_info.end())
	    continue;		// skip clone base
	object_info[ptr.obj].live -= (limit - base);
	assert(object_info[ptr.obj].live >= 0);
	total_live_sectors -= (limit - base);
    }

    /* live sectors, after subtracting dups */
    auto live = object_info[b->seq].live; 
    total_live_sectors += live;

    obj_w_lock.unlock();

    std::unique_lock lk(m);
    total_sectors += b->len / 512;
    if (next_compln == -1)
	next_compln = b->seq;
    lk.unlock();
    
    make_data_hdr(hdr_ptr, b->len, b->cache_seq, &b->entries, b->seq, &uuid);
    size_t total_len = hdr_sectors*512 + b->len;

    /* on completion, t_req calls seq_complete
     */
    auto t_req = new translate_req(b->seq, this, false);
    t_req->b = b;

    objname name(prefix(b->seq), b->seq);
    auto req = objstore->make_write_req(name.c_str(), hdr_ptr, total_len);
    outstanding_writes++;
    req->run(t_req);
}

/* flushes any data buffered in current batch, and blocks until all 
 * outstanding writes are complete.
 * returns sequence number of last written object.
 */
int translate_impl::flush() {
    auto old_batch = check_batch_room(0);
    if (old_batch == NULL)
	return last_sent;

    std::unique_lock lk(m);
    auto _seq = old_batch->seq;

    workers->put_locked(old_batch);

    while (next_compln <= _seq)
	cv.wait(lk);

    return _seq;
}

/* wake up every @wait_time, if data is pending in current batch
 * for @timeout then write it to backend
 */
void translate_impl::flush_thread(thread_pool<int> *p) {
    pthread_setname_np(pthread_self(), "flush_thread");
    auto wait_time = std::chrono::milliseconds(500);
    auto timeout = std::chrono::milliseconds(cfg->flush_msec);
    auto t0 = std::chrono::system_clock::now();
    auto seq0 = seq.load();

    while (p->running) {
	std::unique_lock lk(*p->m);
	if (p->cv.wait_for(lk, wait_time, [p] {return !p->running;}))
	    return;
	lk.unlock();

	if (p->running && seq0 == seq.load() && b->len > 0) {
	    if (std::chrono::system_clock::now() - t0 > timeout) 
		flush();
	}
	else {
	    seq0 = seq.load();
	    t0 = std::chrono::system_clock::now();
	}
    }
}

/* -------------- Checkpointing -------------- */

/* synchronously write a checkpoint
 */
void translate_impl::write_checkpoint(int ckpt_seq) {
    std::vector<ckpt_mapentry> entries;
    std::vector<ckpt_obj> objects;

    seq_record(ckpt_seq);

    /* - map lock protects both object_info and map
     * - damned if I know why I need lk(m) to keep test10 from failing,
     *   but I do.
     */
    std::unique_lock lk(m);
    std::unique_lock obj_w_lock(*map_lock);

    for (auto it = map->begin(); it != map->end(); it++) {
	auto [base, limit, ptr] = it->vals();
	entries.push_back((ckpt_mapentry){.lba = base,
		    .len = limit-base, .obj = (int32_t)ptr.obj,
		    .offset = (int32_t)ptr.offset});
    }

    size_t map_bytes = entries.size() * sizeof(ckpt_mapentry);

    for (auto it = object_info.begin(); it != object_info.end(); it++) {
	auto obj_num = it->first;
	auto [hdr, data, live] = it->second;
	objects.push_back((ckpt_obj){.seq = (uint32_t)obj_num,
		    .hdr_sectors = (uint32_t)hdr,
		    .data_sectors = (uint32_t)data,
		    .live_sectors = (uint32_t)live});
    }
    obj_w_lock.unlock();

    /* wait until all prior objects have been acked by backend, 
     */
    while (next_compln < ckpt_seq && !stopped)
	cv.wait(lk);
    if (stopped)
	return;
    lk.unlock();

    /* assemble the object
     */
    size_t objs_bytes = objects.size() * sizeof(ckpt_obj);
    size_t hdr_bytes = sizeof(obj_hdr) + sizeof(obj_ckpt_hdr);
    int sectors = div_round_up(hdr_bytes + sizeof(ckpt_seq) + map_bytes +
			       objs_bytes, 512);

    auto buf = (char*)calloc(sectors*512, 1);
    auto h = (obj_hdr*)buf;
    *h = (obj_hdr){.magic = LSVD_MAGIC, .version = 1, .vol_uuid = {0},
		   .type = LSVD_CKPT, .seq = (uint32_t)ckpt_seq,
		   .hdr_sectors = (uint32_t)sectors, .data_sectors = 0};
    memcpy(h->vol_uuid, uuid, sizeof(uuid_t));
    auto ch = (obj_ckpt_hdr*)(h+1);

    uint32_t o1 = sizeof(obj_hdr)+sizeof(obj_ckpt_hdr), o2 = o1 + objs_bytes;
    *ch = (obj_ckpt_hdr){.cache_seq = ckpt_cache_seq,
			 .ckpts_offset = 0, .ckpts_len = 0,
			 .objs_offset = o1, .objs_len = o2-o1,
			 .deletes_offset = 0, .deletes_len = 0,
			 .map_offset = o2, .map_len = (uint32_t)map_bytes};

    auto objs = (char*)(ch+1);
    memcpy(objs, (char*)objects.data(), objs_bytes);
    auto maps = objs + objs_bytes;
    memcpy(maps, (char*)entries.data(), map_bytes);
    
    /* and write it
     */
    objname name(prefix(ckpt_seq), ckpt_seq);
    objstore->write_object(name.c_str(), buf, sectors*512);
    seq_complete(ckpt_seq);	// no locks held
    free(buf);

    /* Now re-write the superblock with the new list of checkpoints
     */
    lk.lock();
    checkpoints.push_back(ckpt_seq);
    size_t offset = sizeof(*super_h) + sizeof(*super_sh);

    /* trim checkpoints. This function is the only place we modify
     * checkpoints[]
     */
    std::vector<int> ckpts_to_delete;
    while (checkpoints.size() > 3) {
	ckpts_to_delete.push_back(checkpoints.front());
	checkpoints.erase(checkpoints.begin());
    }
    lk.unlock();
    
    /* this is the only place we modify *super_sh
     */
    super_sh->ckpts_offset = offset;
    super_sh->ckpts_len = checkpoints.size() * sizeof(ckpt_seq);
    auto pc = (uint32_t*)(super_buf + offset);
    for (size_t i = 0; i < checkpoints.size(); i++)
	*pc++ = checkpoints[i];
	
    if (stopped)
	return;
    
    objstore->write_object(super_name, super_buf, 4096);

    for (auto c : ckpts_to_delete) {
	objname name(prefix(c), c);
	objstore->delete_object(name.c_str());
    }
}

/* called on shutdown
 */
int translate_impl::checkpoint(void) {
    auto old_batch = check_batch_room(0);
    if (old_batch != NULL) {
	std::unique_lock lk(m);
	auto _seq = old_batch->seq;
	workers->put_locked(old_batch);

	while (next_compln <= _seq)
	    cv.wait(lk);
    }

    int _seq = seq++;
    write_checkpoint(_seq);
    return _seq;
}


/* -------------- Garbage collection ---------------- */

struct _extent {
    int64_t base;
    int64_t limit;
    extmap::obj_offset ptr;
};

/* [describe GC algorithm here]
 */
void translate_impl::do_gc(bool *running) {
    gc_cycles++;
    int max_obj = seq.load();

    std::shared_lock obj_r_lock(*map_lock);
    std::vector<int> dead_objects;
    for (auto const &p : object_info)  {
	auto [hdrlen, datalen, live] = p.second;
	if (live == 0) {
	    total_sectors -= datalen;
	    dead_objects.push_back(p.first);
	}
    }
    obj_r_lock.unlock();

    std::queue<request*> deletes;
    for (auto const &o : dead_objects) {
	objname name(prefix(o), o);
	auto r = objstore->delete_object_req(name.c_str());
	r->run(NULL);
	deletes.push(r);
	while (deletes.size() > 8) {
	    deletes.front()->wait();
	    deletes.pop();
	}
    }
    while (deletes.size() > 0) {
	deletes.front()->wait();
	deletes.pop();
    }
    
    std::unique_lock obj_w_lock(*map_lock);
    for (auto const &o : dead_objects) 
	object_info.erase(o);
    obj_w_lock.unlock();
    
    /* create list of object info in increasing order of 
     * utilization, i.e. (live data) / (total size)
     */
    obj_r_lock.lock();
    int calculated_total = 0;
    std::set<std::tuple<double,int,int>> utilization;
    for (auto p : object_info)  {
	auto [hdrlen, datalen, live] = p.second;
	double rho = 1.0 * live / datalen;
	sector_t sectors = hdrlen + datalen;
	calculated_total += datalen;
	utilization.insert(std::make_tuple(rho, p.first, sectors));
	assert(sectors <= 20*1024*1024/512);
    }
    
    /* gather list of objects needing cleaning, return if none
     */
    const double threshold = cfg->gc_threshold / 100.0;
    std::vector<std::pair<int,int>> objs_to_clean;
    for (auto [u, o, n] : utilization) {
	if (u > threshold)
	    continue;
	if (objs_to_clean.size() > 32)
	    break;
	objs_to_clean.push_back(std::make_pair(o, n));
    }
    if (objs_to_clean.size() == 0) 
	return;

    /* find all live extents in objects listed in objs_to_clean:
     * - make bitmap from objs_to_clean
     * - find all entries in map pointing to those objects
     */
    std::vector<bool> bitmap(max_obj+1);
    for (auto it = objs_to_clean.begin(); it != objs_to_clean.end(); it++)
	bitmap[it->first] = true;

    extmap::objmap live_extents;
    for (auto it = map->begin(); it != map->end(); it++) {
	auto [base, limit, ptr] = it->vals();
	if (ptr.obj <= max_obj && bitmap[ptr.obj])
	    live_extents.update(base, limit, ptr);
    }
    obj_r_lock.unlock();

    /* everything before this point was in-memory only, with the 
     * translation instance mutex held, doing no I/O.
     */

    /* this is really gross. We really should check to see if data is
     * in the read cache, and put retrieved data there...
     */
    if (live_extents.size() > 0) {
	/* temporary file, delete on close. 
	 */
	char temp[cfg->cache_dir.size() + 20];
	sprintf(temp, "%s/gc.XXXXXX", cfg->cache_dir.c_str());
	int fd = mkstemp(temp);

	/* read all objects in completely. Someday we can check to see whether
	 * (a) data is already in cache, or (b) sparse reading would be quicker
	 */
	extmap::cachemap file_map;
	sector_t offset = 0;
	char *buf = (char*)malloc(20*1024*1024);

	for (auto [i,sectors] : objs_to_clean) {
	    objname name(prefix(i), i);
	    objstore->read_object(name.c_str(), buf, sectors*512UL, 0);
	    gc_sectors_read += sectors;
	    extmap::obj_offset _base = {i, 0}, _limit = {i, sectors};
	    file_map.update(_base, _limit, offset);
	    if (write(fd, buf, sectors*512) < 0)
		throw("no space");
	    offset += sectors;
	}
	free(buf);

	auto file_offset = offset;

	std::vector<_extent> all_extents;
	for (auto it = live_extents.begin(); it != live_extents.end(); it++) {
	    auto [base, limit, ptr] = it->vals();
	    all_extents.push_back((_extent){base, limit, ptr});
	}
	
	/* outstanding writes
	 */
	std::queue<translate_req*> requests;
	
	while (all_extents.size() > 0) {
	    sector_t sectors = 0, max = 16 * 1024; // 8MB
	    
	    auto it = all_extents.begin();
	    while (it != all_extents.end() && sectors < max) {
		auto [base, limit, ptr] = *it++;
		sectors += (limit - base);
		(void)ptr;	// suppress warning
	    }
	    std::vector<_extent> extents(std::make_move_iterator(all_extents.begin()),
					 std::make_move_iterator(it));
	    all_extents.erase(all_extents.begin(), it);
	    
	    /* figure out what we need to read from the file, create
	     * the header etc., then drop the lock and actually read it.
	     */
	    size_t prepend = 2048*sizeof(data_map) + 512;
	    char *_buf = (char*)malloc(sectors * 512 + prepend);
	    char *buf = _buf + prepend;

	    /* file offset, buffer ptr, length */
	    std::vector<std::tuple<sector_t,char*,size_t>> to_read; 
	    
	    off_t byte_offset = 0;
	    sector_t data_sectors = 0;
	    std::vector<data_map> obj_extents;

	    /* the extents may have been fragmented in the meantime...
	     */
	    sector_t _debug_sector = 0;
	    std::unique_lock lk(m); // not sure exactly why test10 needs this...
	    obj_r_lock.lock();
	    for (auto const & [base, limit, ptr] : extents) {
		for (auto it2 = map->lookup(base);
		     it2->base() < limit && it2 != map->end(); it2++) {
		     /* [_base,_limit] is a piece of the extent
		      * obj_base is where that piece starts in the object
		      */
		    auto [_base, _limit, obj_base] = it2->vals(base, limit);

		    /* skip if it's not still in the object, otherwise
		     * _obj_limit is where it ends.
		     */
		    if (obj_base.obj != ptr.obj)
			continue;
		    sector_t _sectors = _limit - _base;
		    auto obj_limit =
			extmap::obj_offset{obj_base.obj,
					   obj_base.offset+_sectors};

		    /* file_sector is where that piece starts in 
		     * the GC file...
		     */
		    auto it3 = file_map.lookup(obj_base);
		    auto [file_base,file_limit,file_sector] =
			it3->vals(obj_base, obj_limit);
		    (void)file_limit; // suppress warning
		    (void)file_base;  // suppress warning
		    assert(file_sector != _debug_sector);
		    _debug_sector = file_sector;
		    size_t bytes = _sectors*512;
		    to_read.push_back(std::make_tuple(file_sector,
						      buf+byte_offset, bytes));

		    obj_extents.push_back((data_map){(uint64_t)_base, (uint64_t)_sectors});

		    data_sectors += _sectors;
		    byte_offset += bytes;
		}
	    }
	    int32_t _seq = seq++;
	    obj_r_lock.unlock();
	    lk.unlock();

	    seq_record(_seq);
	    gc_sectors_written += data_sectors; // only written in this thread
	    int hdr_size = div_round_up(sizeof(obj_hdr) + sizeof(obj_data_hdr) +
					obj_extents.size()*sizeof(data_map), 512);
	    char *hdr = buf - hdr_size*512;
	    int hdr_sectors = make_gc_hdr(hdr, _seq, data_sectors,
					  obj_extents.data(), obj_extents.size());
	    assert(hdr_size == hdr_sectors);
	    sector_t offset = hdr_sectors;

	    int gc_sectors = byte_offset / 512;
	    obj_info oi = {.hdr = hdr_sectors, .data = gc_sectors,
			   .live = gc_sectors};

	    obj_w_lock.lock();
	    object_info[_seq] = oi;
	    std::vector<extmap::lba2obj> deleted;
	    for (auto const &e : obj_extents) {
		extmap::obj_offset oo = {_seq, offset};
		map->update(e.lba, e.lba+e.len, oo, &deleted);
		offset += e.len;
	    }

	    for (auto &d : deleted) {
		auto [base, limit, ptr] = d.vals();
		if (object_info.find(ptr.obj) == object_info.end())
		    continue;	// skip clone base
		object_info[ptr.obj].live -= (limit - base);
		assert(object_info[ptr.obj].live >= 0);
		total_live_sectors -= (limit - base);
	    }
	    obj_w_lock.unlock();

	    /* Now we actually read from the file
	     */
	    for (auto const & tpl : to_read) {
		auto [file_sector, ptr, bytes] = tpl;
		assert(file_sector < file_offset);  // total file size
		auto err = pread(fd, ptr, bytes, file_sector*512);
		assert(err == (ssize_t)bytes);
	    }
	    
	    auto t_req = new translate_req(_seq, this, true);
	    t_req->to_free.push_back(_buf);

	    if (stopped)
		return;
	    
	    objname name(prefix(_seq), _seq);
	    auto req = objstore->make_write_req(name.c_str(), hdr,
						(hdr_sectors+data_sectors)*512);
	    outstanding_writes++;
	    req->run(t_req);
	    requests.push(t_req);
	    
	    while (requests.size() > 8) {
		auto t = requests.front();
		t->wait();
		requests.pop();
	    }
	}
	while (requests.size() > 0) {
	    auto t = requests.front();
	    t->wait();
	    requests.pop();
	}
	close(fd);
	unlink(temp);
    }

    obj_w_lock.lock();
    for (auto it = objs_to_clean.begin(); it != objs_to_clean.end(); it++) {
	total_sectors -= object_info[it->first].data;
	object_info.erase(object_info.find(it->first));
    }
    obj_w_lock.unlock();
    
    /* write checkpoint *before* deleting any objects
     */
    if (stopped)
	return;
    if (objs_to_clean.size()) {
	int ckpt_seq = seq++;
	write_checkpoint(ckpt_seq);
    
	for (auto it = objs_to_clean.begin(); it != objs_to_clean.end(); it++) {
	    objname name(prefix(it->first), it->first);
	    objstore->delete_object(name.c_str());
	    gc_deleted++;		// single-threaded, no lock needed
	}
    }
}

void translate_impl::stop_gc(void) {
    delete misc_threads;
    std::unique_lock lk(m);
    while (gc_running)
	gc_cv.wait(lk);
}

void translate_impl::gc_thread(thread_pool<int> *p) {
    auto interval = std::chrono::milliseconds(100);
    //sector_t trigger = 128 * 1024 * 2; // 128 MB
    const char *name = "gc_thread";
    pthread_setname_np(pthread_self(), name);
	
    while (p->running) {
	std::unique_lock lk(m);
	p->cv.wait_for(lk, interval);
	if (!p->running)
	    return;

	/* check to see if we should run a GC cycle
	 */
	//if (total_sectors - total_live_sectors < trigger)
	//    continue;
	//if ((total_live_sectors / (double)total_sectors) > (cfg->gc_threshold / 100.0))
	//continue;

	gc_running = true;
	lk.unlock();

	do_gc(&p->running);

	lk.lock();
	gc_running = false;
	gc_cv.notify_all();
    }
}
    
/* ---------------- Debug ---------------- */

/* synchronous read from offset (in bytes)
 */
ssize_t translate_impl::readv(size_t offset, iovec *iov, int iovcnt) {
    smartiov iovs(iov, iovcnt);
    size_t len = iovs.bytes();
    int64_t base = offset / 512;
    int64_t sectors = len / 512, limit = base + sectors;

    /* various things break when map size is zero
     */
    if (map->size() == 0) {
	iovs.zero();
	return len;
    }

    /* object number, offset (bytes), length (bytes) */
    std::vector<std::tuple<int, size_t, size_t>> regions;
	
    auto prev = base;
    {
	std::unique_lock lk(m);
	std::shared_lock slk(*map_lock);

	for (auto it = map->lookup(base);
	     it != map->end() && it->base() < limit; it++) {
	    auto [_base, _limit, oo] = it->vals(base, limit);
	    if (_base > prev) {	// unmapped
		size_t _len = (_base - prev)*512;
		regions.push_back(std::tuple(-1, 0, _len));
	    }
	    size_t _len = (_limit - _base) * 512, _offset = oo.offset * 512;
	    regions.push_back(std::tuple((int)oo.obj, _offset, _len));
	    prev = _limit;
	}
    }

    if (regions.size() == 0) {
	iovs.zero();
	return 0;
    }
    
    size_t iov_offset = 0;
    for (auto [obj, _offset, _len] : regions) {
	auto slice = iovs.slice(iov_offset, iov_offset + _len);
	if (obj == -1)
	    slice.zero();
	else {
	    objname name(prefix(obj), obj);
	    auto [iov,iovcnt] = slice.c_iov();
	    objstore->read_object(name.c_str(), iov, iovcnt, _offset);
	}
	iov_offset += _len;
    }

    if (iov_offset < iovs.bytes()) {
	auto slice = iovs.slice(iov_offset, len - iov_offset);
	slice.zero();
    }
    
    return 0;
}

int translate_create_image(backend *objstore, const char *name,
			   uint64_t size) {
    char buf[4096];
    memset(buf, 0, 4096);

    auto _hdr = (obj_hdr*) buf;
    *_hdr = (obj_hdr){LSVD_MAGIC,
		      1,	  // version
		      {0},	  // UUID
		      LSVD_SUPER, // type
		      0,	  // seq
		      8,	  // hdr_sectors
		      0};	  // data_sectors
    uuid_generate_random(_hdr->vol_uuid);

    auto _super = (super_hdr*)(_hdr + 1);
    uint64_t sectors = size / 512;
    *_super = (super_hdr){sectors, // vol_size
			  0, 0,	   // checkpoint offset, len
			  0, 0,	   // clone offset, len
			  0, 0};   // snap offset, len
    
    auto rv = objstore->write_object(name, buf, 4096);
    return rv;
}

int translate_get_uuid(backend *objstore, const char *name, uuid_t &uu) {
    char buf[4096];
    int rv = objstore->read_object(name, buf, sizeof(buf), 0);
    if (rv < 0)
	return rv;
    auto hdr = (obj_hdr*)buf;
    memcpy(uu, hdr->vol_uuid, sizeof(uuid_t));
    return 0;
}

int translate_remove_image(backend *objstore, const char *name) {

    /* read the superblock to get the list of checkpoints
     */
    char buf[4096];
    int rv = objstore->read_object(name, buf, sizeof(buf), 0);
    if (rv < 0)
	return rv;
    auto hdr = (obj_hdr*) buf;
    auto sh = (super_hdr*)(hdr+1);

    if (hdr->magic != LSVD_MAGIC || hdr->type != LSVD_SUPER)
	return -1;

    int seq = 1;
    std::vector<uint32_t> ckpts;
    decode_offset_len<uint32_t>(buf, sh->ckpts_offset, sh->ckpts_len, ckpts);

    /* read the most recent checkpoint and get its object map
     */
    if (ckpts.size() > 0) {
	object_reader r(objstore);
	seq = ckpts.back();
	objname obj(name, seq);
	auto ckpt_buf = r.read_object_hdr(obj.c_str(), false);
	auto c_hdr = (obj_hdr*)ckpt_buf;
	auto c_data = (obj_ckpt_hdr*)(c_hdr+1);
	if (c_hdr->magic != LSVD_MAGIC || c_hdr->type != LSVD_CKPT)
	    return -1;
	std::vector<ckpt_obj> objects;
	decode_offset_len<ckpt_obj>(ckpt_buf, c_data->objs_offset,
				    c_data->objs_len, objects);

	/* delete all the objects in the objmap
	 */
	std::queue<request*> deletes;
	for (auto const & o : objects) {
	    objname obj(name, o.seq);
	    auto r = objstore->delete_object_req(obj.c_str());
	    r->run(NULL);
	    deletes.push(r);
	    while (deletes.size() > 8) {
		deletes.front()->wait();
		deletes.pop();
	    }
	}
	while (deletes.size() > 0) {
	    deletes.front()->wait();
	    deletes.pop();
	}
	
	/* delete all the checkpoints
	 */
	for (auto const & c : ckpts) {
	    objname obj(name, c);
	    objstore->delete_object(obj.c_str());
	}
    }
    /* delete any objects after the last checkpoint, up to the first run of
     * 32 missing sequence numbers
     */
    for (int n = 0; n < 16; seq++, n++) {
	objname obj(name, seq);
	if (objstore->delete_object(obj.c_str()) >= 0)
	    n = 0;
    }

    /* and delete the superblock
     */
    objstore->delete_object(name);
    return 0;
}

