#include <unistd.h>
#include <uuid/uuid.h>

#include <queue>
#include <map>
#include <thread>

#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <atomic>

#include "lsvd_types.h"
#include "smartiov.h"
#include "fake_rbd.h"
#include "config.h"
#include "extent.h"
#include "backend.h"
#include "translate.h"
#include "read_cache.h"
#include "journal.h"
#include "write_cache.h"
#include "image.h"

#include "objects.h"
#include "request.h"

#include "misc_cache.h"
#include "nvme.h"

#include "config.h"

// tuple :	used for retrieving maps
struct tuple {
    int base;
    int limit;
    int obj;                    // object map
    int offset;
    int plba;                   // write cache map
};

// getmap_s :	more helper structures
struct getmap_s {
    int i;
    int max;
    struct tuple *t;
};

extern translate *image_2_xlate(rbd_image_t image);


// struct rbd_image;
extern rbd_image *make_rbd_image(backend *b, translate *t,
				 write_cache *w, read_cache *r);


char *logbuf, *p_log, *end_log;
#include <stdarg.h>
std::mutex m;
void do_log(const char *fmt, ...) {
    std::unique_lock lk(m);
    if (!logbuf) {
	size_t sz = 64*1024;
	const char *env = getenv("LSVD_DEBUG_BUF");
	if (env)
	    sz = atoi(env);
	p_log = logbuf = (char*)malloc(sz);
	end_log = p_log + sz;
    }
    va_list args;
    va_start(args, fmt);
    ssize_t max = end_log - p_log - 1;
    if (max > 256)
	p_log += vsnprintf(p_log, max, fmt, args);
}

FILE *_fp;
void fp_log(const char *fmt, ...) {
    //std::unique_lock lk(m);
    if (_fp == NULL)
	_fp = fopen("/tmp/lsvd.log", "w");
    va_list args;
    va_start(args, fmt);
    vfprintf(_fp, fmt, args);
    fflush(_fp);
}


struct timelog {
    uint64_t loc : 8;
    uint64_t val : 48;
    uint64_t time;
};

struct timelog *tl;
std::atomic<int> tl_index;
int tl_max = 10000000;

#include <x86intrin.h>
void log_time(uint64_t loc, uint64_t value) {
    return;
    if (tl == NULL)
	tl = (struct timelog*)malloc(tl_max * sizeof(struct timelog));
    auto t = __rdtsc();
    auto i = tl_index++;
    if (i < tl_max) 
	tl[i] = {loc, value, t};
}

void save_log_time(void) {
    return;
    FILE *fp = fopen("/tmp/timelog", "wb");
    size_t bytes = tl_index * sizeof(struct timelog);
    fwrite(tl, bytes, 1, fp);
    fclose(fp);
}

/* random run-time debugging stuff, not used at the moment...
 */
#if CRC_DEBUGGER
#include <zlib.h>
static std::map<int,uint32_t> sector_crc;
char zbuf[512];
static std::mutex zm;

void add_crc(sector_t sector, iovec *iov, int niovs) {
    std::unique_lock lk(zm);
    for (int i = 0; i < niovs; i++) {
	for (size_t j = 0; j < iov[i].iov_len; j += 512) {
	    const unsigned char *ptr = j + (unsigned char*)iov[i].iov_base;
	    sector_crc[sector] = (uint32_t)crc32(0, ptr, 512);
	    sector++;
	}
    }
}

void check_crc(sector_t sector, iovec *iov, int niovs, const char *msg) {
    std::unique_lock lk(zm);
    for (int i = 0; i < niovs; i++) {
	for (size_t j = 0; j < iov[i].iov_len; j += 512) {
	    const unsigned char *ptr = j + (unsigned char*)iov[i].iov_base;
	    if (sector_crc.find(sector) == sector_crc.end()) {
		assert(memcmp(ptr, zbuf, 512) == 0);
	    }
	    else {
		unsigned crc1 = 0, crc2 = 0;
		assert((crc1 = sector_crc[sector]) == (crc2 = crc32(0, ptr, 512)));
	    }
	    sector++;
	}
    }
}

void list_crc(sector_t sector, int n) {
    for (int i = 0; i < n; i++)
	printf("%ld %08x\n", sector+i, sector_crc[sector+i]);
}
#endif

#ifdef PERF_DEBUGGER
#include <fcntl.h>
void getcpu(int pid, int tid, int &u, int &s) {
    if (tid == 0) 
	return;
    char procname[128];
    sprintf(procname, "/proc/%d/task/%d/stat", pid, tid);
    int fd = open(procname, O_RDONLY);
    char buf[512], *p = buf;
    read(fd, buf, sizeof(buf));
    close(fd);
    for (int i = 0; i < 13; i++)
	p = strchr(p, ' ') + 1;
    u = strtol(p, &p, 0);
    s = strtol(p, &p, 0);
}

int __dbg_wc_tid;
int __dbg_gc_tid;
int __dbg_fio_tid;
#include <sys/syscall.h>

int get_tid(void) {
    return syscall(SYS_gettid);
}

int __dbg_write1;		// writes launched
int __dbg_write2;		// writes completed
int __dbg_write3, __dbg_write4;	// objects written, completed

std::mutex *__dbg_wcache_m;
std::mutex *__dbg_xlate_m;
const char *__dbg_gc_state = "   -";
int __dbg_gc_reads;
int __dbg_gc_writes;
int __dbg_gc_deletes;
int __dbg_gc_n;

int __dbg_t_room;
int __dbg_w_room;

std::atomic<int> __dbg_in_lsvd;

bool read_lock(std::mutex *m) {
    if (!m)
	return false;
    bool val = m->try_lock();
    if (val)
	m->unlock();
    return !val;
}
bool read_lock(std::shared_mutex *m) {
    if (!m)
	return false;
    bool val = m->try_lock();
    if (val)
	m->unlock();
    return !val;
}
    
#include <sys/time.h>
struct timeval tv0;
double gettime(void) {
    if (tv0.tv_sec == 0)
	gettimeofday(&tv0, NULL);
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec - tv0.tv_sec + (tv.tv_usec - tv0.tv_usec) / 1.0e6;
}

std::atomic<int> __dbg_wq_1;
std::atomic<int> __dbg_wq_2;
std::atomic<int> __dbg_wq_3;
std::atomic<int> __dbg_wq_4;

void debug_thread(rbd_image *img) {
    int pid = getpid();
    int write1 = __dbg_write1, write2 = __dbg_write2;
    int write3 = 0, write4 = 0;
    int gcr = 0, gcw = 0, gcd = 0;
    int uf=0,sf=0,uw=0,sw=0,ug=0,sg=0;
    int t_room = 0, w_room = 0;
    int xlate_m = 0, wcache_m = 0, obj_m = 0;
    double time0 = gettime();
    int in_lsvd = 0;
    
    while (!img->done) {
	int t1 = __dbg_write1, t2 = __dbg_write2;
	int t3 = __dbg_write3, t4 = __dbg_write4;

	int n1 = __dbg_gc_reads, n2 = __dbg_gc_writes, n3 = __dbg_gc_deletes;

	int uf_,sf_,uw_,sw_,ug_,sg_;
	getcpu(pid, __dbg_fio_tid, uf_, sf_);
	getcpu(pid, __dbg_wc_tid, uw_, sw_);
	getcpu(pid, __dbg_gc_tid, ug_, sg_);

	int q1 = __dbg_wq_1;
	int q2 = __dbg_wq_2;
	int q3 = __dbg_wq_3;
	int q4 = __dbg_wq_4;
	double time1 = gettime();
	printf("%.3f %- 4d\t%- 4d\t%- 4d\t%- 4d\t%d %d %d\t%s %d\t%d %d %d\t%d %d %d %d %d %d\t%d %d\t%d\t%d %d %d %d\n",
	       time1-time0,
	       t1-write1, t2-write2, t3-write3, t4-write4,
	       xlate_m, wcache_m, obj_m,
	       __dbg_gc_state, __dbg_gc_n, 
	       n1-gcr, n2-gcw, n3-gcd,
	       uf_-uf, sf_-sf, uw_-uw, sw_-sw, ug_-ug, sg_-sg,
	       w_room, t_room,
	       in_lsvd,
	       q1, q2, q3, q4);
	time0 = time1;
	write1 = t1; write2 = t2; write3 = t3; write4 = t4;
	gcr = n1; gcw = n2; gcd = n3;
	uf=uf_; sf=sf_; uw=uw_; sw=sw_; ug=ug_; sg=sg_;
	t_room = w_room = 0;
	xlate_m = wcache_m = obj_m = 0;
	in_lsvd = 0;
	for (int i = 0; i < 20 && !img->done; i++) {
	    t_room += __dbg_t_room;
	    w_room += __dbg_w_room;
	    xlate_m += read_lock(__dbg_xlate_m);
	    wcache_m += read_lock(__dbg_wcache_m);
	    obj_m += read_lock(&img->map_lock);
	    in_lsvd += (__dbg_in_lsvd > 0);
	    usleep(10000);
	}
    }
}
#endif // PERF_DEBUGGER
