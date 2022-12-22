#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <argp.h>

#include <random>
#include <chrono>
#include <filesystem>
namespace fs = std::filesystem;
#include <iostream>
#include <queue>
#include <map>
#include <mutex>
#include <atomic>

#include "fake_rbd.h"
#include "objects.h"
#include "lsvd_types.h"

extern bool __lsvd_dbg_reverse;
extern bool __lsvd_dbg_be_delay;
extern bool __lsvd_dbg_be_delay_ms;
extern bool __lsvd_dbg_be_threads;

std::mt19937_64 rng;

struct cfg {
    const char *cache_dir;
    const char *cache_size;
    const char *obj_prefix;
    const char *backend;
    int    run_len;
    size_t window;
    int    image_sectors;
    float  read_fraction;
    int    n_runs;
    std::vector<long> seeds;
    bool   reopen;
    bool   restart;
    bool   verbose;
    bool   existing;
};


/* empirical fit to the pattern of writes in the ubuntu install,
 * truncated to 2MB writes max
 */
int n_sectors(void) {
    struct { float p; int max; } params[] = {
	{0.5, 8},
	{0.25, 32},
	{0.125, 64},
	{0.0625, 128},
	{1.0, 4096}};
    std::uniform_real_distribution<> uni(0.0,1.0);
    float r = uni(rng);
    int max = 8;

    for (size_t i = 0; i < sizeof(params) / sizeof(params[0]); i++) {
	if (r < params[i].p) {
	    max = params[i].max;
	    break;
	}
	r -= params[i].p;
    }

    std::uniform_int_distribution<int> uni_max(8/8,max/8);
    return uni_max(rng) * 8;
}

int gen_lba(int max, int n) {
    std::uniform_int_distribution<int> uni(0,(max-n)/8);
    return uni(rng) * 8;
}

char *rnd_data;
const int max_sectors = 32768;

void init_random(void) {
    size_t bytes = max_sectors * 512;
    rnd_data = (char*)malloc(bytes);
    for (long *p = (long*)rnd_data, *end = (long*)(rnd_data+bytes); p<end; p++)
	*p = rng();
}

void get_random(char *buf, int lba, int sectors, int seq) {
    int slack = (max_sectors - sectors) * 512;
    std::uniform_int_distribution<int> uni(0,slack);
    int offset = uni(rng);
    memcpy(buf, rnd_data+offset, sectors*512);
    for (auto p = (int*)buf; sectors > 0; sectors--, p += 512/4) {
	p[0] = lba++;
	p[1] = seq;
    }
}

extern "C" void rbd_uuid(rbd_image_t image, uuid_t *uuid);
std::string get_cache_name(rbd_image_t img) {
    uuid_t uu;
    rbd_uuid(img, &uu);
    char uuid_str[64];
    uuid_unparse(uu, uuid_str);
    return std::string(uuid_str) + ".cache";
}

void clean_cache(std::string cache_dir) {
    const char *suffix = ".cache";
    for (auto const& dir_entry : fs::directory_iterator{cache_dir}) {
	std::string entry{dir_entry.path().filename()};
	if (!strcmp(suffix, entry.c_str() + entry.size() - strlen(suffix)))
	    fs::remove(dir_entry.path());
	if (!strncmp(entry.c_str(), "gc.", 3))
	    fs::remove(dir_entry.path());
    }
}

#include <zlib.h>
static std::map<int,uint32_t> sector_crc;
static std::map<int,int> sector_seq;
char _zbuf[512];
static std::atomic<int> _seq;
static std::mutex zm;

void add_crc(sector_t sector, const char *_buf, size_t bytes, int seq, bool verbose) {
    auto buf = (const unsigned char *)_buf;
    std::unique_lock lk(zm);
    if (verbose)
	printf("%d %ld", seq, sector);
    for (size_t i = 0; i < bytes; i += 512) {
	const unsigned char *ptr = buf + i;
	auto crc = sector_crc[sector] = (uint32_t)crc32(0, ptr, 512);
	sector_seq[sector] = seq;
	if (verbose)
	    printf(" %x", crc);
	sector++;
    }
    if (verbose)
	printf("\n");
}

void check_crc(sector_t sector, const char *_buf, size_t bytes) {
    auto buf = (const unsigned char *)_buf;
    std::unique_lock lk(zm);
    for (size_t i = 0; i < bytes; i += 512) {
	const unsigned char *ptr = buf + i;
	if (sector_crc.find(sector) == sector_crc.end()) {
	    assert(memcmp(ptr, _zbuf, 512) == 0);
	}
	else {
	    unsigned crc1 = 0, crc2 = 0;
	    assert((crc1 = sector_crc[sector]) == (crc2 = crc32(0, ptr, 512)));
	}
	sector++;
    }
}

unsigned show_crc(sector_t sector) {
    return sector_crc[sector];
}
unsigned show_seq(sector_t sector) {
    return sector_seq[sector];
}

typedef std::pair<rbd_completion_t,char*> opinfo;

void drain(std::queue<opinfo> &q, size_t window) {
    while (q.size() > window) {
	auto [c,ptr] = q.front();
	q.pop();
	rbd_aio_wait_for_complete(c);
	rbd_aio_release(c);
	free(ptr);
    }
}

bool started = false;

void run_test(unsigned long seed, struct cfg *cfg) {
    printf("seed: 0x%lx\n", seed);
    rng.seed(seed);

    init_random();
    rados_ioctx_t io = 0;
    rbd_image_t img;

    setenv("LSVD_CACHE_SIZE", cfg->cache_size, 1);
    setenv("LSVD_BACKEND", cfg->backend, 1);
    setenv("LSVD_CACHE_DIR", cfg->cache_dir, 1);
    
    if (!started || cfg->restart) {
	//clean_cache(cfg->cache_dir);
	rbd_remove(io, cfg->obj_prefix);
	rbd_create(io, cfg->obj_prefix, cfg->image_sectors, NULL);
	sector_crc.clear();
	started = true;
    }

    std::vector<data_map> writes;
    
    int rv = rbd_open(io, cfg->obj_prefix, &img, NULL);
    assert(rv >= 0);

    std::queue<std::pair<rbd_completion_t,char*>> q;
    std::uniform_real_distribution<> uni(0.0,1.0);
    
    for (int i = 0; i < cfg->run_len; i++) {
	drain(q, cfg->window-1);
	if (i % 1000 == 999)
	    printf("+"), fflush(stdout);
	rbd_completion_t c;
	rbd_aio_create_completion(NULL, NULL, &c);

	int n = n_sectors();
	int lba = gen_lba(cfg->image_sectors, n);
	auto ptr = (char*)aligned_alloc(512, n*512);
	
	q.push(std::make_pair(c, ptr));
	if (uni(rng) < cfg->read_fraction) {
	    rbd_aio_read(img, 512L * lba, 512L * n, ptr, c);
	}
	else {
	    auto s = ++_seq;
	    get_random(ptr, lba, n, s);
	    rbd_aio_write(img, 512L * lba, 512L * n, ptr, c);
	    add_crc(lba, ptr, n*512L, s, cfg->verbose);
	    writes.push_back({(uint64_t)lba,(uint64_t)n});
	}
    }
    drain(q, 0);
    printf("\n");

    extern char *p_log, *end_log;
    if (p_log && p_log+16 < end_log)
	p_log += snprintf(p_log, end_log-p_log, "\n\nWRITE DONE\n\n");
			 
    if (cfg->reopen) {
	rbd_close(img);
	if (p_log && p_log+16 < end_log)
	    p_log += snprintf(p_log, end_log-p_log, "\nCLOSE done\n\n");
	if (rbd_open(io, cfg->obj_prefix, &img, NULL) < 0)
	    printf("failed: rbd_open\n"), exit(1);
	if (p_log && p_log+16 < end_log)
	    p_log += snprintf(p_log, end_log-p_log, "\nREOPEN done\n\n");	
    }

    auto tmp = __lsvd_dbg_be_delay;
    __lsvd_dbg_be_delay = false;
    auto buf = (char*)aligned_alloc(512, 64*1024);
    int i = 0;
    for (int sector = 0; sector < cfg->image_sectors; sector += 64*2) {
	rbd_read(img, sector*512L, 64*1024, buf);
	check_crc(sector, buf, 64*1024);
	if (++i > cfg->image_sectors / 128 / 10) {
	    printf("-"); fflush(stdout);
	    i = 0;
	}
    }
    printf("\n");
    free(buf);
    rbd_close(img);
    __lsvd_dbg_be_delay = tmp;
    if (p_log && p_log+16 < end_log)
	p_log += snprintf(p_log, end_log-p_log, "\n\nREAD DONE\n\n");
}


static char args_doc[] = "RUNS";

static struct argp_option options[] = {
    {"seed",     's', "S",    0, "use this seed (one run)"},
    {"len",      'l', "N",    0, "run length"},
    {"window",   'w', "W",    0, "write window"},
    {"size",     'z', "S",    0, "volume size (e.g. 1G, 100M)"},
    {"cache-dir",'d', "DIR",  0, "cache directory"},
    {"prefix",   'p', "PREFIX", 0, "object prefix"},
    {"reads",    'r', "FRAC", 0, "fraction reads (0.0-1.0)"},
    {"close",    'c', 0,      0, "close and re-open"},
    {"keep",     'k', 0,      0, "keep data between tests"},
    {"verbose",  'v', 0,      0, "print LBAs and CRCs"},
    {"reverse",  'R', 0,      0, "reverse NVMe completion order"},    
    {"existing", 'x', 0,      0, "don't delete existing cache"},    
    {"delay",    'D', 0,      0, "add random backend delays"},    
    {"rados",    'O', 0,      0, "use RADOS"},
    {"cache-size",'Z', "N",    0, "cache size (K/M/G)"},
    {0},
};

struct cfg _cfg = {
    "/tmp",			// cache_dir
    "/tmp/bkt/obj",		// obj_prefix
    "100m",                     // cache_size
    "file",                	// backend
    10000, 			// run_len
    16,				// window
    1024*1024*2,		// image_sectors,
    0.0,			// read_fraction
    1,				// n_runs
    {},				// seeds
    false,			// reopen
    true,			// restart
    false,			// verbose
    false};			// existing

off_t parseint(char *s)
{
    off_t val = strtoul(s, &s, 0);
    if (toupper(*s) == 'G')
        val *= (1024*1024*1024);
    if (toupper(*s) == 'M')
        val *= (1024*1024);
    if (toupper(*s) == 'K')
        val *= 1024;
    return val;
}

static error_t parse_opt(int key, char *arg, struct argp_state *state)
{
    switch (key) {
    case ARGP_KEY_INIT:
	break;
    case ARGP_KEY_ARG:
        _cfg.n_runs = atoi(arg);
        break;
    case 's':
	_cfg.seeds.push_back(strtoul(arg, NULL, 0));
	break;
    case 'l':
	_cfg.run_len = atoi(arg);
	break;
    case 'w':
	_cfg.window = atoi(arg);
	break;
    case 'z':
	_cfg.image_sectors = parseint(arg) / 512;
	break;
    case 'd':
	_cfg.cache_dir = arg;
	break;
    case 'p':
	_cfg.obj_prefix = arg;
	break;
    case 'r':
	_cfg.read_fraction = atof(arg);
	break;
    case 'c':
	_cfg.reopen = true;
	break;
    case 'k':
	_cfg.restart = false;
	break;
    case 'v':
	_cfg.verbose = true;
	break;
    case 'R':
	__lsvd_dbg_reverse = true;
	break;
    case 'x':
	_cfg.existing = true;
	break;
    case 'D':
	__lsvd_dbg_be_delay = true;
	break;
    case 'O':
	_cfg.backend = "rados";
	break;
    case 'Z':
	_cfg.cache_size = arg;
	break;
    case ARGP_KEY_END:
        break;
    }
    return 0;
}
static struct argp argp = { options, parse_opt, NULL, args_doc};

int main(int argc, char **argv) {
    argp_parse (&argp, argc, argv, 0, 0, 0);

    if (_cfg.seeds.size() > 0) {
	for (int i = 0; i < _cfg.n_runs; i++)
	    for (auto s : _cfg.seeds)
		run_test(s, &_cfg);
    }
    else {
	auto now = std::chrono::system_clock::now();
	auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
	auto value = now_ms.time_since_epoch();
	long seed = value.count();

	rng.seed(seed);

	std::vector<unsigned long> seeds;
	for (int i = 0; i < _cfg.n_runs; i++)
	    seeds.push_back(rng());
	for (auto s : seeds)
	    run_test(s, &_cfg);
    }
}
