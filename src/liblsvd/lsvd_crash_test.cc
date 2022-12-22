#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <argp.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <zlib.h>

#include <random>
#include <chrono>
#include <filesystem>
namespace fs = std::filesystem;
#include <iostream>
#include <queue>
#include <map>
#include <mutex>
#include <atomic>
#include <algorithm>
#include <fstream>

#include "fake_rbd.h"
#include "objects.h"
#include "journal.h"
#include "lsvd_types.h"
#include "lsvd_debug.h"
#include "objname.h"
#include "extent.h"

extern bool __lsvd_dbg_reverse;
extern bool __lsvd_dbg_be_delay;
extern bool __lsvd_dbg_be_delay_ms;
extern bool __lsvd_dbg_be_threads;
extern bool __lsvd_dbg_rename;

std::mt19937_64 rng;

struct cfg {
    const char *cache_dir;
    const char *cache_size;
    const char *obj_prefix;
    int    run_len;
    size_t window;
    int    image_sectors;
    float  read_fraction;
    int    n_runs;
    std::vector<long> seeds;
    bool   restart;
    bool   verbose;
    bool   existing;
    int    lose_objs;
    bool   wipe_cache;
    size_t lose_writes;
    bool   wipe_data;
    bool   sleep;
    long   seed2;
};


/* empirical fit to the pattern of writes in the ubuntu install,
 * except it has lots more writes in the 128..32768 sector range
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


std::string get_cache_filename(struct cfg *cfg) {
    char buf[4096];

    std::ifstream obj_fp(std::string(cfg->obj_prefix), std::ios::binary);
    assert(obj_fp.read(buf, sizeof(buf)));
    auto super = (obj_hdr*)buf;

    char uuid_str[64];
    uuid_unparse(super->vol_uuid, uuid_str);
    auto uuid_name = std::string(cfg->cache_dir) + "/" + std::string(uuid_str) + ".cache";
    
    auto stem = fs::path(cfg->obj_prefix).filename();
    auto obj_name = std::string(cfg->cache_dir) + "/" + std::string(stem) + ".cache";

    if (fs::exists(obj_name))
	return obj_name;
    if (fs::exists(uuid_name))
	return uuid_name;
    assert(false);
}

void obj_delete(struct cfg *cfg) {
    std::vector<std::pair<int, std::string>> files;
    std::string dir = fs::path(cfg->obj_prefix).parent_path();
    auto stem = fs::path(cfg->obj_prefix).filename();
    size_t stem_len = strlen(stem.c_str());
    
    for (auto const& dir_entry : fs::directory_iterator{dir}) {
	std::string entry{dir_entry.path().filename()};
	if (strncmp(entry.c_str(), stem.c_str(), stem_len) == 0)
	    if (entry.size() == stem_len + 9) {
		int seq = strtol(entry.c_str() + stem_len+1, NULL, 16);
		files.push_back(std::make_pair(seq, entry));
	    }
	}

    std::sort(files.begin(), files.end());

    /* find the last checkpoint - we're guaranteed not to have lost
     * that or any preceding objects
     */
    int last_ckpt = 0;
    for (auto [seq, name] : files) {
	std::string path = dir + "/" + name;
	std::ifstream fp(path, std::ios::binary);
	char buf[512];
	if (fp.read(buf, sizeof(buf))) {
	    auto _hdr = (obj_hdr*) buf;
	    if (_hdr->type == LSVD_CKPT)
		last_ckpt = seq;
	}
    }

    printf("last_ckpt %08x last_file %08x\n", last_ckpt, files[files.size()-1].first);
    
    std::uniform_real_distribution<> uni(0.0,1.0);

    for (int i = 0, j = files.size()-1; i < cfg->lose_objs; i++, j--) {
	auto [seq, file] = files[j];
	if (seq <= last_ckpt)
	    break;
	if (uni(rng) < 0.5) {
	    fs::rename(dir + "/" + file, dir + "/" + file + ".bak");
	    //fs::remove(dir + "/" + file);
	    printf("rm %s\n", file.c_str());
	}
    }
}

void lose_writes(struct cfg *cfg) {
    auto cache = get_cache_filename(cfg);
    int fd = open(cache.c_str(), O_RDWR, 0777);
    
    char buf[4096];
    assert(pread(fd, buf, sizeof(buf), 0) == sizeof(buf));
    auto super = (j_super*)buf;
    assert(super->magic == LSVD_MAGIC && super->type == LSVD_J_SUPER);

    assert(pread(fd, buf, sizeof(buf), super->write_super * 4096) == 4096);
    auto w_super = (j_write_super*)buf;
    assert(w_super->magic == LSVD_MAGIC && w_super->type == LSVD_J_W_SUPER);

    auto base = w_super->base, limit = w_super->limit;
    std::vector<int> header_pages;
    uint32_t seq = 0;
    
    for (int i = base; i < limit; ) {
	assert(pread(fd, buf, sizeof(buf), i * 4096) == 4096);
	auto hdr = (j_hdr*)buf;
	if (seq == 0)
	    seq = hdr->seq - 1;
	if (hdr->magic != LSVD_MAGIC || hdr->type != LSVD_J_DATA || hdr->seq != seq+1)
	    break;
	seq++;
	header_pages.push_back(i);
	i += hdr->len;
    }
    
    memset(buf, 0, sizeof(buf));

    std::uniform_real_distribution<> uni(0.0,1.0);
    std::reverse(header_pages.begin(), header_pages.end());
    for (size_t i = 0; i < cfg->lose_writes && i < header_pages.size(); i++)
	if (uni(rng) < 0.5) {
	    int n = header_pages[i];
	    assert(pwrite(fd, buf, sizeof(buf), n * 4096) == 4096);
	    printf("overwriting %d\n", n);
	}
    close(fd);
}

int print_status(struct cfg *cfg) {
    auto prefix = std::string(cfg->obj_prefix);
    auto stem = fs::path(prefix).filename();
    auto parent = fs::path(prefix).parent_path();
    size_t stem_len = strlen(stem.c_str());
    
    /* find the most recent object written
     */
    std::vector<std::pair<int, std::string>> files;
    for (auto const& dir_entry : fs::directory_iterator{parent}) {
	std::string entry{dir_entry.path().filename()};
	if (strncmp(entry.c_str(), stem.c_str(), stem_len) == 0)
	    if (entry.size() == stem_len + 9) {
		int seq = strtol(entry.c_str() + stem_len+1, NULL, 16);
		files.push_back(std::make_pair(seq, entry));
	    }
	}

    std::sort(files.begin(), files.end());
    auto end = files.size() - 1;
    printf("last object: %x %s\n", files[end].first, files[end].second.c_str());
    int rv = files[end].first;
    auto min = std::max(0, (int)files.size() - 20);
    
    int n = files[min].first;
    const char *msg = "missing objects:\n";
    for (int i = min; i < (int)files.size(); i++, n++) {
	while (n < files[i].first) {
	    printf("%s%s.%08x\n", msg, stem.c_str(), n++);
	    msg = "";
	}
    }
    for (int i = min; i < (int)files.size(); i++)
	printf("%s\n", files[i].second.c_str());
    
    auto cache = get_cache_filename(cfg);
    char buf[4096];
    int fd = open(cache.c_str(), O_RDONLY);
    pread(fd, buf, sizeof(buf), 0);
    auto super = (j_super*)buf;
    assert(super->magic == LSVD_MAGIC && super->type == LSVD_J_SUPER);
    pread(fd, buf, sizeof(buf), super->write_super * 4096);
    auto w_super = (j_write_super*)buf;
    assert(w_super->magic == LSVD_MAGIC && w_super->type == LSVD_J_W_SUPER);
    auto base = w_super->base, limit = w_super->limit;

    int seq = 0, highest = 0;
    for (int i = base; i < limit;) {
	pread(fd, buf, sizeof(buf), i * 4096);
	auto hdr = (j_hdr*)buf;
	if (seq == 0)
	    seq = hdr->seq - 1;
	if (hdr->magic != LSVD_MAGIC || (int)hdr->seq != seq+1)
	    break;
	assert(hdr->len != 0);
	seq++;
	highest = i;
	i = i + hdr->len;
    }
    printf("last write cache page: %d\n", highest);
    close(fd);
    return rv;
}

int super_n;
char super_buf[4096];
std::vector<std::string> ckpt_files;

void save_super(struct cfg *cfg) {
    char buf[10];
    sprintf(buf, "%d", super_n++);
    std::string super(cfg->obj_prefix);
    fs::copy_file(super, super + "." + std::string(buf));
    int fd = open(cfg->obj_prefix, O_RDONLY);
    read(fd, super_buf, sizeof(super_buf));
    printf("saved %s crc %08x\n", cfg->obj_prefix,
	   (uint32_t)crc32(0, (const unsigned char*)super_buf, 4096));
    close(fd);
    auto h = (obj_hdr*)super_buf;
    auto sh = (super_hdr*)(h+1);
    auto cp = (int*)(super_buf + sh->ckpts_offset);
    ckpt_files.clear();
    for (int i = 0; i < sh->ckpts_len/4; i++) {
	char buf[128];
	sprintf(buf, "%s.%08x", cfg->obj_prefix, cp[i]);
	ckpt_files.push_back(std::string(buf));
	assert(access(buf, R_OK) == 0);
    }
}

void restore_super(struct cfg *cfg) {
    int fd = open(cfg->obj_prefix, O_WRONLY, 0);
    printf("restoring super crc %08x\n",
	   (uint32_t)crc32(0, (const unsigned char*)super_buf, 4096));
    pwrite(fd, super_buf, sizeof(super_buf), 0);
    close(fd);
    for (auto s : ckpt_files) {
	if (access(s.c_str(), R_OK) != 0) {
	    printf("resurrecting %s\n", s.c_str());
	    fs::rename(s + ".bak", s);
	}
    }
}

void get_obj_files(std::string prefix, std::vector<std::string> &files) {
    auto stem = fs::path(prefix).filename();
    auto parent = fs::path(prefix).parent_path();
    size_t stem_len = strlen(stem.c_str());

    for (auto const& dir_entry : fs::directory_iterator{parent}) {
        std::string entry{dir_entry.path().filename()};
        if (strncmp(entry.c_str(), stem.c_str(), stem_len) == 0)
            if (entry.size() == stem_len + 9)
		files.push_back(entry);
    }
    std::sort(files.begin(), files.end());
}

void check_rcache(struct cfg *cfg) {
    // crc32 -> <obj#,block>
    std::map<uint32_t,std::pair<int,int>> crcmap;
    std::map<std::pair<int,int>,uint32_t> objmap;
    bool failed = false;
    
    auto cache = get_cache_filename(cfg);
    int fd = open(cache.c_str(), O_RDONLY);
    char buf1[4096];
    pread(fd, buf1, 4096, 2*4096);
    auto r_super = (j_read_super*)buf1;
    size_t map_bytes = r_super->map_blocks * 4096;
    auto _flat_map = malloc(map_bytes);
    pread(fd, _flat_map, map_bytes, r_super->map_start * 4096);
    auto flat_map = (extmap::obj_offset*)_flat_map;

    std::set<int> objects;

    for (int i = 0; i < r_super->units; i++) {
	auto oo = flat_map[i];
	if (objects.find(oo.obj) != objects.end())
	    continue;
	char name[128];
	sprintf(name, "%s.%08x", cfg->obj_prefix, (uint32_t)oo.obj);
	int fd = open(name, O_RDONLY);
	if (fd == -1) {
	    sprintf(name, "%s.%08x.bak", cfg->obj_prefix, (uint32_t)oo.obj);
	    fd = open(name, O_RDONLY);
	}
	
	char buf[64*1024];
	for (int i = 0, rv = sizeof(buf); rv == sizeof(buf); i++) {
	    memset(buf, 0, sizeof(buf));
	    if ((rv = read(fd, buf, sizeof(buf))) <= 0)
		break;
	    uint32_t crc = crc32(0, (unsigned char*)buf, sizeof(buf));
	    auto oop = std::make_pair((int)oo.obj, (int)i);
	    crcmap[crc] = oop;
	    objmap[oop] = crc;
	}
	objects.insert(oo.obj);
	close(fd);
    }

    // note - all-zeros 64KB CRC = d7978eeb
    for (int i = 0; i < r_super->units; i++) {
	auto oo = flat_map[i];
	if (oo.obj == 0)
	    continue;
	auto _oo = std::make_pair((int)oo.obj, (int)oo.offset);
	assert(objmap.find(_oo) != objmap.end());
	auto crc1 = objmap[_oo];
	char buf[64*1024];
	memset(buf, 0, sizeof(buf));
	// 16 4KB pages in a 64KB block
	pread(fd, buf, sizeof(buf), (r_super->base + i*16) * 4096L);
	uint32_t crc2 = crc32(0, (unsigned char*)buf, 64*1024);
	if (crc1 != crc2) {
	    failed = true;
	    printf("%d (%d.%d) failed: %08x (should be %08x)\n", i,
		   (int)oo.obj, (int)oo.offset, crc2, crc1);
	    if (crcmap.find(crc2) != crcmap.end()) {
		auto [obj,offset] = crcmap[crc2];
		printf("  %08x is for %d.%d\n", crc2, obj, offset);
	    }
	}
    }

    assert(!failed);
}

char wcache_state_buf[4096];
int backup_n;
void save_wcache_state(struct cfg *cfg) {
    auto cache = get_cache_filename(cfg);
    char buf[10];
    sprintf(buf, "%d", backup_n++);

    fs::copy_file(cache, cache + "." + std::string(buf));

    int fd = open(cache.c_str(), O_RDONLY);
    pread(fd, wcache_state_buf, 4096, 1*4096); // assume write super is block 1
    auto super = (j_super*)wcache_state_buf;
    assert(super->magic == LSVD_MAGIC && super->type == LSVD_J_W_SUPER);
    close(fd);
}
void restore_wcache_state(struct cfg *cfg) {
    auto cache = get_cache_filename(cfg);
#if 0
    fs::copy_file(cache + ".bak", cache, fs::copy_options::overwrite_existing);
#else
    int fd = open(cache.c_str(), O_WRONLY, 0777);
    pwrite(fd, wcache_state_buf, 4096, 1*4096); // assume write super is block 1
    close(fd);
#endif
}

extern char *p_log, *logbuf;

struct triple {
    int lba;
    int seq;
    uint32_t crc;
};

struct op {
    int sector;
    int len;
    int seq;
    std::vector<uint32_t> *crcs;
};

#include <sys/mman.h>

extern bool __lsvd_dbg_no_gc;

void run_test(struct cfg *cfg) {
    const int buflen = 16000000;
    auto child_logbuf = (char*) mmap(NULL, buflen, PROT_READ | PROT_WRITE, 
				     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    rados_ioctx_t io = 0;
    rbd_image_t img;
    bool started = false;
    int seq = 0;
    std::vector<triple> should_be_crcs(cfg->image_sectors);
    std::vector<op> op_crcs;

    setenv("LSVD_CACHE_SIZE", cfg->cache_size, 1);
    setenv("LSVD_BACKEND", "file", 1);
    setenv("LSVD_CACHE_DIR", cfg->cache_dir, 1);
    
    std::vector<std::tuple<int,int>> io_list;

    for (auto s : cfg->seeds) {
#if 0
	p_log = logbuf;
	if (p_log)
	    *p_log = 0;
#endif
	do_log("RESTART\n\n");

	printf("seed: 0x%lx\n", s);
	rng.seed(s);

	init_random();

	if (!started || cfg->restart) {
	    clean_cache(cfg->cache_dir);
	    rbd_remove(io, cfg->obj_prefix);
	    rbd_create(io, cfg->obj_prefix, cfg->image_sectors, NULL);
	    started = true;
	    seq = 0;
	    for (int i = 0; i < cfg->image_sectors; i++)
		should_be_crcs[i] = (triple){0, 0, 0xb2aa7578};
	    op_crcs.clear();
	}

	std::uniform_real_distribution<> uni(0.0,1.0);
	int n_requests = cfg->run_len + (uni(rng)-0.5)*100;
    

	int start = io_list.size();
	for (int i = 0; i < n_requests; i++) {
	    int n = n_sectors();
	    int lba = gen_lba(cfg->image_sectors, n);
	    io_list.push_back(std::make_tuple(lba, n));
	}

	int pid = fork();
	if (pid == 0) {
	    extern char *logbuf, *p_log, *end_log;

	    logbuf = child_logbuf;
	    p_log = (char*)memchr(logbuf, 0, buflen);
	    end_log = child_logbuf+buflen;
	    do_log("\n\nRESTART\n\n\n");

	    if (cfg->sleep) {
		printf("pid %d\n", getpid());
		sleep(10);
	    }
	    if (rbd_open(io, cfg->obj_prefix, &img, NULL) < 0)
		printf("failed: rbd_open\n"), exit(1);

	    std::queue<std::pair<rbd_completion_t,char*>> q;
	    for (int i = 0; i < n_requests; i++) {
		drain(q, cfg->window-1);
		if (i % 1000 == 999)
		    printf("+"), fflush(stdout);
		rbd_completion_t c;
		rbd_aio_create_completion(NULL, NULL, &c);

		auto [lba, n] = io_list[start+i];
		auto ptr = (char*)aligned_alloc(512, n*512);
	
		q.push(std::make_pair(c, ptr));
		if (uni(rng) < cfg->read_fraction) {
		    rbd_aio_read(img, 512L * lba, 512L * n, ptr, c);
		}
		else {
		    auto s = ++seq;
		    get_random(ptr, lba, n, s);
		    rbd_aio_write(img, 512L * lba, 512L * n, ptr, c);
		}
	    }
	    printf(" K\n");
	    _exit(0);
	}

	int start2 = op_crcs.size();
	/* this relies on the random number stream being identical in 
	 * the parent and child processes
	 */
	int n_writes = 0;
	for (int i = 0; i < n_requests; i++) {
	    if (uni(rng) < cfg->read_fraction)
		continue;
	    n_writes++;
	    auto [lba, n] = io_list[start+i];
	    auto ptr = (char*)malloc(n*512);
	    auto s = ++seq;
	    get_random(ptr, lba, n, s);

	    auto crc_list = new std::vector<uint32_t>();
	    for (size_t j = 0; j < 512UL*n; j += 512) {
		auto p = (const unsigned char*)ptr + j;
		auto crc = (uint32_t)crc32(0, p, 512);
		crc_list->push_back(crc);
	    }
	    op_crcs.push_back((op){lba, n, s, crc_list});
	    free(ptr);
	}

	int status;
	waitpid(pid, &status, 0);
	int last_obj = print_status(cfg);

	
	if (cfg->lose_objs)
	    obj_delete(cfg);
	if (cfg->wipe_cache)
	    clean_cache(cfg->cache_dir);
	else if (cfg->lose_writes)
	    lose_writes(cfg);

	if (!cfg->restart) {
	    save_wcache_state(cfg);
	    save_super(cfg);
	}
	
	//check_rcache(cfg);

	extern bool __lsvd_dbg_no_gc;
	__lsvd_dbg_no_gc = true;
	if (rbd_open(io, cfg->obj_prefix, &img, NULL) < 0)
	    printf("failed: rbd_open\n"), exit(1);

	//check_rcache(cfg);
	extern void do_log(const char*, ...);
	do_log("cache checked\n");
	auto tmp = __lsvd_dbg_be_delay;
	__lsvd_dbg_be_delay = false;

	const int bufsize = 128*1024;
	auto buf = (char*)aligned_alloc(512, bufsize);
	std::vector<triple> image_info(cfg->image_sectors);
    
	int i = 0;
	for (int sector = 0; sector < cfg->image_sectors; sector += bufsize/512) {
	    memset(buf, 0, bufsize);
	    rbd_read(img, sector*512L, bufsize, buf);
	    for (int j = 0; j < bufsize; j += 512) {
		auto p = (const unsigned char*)buf + j;
		assert(*(int*)p == sector + j/512 || *(int*)p == 0);
		assert(*(int*)(p+4) >= 0);
		uint32_t crc = crc32(0, p, 512);
		image_info[sector + j/512] = (triple){*(int*)p,
						      *(int*)(p+4), crc};
		assert(*(int*)(p+4) <= (int)image_info.size()+1);
	    }
	    if (++i > cfg->image_sectors / (bufsize/512) / 10) {
		printf("-"); fflush(stdout);
		i = 0;
	    }
	}
	printf("\n");
    
	rbd_close(img);
	//check_rcache(cfg);

	int max_seq = 0;
	for (int i = 0; i < cfg->image_sectors; i++)
	    if (image_info[i].seq > max_seq) {
		max_seq = image_info[i].seq;
		assert(max_seq <= (int)op_crcs.size()+1);
	    }

	printf("max_seq = %d start = %d start2 = %d\n", max_seq, start, start2);
	int secs = 0;
	for (size_t i = max_seq; i < op_crcs.size(); i++)
	    secs += op_crcs[i].len;
	printf("lost %d writes (%.1f MB)\n",
	       (int)op_crcs.size() - max_seq, secs / 2.0 / 1024);
	assert(cfg->lose_writes || (n_writes - max_seq) < (int)cfg->window);
    
	for (int i = start2; i < max_seq; i++) {
	    auto [lba,len,seq,crcs] = op_crcs[i];
	    assert(seq == i+1);
	    assert(seq <= max_seq);
	    int j = 0;
	    for (auto c : *crcs) {
		should_be_crcs[lba+j] = (triple){lba+j,seq,c};
		j++;
	    }
	}
	int failed = 0;
	for (int i = 0; i < cfg->image_sectors && failed < 100; i++)
	    if (should_be_crcs[i].crc != image_info[i].crc) {
		printf("%d : %08x (seq %d) should be: %08x (%d)\n", i,
		       image_info[i].crc, image_info[i].seq,
		       should_be_crcs[i].crc, should_be_crcs[i].seq);
		failed++;
	    }
	if (failed) {
	    check_rcache(cfg);
	    assert(false);
	}
	printf("ok\n");

	__lsvd_dbg_no_gc = false;
	
	if (!cfg->restart) {
	    restore_wcache_state(cfg);
	    restore_super(cfg);
	    for (int i = 1; i < 64; i++) {
		char name[128];
		sprintf(name, "%s.%08x", cfg->obj_prefix, last_obj + i);
		if (unlink(name) >= 0)
		    printf("removed %s\n", name);
	    }
	    // works fine if I do this: (or don't delete objects above)
	    // well, almost fine.
	    //rbd_open(io, cfg->obj_prefix, &img, NULL);
	    //rbd_close(img);
	}

	free(buf);
	__lsvd_dbg_be_delay = tmp;
    }
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
    {"keep",     'k', 0,      0, "keep data between tests"},
    {"verbose",  'v', 0,      0, "print LBAs and CRCs"},
    {"reverse",  'R', 0,      0, "reverse NVMe completion order"},
    {"existing", 'x', 0,      0, "don't delete existing cache"},
    {"delay",    'D', 0,      0, "add random backend delays"},
    {"lose-objs",'o', "N",    0, "delete some of last N objects"},
    {"wipe-cache",'W', 0,     0, "delete cache on restart"},
    {"lose-writes",'L', "N",    0, "delete some of last N cache writes"},
    {"cache-size",'Z', "N",    0, "cache size (K/M/G)"},
    {"no-wipe",  'n', 0,      0, "don't clear image between runs"},
    {"sleep",    'S', 0,      0, "child sleeps for debug attach"},
    {"seed2",    '2', 0,      0, "seed-generating seed"},
    {0},
};

struct cfg _cfg = {
    "/tmp",			// cache_dir
    "200M",			// cache_size
    "/tmp/bkt/obj",		// obj_prefix
    10000, 			// run_len
    16,				// window
    1024*1024*2,		// image_sectors,
    0.0,			// read_fraction
    1,				// n_runs
    {},				// seeds
    true,			// restart
    false,			// verbose
    false,			// existing
    0,				// lose_objs
    false,			// wipe_cache
    0,				// lose_writes
    true,			// wipe_data
    false,			// sleep
    0};				// seed2

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
    case 'o':
	_cfg.lose_objs = atoi(arg);
	break;
    case 'W':
	_cfg.wipe_cache = true;
	break;
    case 'L':
	_cfg.lose_writes = atoi(arg);
	break;
    case 'Z':
	_cfg.cache_size = arg;
	break;
    case 'n':
	_cfg.wipe_data = false;
	break;
    case 'S':
	_cfg.sleep = true;
	break;
    case '2':
	_cfg.seed2 = strtoul(arg, NULL, 0);
	break;
    case ARGP_KEY_END:
        break;
    }
    return 0;
}
static struct argp argp = { options, parse_opt, NULL, args_doc};

int main(int argc, char **argv) {
    argp_parse (&argp, argc, argv, 0, 0, 0);
    __lsvd_dbg_rename = true;
    
    if (_cfg.seeds.size() == 0) {
	long seed = _cfg.seed2;
	if (seed == 0) {
	    auto now = std::chrono::system_clock::now();
	    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
	    auto value = now_ms.time_since_epoch();
	    seed = value.count();
	    printf("seed2: 0x%lx\n", seed);
	}

	rng.seed(seed);
	for (int i = 0; i < _cfg.n_runs; i++)
	    _cfg.seeds.push_back(rng());
    }

    run_test(&_cfg);
}
