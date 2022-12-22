/*
 * file:        file_backend.cc
 * description: local filesystem-based object backend
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <libaio.h>
#include <errno.h>
#include <sys/stat.h>

#include <map>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <random>
#include <algorithm>

#include "lsvd_types.h"
#include "backend.h"
#include "request.h"
#include "smartiov.h"
#include "io.h"
#include "misc_cache.h"
#include "objects.h"

extern void do_log(const char *, ...);

bool __lsvd_dbg_be_delay = false;
long __lsvd_dbg_be_seed = 1;
int __lsvd_dbg_be_threads = 10;
int __lsvd_dbg_be_delay_ms = 5;
bool __lsvd_dbg_rename = false;
static std::mt19937_64 rng;

class file_backend_req;

void verify_obj(iovec *iov, int iovcnt) {
    auto h = (obj_hdr*)iov[0].iov_base;
    if (h->type != LSVD_DATA)
	return;
    auto dh = (obj_data_hdr*)(h+1);
    auto map = (data_map*)((char*)h + dh->data_map_offset);
    int n_map = dh->data_map_len / sizeof(data_map);
    uint32_t sum = 0;
    for (int i = 0; i < n_map; i++)
	sum += map[i].len;
    assert(sum == h->data_sectors);
}

class file_backend : public backend {
    void worker(thread_pool<file_backend_req*> *p);
    std::mutex m;
    
public:
    file_backend(const char *prefix);
    ~file_backend();

    /* see backend.h 
     */
    int write_object(const char *name, iovec *iov, int iovcnt);
    int write_object(const char *name, char *buf, size_t len);
    int read_object(const char *name, iovec *iov, int iovcnt, size_t offset);
    int read_object(const char *name, char *buf, size_t len, size_t offset);
    int delete_object(const char *name);
    request *delete_object_req(const char *name);
    
    /* async I/O
     */
    request *make_write_req(const char *name, iovec *iov, int iovcnt);
    request *make_write_req(const char *name, char *buf, size_t len);
    request *make_read_req(const char *name, size_t offset,
                           iovec *iov, int iovcnt);
    request *make_read_req(const char *name, size_t offset,
                           char *buf, size_t len);
    thread_pool<file_backend_req*> workers;
};

#include <zlib.h>
/* trivial methods 
 */
int file_backend::write_object(const char *name, iovec *iov, int iovcnt) {
    int fd;
    if ((fd = open(name, O_RDWR | O_CREAT | O_TRUNC, 0777)) < 0)
	return -1;
    auto val = writev(fd, iov, iovcnt);
    close(fd);
    return val < 0 ? -1 : val;
}

int file_backend::write_object(const char *name, char *buf, size_t len) {
    iovec iov = {buf, len};
    return write_object(name, &iov, 1);
}

int file_backend::read_object(const char *name, iovec *iov, int iovcnt,
				  size_t offset) {
    int fd;
    if ((fd = open(name, O_RDONLY)) < 0)
       return -1;
    if (fd < 0)
       return -1;
    auto val = preadv(fd, iov, iovcnt, offset);
    close(fd);
    return val < 0 ? -1 : val;
}

int file_backend::read_object(const char *name, char *buf, size_t len,
			      size_t offset) {
    iovec iov = {buf, len};
    return read_object(name, &iov, 1, offset);
}

int file_backend::delete_object(const char *name) {
    int rv;
    if (__lsvd_dbg_rename) {
	std::string newname = std::string(name) + ".bak";
	rv = rename(name, newname.c_str());
    }
    else
	rv = unlink(name);
    return rv < 0 ? -1 : 0;
}

/* this only gets used for deleting images
 * the async nature is obviously fraudulent
 */
class f_delete_req : public request {
    int rv;
public:
    f_delete_req(const char *name) { rv = unlink(name); }
    ~f_delete_req() {}
    void wait() { delete this; }
    void run(request *parent) {}
    void notify(request *child) {}
    void release() { }
};

request *file_backend::delete_object_req(const char *name) {
    return (request*)new f_delete_req(name);
}

void trim_partial(const char *_prefix) {
    auto prefix = std::string(_prefix);
    auto stem = fs::path(prefix).filename();
    auto parent = fs::path(prefix).parent_path();
    size_t stem_len = strlen(stem.c_str());
    int rv;
    
    for (auto const& dir_entry : fs::directory_iterator{parent}) {
	std::string entry{dir_entry.path().filename()};
	if (strncmp(entry.c_str(), stem.c_str(), stem_len) == 0 &&
	    entry.size() == stem_len + 9) {
	    char buf[512];
	    auto h = (obj_hdr *)buf;
	    int fd = open(dir_entry.path().c_str(), O_RDONLY);
	    struct stat sb;
	    if (fstat(fd, &sb) < 0) {
		close(fd);
		continue;
	    }
	    rv = read(fd, buf, sizeof(buf));
	    if (rv < 512 || h->magic != LSVD_MAGIC ||
		(h->hdr_sectors + h->data_sectors)*512 != sb.st_size) {
		printf("deleting partial object: %s (%d vs %d)\n",
		       dir_entry.path().c_str(), (int)sb.st_size,
		       (int)(h->hdr_sectors + h->data_sectors));
		rename(dir_entry.path().c_str(),
		       (std::string(dir_entry.path()) + ".bak").c_str());
	    }
	    close(fd);
	}
    }
}

int iov_sum(iovec *iov, int niov) {
    int sum = 0;
    for (int i = 0; i < niov; i++)
	sum += iov[i].iov_len;
    return sum;
}

class file_backend_req : public request {
    enum lsvd_op    op;
    smartiov        _iovs;
    size_t          offset;
    char            name[64];
    request        *parent = NULL;
    file_backend   *be;
    int             fd = -1;
    int             retval;
    
public:
    file_backend_req(enum lsvd_op op_, const char *name_,
		     iovec *iov, int iovcnt, size_t offset_,
		     file_backend *be_) : _iovs(iov, iovcnt) {
	op = op_;
	offset = offset_;
	be = be_;
	strcpy(name, name_);
    }
    file_backend_req(enum lsvd_op op_, const char *name_,
		     char *buf, size_t len, size_t offset_,
		     file_backend *be_) {
	op = op_;
	offset = offset_;
	be = be_;
	strcpy(name, name_);
	iovec iov = {buf, len};
	_iovs.push_back(iov);
    }
    ~file_backend_req() {}

    void      wait() {}		   // TODO: ?????
    void      run(request *parent);
    void      notify(request *child);
    void      release() {};

    void exec(void) {
	auto [iov,niovs] = _iovs.c_iov();
	if (op == OP_READ) {
	    if ((fd = open(name, O_RDONLY)) < 0)
		retval = fd;
	    else {
		retval = preadv(fd, iov, niovs, offset);
		close(fd);
	    }
	}
	else {
	    if ((fd = open(name, O_RDWR | O_CREAT | O_TRUNC, 0777)) < 0)
		retval = fd;
	    else {
		retval = pwritev(fd, iov, niovs, offset);
		close(fd);
	    }
	}
	notify(NULL);
    }
};


/* methods that depend on file_backend_req
 */
file_backend::file_backend(const char *prefix) : workers(&m) {
    if (prefix)
	trim_partial(prefix);
    rng.seed(__lsvd_dbg_be_seed);
    for (int i = 0; i < __lsvd_dbg_be_threads; i++) 
	workers.pool.push(std::thread(&file_backend::worker, this, &workers));
}

file_backend::~file_backend() {
}

void file_backend::worker(thread_pool<file_backend_req*> *p) {
    pthread_setname_np(pthread_self(), "file_worker");
    while (p->running) {
	file_backend_req *req;
	if (!p->get(req)) 
	    return;
	if (__lsvd_dbg_be_delay) {
	    std::uniform_int_distribution<int> uni(0,__lsvd_dbg_be_delay_ms*1000);
	    useconds_t usecs = uni(rng);
	    usleep(usecs);
	}
	req->exec();
    }
}

/* TODO: run() ought to return error/success
 */
void file_backend_req::run(request *parent_) {
    parent = parent_;
    be->workers.put(this);
}

/* TODO: this assumes no use of wait/release
 */
void file_backend_req::notify(request *unused) {
    parent->notify(this);
    delete this;
}

request *file_backend::make_write_req(const char*name, iovec *iov, int niov) {
    assert(access(name, F_OK) != 0);
    verify_obj(iov, niov);
    return new file_backend_req(OP_WRITE, name, iov, niov, 0, this);
}
request *file_backend::make_write_req(const char *name, char *buf, size_t len) {
    assert(access(name, F_OK) != 0);
    return new file_backend_req(OP_WRITE, name, buf, len, 0, this);
}
    


request *file_backend::make_read_req(const char *name, size_t offset,
				     iovec *iov, int iovcnt) {
    return new file_backend_req(OP_READ, name, iov, iovcnt, offset, this);
}

request *file_backend::make_read_req(const char *name, size_t offset,
				     char *buf, size_t len) {
    iovec iov = {buf, len};
    return new file_backend_req(OP_READ, name, &iov, 1, offset, this);
}

backend *make_file_backend(const char *prefix) {
    return new file_backend(prefix);
}
