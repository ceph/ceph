#include <stdio.h>
#include <stdlib.h>
#include <argp.h>
#include <string>
#include <uuid/uuid.h>
#include <fcntl.h>

#include "fake_rbd.h"
#include "config.h"

struct backend;

extern backend *get_backend(lsvd_config *cfg, rados_ioctx_t io, const char *name);
extern int translate_get_uuid(backend *, const char *, uuid_t &);



const char *backend = "file";
const char *image_name;
const char *cache_dir;
const char *cache_dev;
enum _op {OP_CREATE = 1, OP_DELETE = 2, OP_INFO = 3, OP_MKCACHE};
enum _op op;
size_t size = 0;

static long parseint(const char *_s)
{
    char *s = (char*)_s;
    long val = strtol(s, &s, 0);
    if (toupper(*s) == 'G')
        val *= (1024*1024*1024);
    if (toupper(*s) == 'M')
        val *= (1024*1024);
    if (toupper(*s) == 'K')
        val *= 1024;
    return val;
}

static struct argp_option options[] = {
    {"cache-dir",'d', "DIR",  0, "cache directory"},
    {"rados",    'O', 0,      0, "use RADOS"},
    {"create",   'C', 0,      0, "create image"},
    {"mkcache",  'k', "DEV",  0, "use DEV as cache"},
    {"size",     'z', "SIZE", 0, "size in bytes (M/G=2^20,2^30)"},
    {"delete",   'D', 0,      0, "delete image"},
    {"info",     'I', 0,      0, "show image information"},
    {0},
};

static char args_doc[] = "IMAGE";

static error_t parse_opt(int key, char *arg, struct argp_state *state)
{
    switch (key) {
    case ARGP_KEY_ARG:
        image_name = arg;
        break;
    case 'd':
	cache_dir = arg;
	break;
    case 'O':
	backend = "rados";
        break;
    case 'C':
	op = OP_CREATE;
        break;
    case 'z':
	size = parseint(arg);
	break;
    case 'D':
	op = OP_DELETE;
        break;
    case 'I':
	op = OP_INFO;
        break;
    case 'k':
	op = OP_MKCACHE;
	cache_dev = arg;
	break;
    case ARGP_KEY_END:
	if (op == 0 || (op == OP_CREATE && size == 0))
	    argp_usage(state);
        break;
    }
    return 0;
}

static struct argp argp = { options, parse_opt, NULL, args_doc};

void info(rados_ioctx_t io, const char *image_name) {
    lsvd_config  cfg;
    int rv;
    if ((rv = cfg.read()) < 0) {
	printf("error reading config: %d\n", rv);
	exit(1);
    }
    auto objstore = get_backend(&cfg, io, NULL);
    uuid_t uu;
    if ((rv = translate_get_uuid(objstore, image_name, uu)) < 0) {
	printf("error reading superblock: %d\n", rv);
	exit(1);
    }
    auto cache_file = cfg.cache_filename(uu, image_name);
    printf("image: %s\n", image_name);
    printf("cache: %s\n", cache_file.c_str());
}

extern int make_cache(int fd, uuid_t &uuid, int n_pages);
extern size_t getsize64(int fd);

void mk_cache(rados_ioctx_t io, const char *image_name, const char *dev_name) {
    int rv, fd = open(dev_name, O_RDWR);
    if (fd < 0) {
	perror("device file open");
	exit(1);
    }
    auto sz = getsize64(fd);

    lsvd_config  cfg;
    if ((rv = cfg.read()) < 0) {
	printf("error reading config: %d\n", rv);
	exit(1);
    }
    auto objstore = get_backend(&cfg, io, NULL);
    uuid_t uu;
    if ((rv = translate_get_uuid(objstore, image_name, uu)) < 0) {
	printf("error reading superblock: %d\n", rv);
	exit(1);
    }
    auto cache_file = cfg.cache_filename(uu, image_name);

    auto n_pages = sz / 4096;
    if (make_cache(fd, uu, n_pages) < 0) {
	printf("make_cache failed\n");
	exit(1);
    }
    if ((rv = symlink(dev_name, cache_file.c_str())) < 0) {
	perror("symbolic link");
	exit(1);
    }
    close(fd);
}

int main(int argc, char **argv) {
    argp_parse (&argp, argc, argv, 0, 0, 0);

    setenv("LSVD_BACKEND", backend, 1);
    if (cache_dir != NULL)
	setenv("LSVD_CACHE_DIR", cache_dir, 1);
    rados_ioctx_t io = 0;
    if (op == OP_CREATE && size > 0)
	rbd_create(io, image_name, size, NULL);
    else if (op == OP_DELETE)
	rbd_remove(io, image_name);
    else if (op == OP_INFO)
	info(io, image_name);
    else if (op == OP_MKCACHE)
	mk_cache(io, image_name, cache_dev);
}
