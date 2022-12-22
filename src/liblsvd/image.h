/*
 * file:        image.h
 * description: the fake RBD image. 
 *
 * TODO: fix up lsvd_debug.cc and move this into lsvd.cc
 */

#ifndef __IMAGE_H__
#define __IMAGE_H__

struct event_socket {
    int socket;
    int type;
public:
    event_socket(): socket(-1), type(0) {}
    bool is_valid() const { return socket != -1; }
    int init(int fd, int t) {
	socket = fd;
	type = t;
	return 0;
    }
    int notify() {
	int rv;
	switch (type) {
	case EVENT_TYPE_PIPE:
	{
	    char buf[1] = {'i'}; // why 'i'???
	    rv = write(socket, buf, 1);
	    rv = (rv < 0) ? -errno : 0;
	    break;
	}
	case EVENT_TYPE_EVENTFD:
	{
	    uint64_t value = 1;
	    rv = write(socket, &value, sizeof (value));
	    rv = (rv < 0) ? -errno : 0;
	    break;
	}
	default:
	    rv = -1;
	}
	return rv;
    }
};

struct rbd_image {
    lsvd_config  cfg;
    ssize_t      size;          // bytes

    std::shared_mutex map_lock;
    extmap::objmap    map;

    backend     *objstore;
    translate   *xlate;
    write_cache *wcache;
    read_cache  *rcache;
    int          fd;            /* cache file */
    
    std::mutex   m;             /* protects completions */
    event_socket ev;
    std::queue<rbd_completion_t> completions;

    int          refcount = 0;

    std::thread  dbg;
    bool         done = false;

    rbd_image() {}
    ~rbd_image() {}

    int image_open(rados_ioctx_t io, const char *name);
    int image_close(void);
    int poll_io_events(rbd_completion_t *comps, int numcomp);
    void notify(rbd_completion_t c);
};

#endif
