// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "armor.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/simple_spin.h"
#include "common/strtol.h"
#include "common/likely.h"
#include "include/atomic.h"
#include "common/RWLock.h"
#include "include/types.h"
#include "include/compat.h"
#if defined(HAVE_XIO)
#include "msg/xio/XioMsg.h"
#endif

#include <errno.h>
#include <fstream>
#include <sstream>
#include <sys/uio.h>
#include <limits.h>

#include <ostream>
namespace ceph {

// number of buffer::ptr's to embed in buffer::raw.
#define RAW_NUM_PTRS 3

#ifdef BUFFER_DEBUG
static simple_spinlock_t buffer_debug_lock = SIMPLE_SPINLOCK_INITIALIZER;
# define bdout { simple_spin_lock(&buffer_debug_lock); std::cout
# define bendl std::endl; simple_spin_unlock(&buffer_debug_lock); }
#else
# define bdout if (0) { std::cout
# define bendl std::endl; }
#endif

  static atomic_t buffer_total_alloc;
  const bool buffer_track_alloc = get_env_bool("CEPH_BUFFER_TRACK");

  void buffer::inc_total_alloc(unsigned len) {
    if (buffer_track_alloc)
      buffer_total_alloc.add(len);
  }
  void buffer::dec_total_alloc(unsigned len) {
    if (buffer_track_alloc)
      buffer_total_alloc.sub(len);
  }
  int buffer::get_total_alloc() {
    return buffer_total_alloc.read();
  }

  static atomic_t buffer_cached_crc;
  static atomic_t buffer_cached_crc_adjusted;
  static bool buffer_track_crc = get_env_bool("CEPH_BUFFER_TRACK");

  void buffer::track_cached_crc(bool b) {
    buffer_track_crc = b;
  }
  int buffer::get_cached_crc() {
    return buffer_cached_crc.read();
  }
  int buffer::get_cached_crc_adjusted() {
    return buffer_cached_crc_adjusted.read();
  }

  static atomic_t buffer_c_str_accesses;
  static bool buffer_track_c_str = get_env_bool("CEPH_BUFFER_TRACK");

  void buffer::track_c_str(bool b) {
    buffer_track_c_str = b;
  }
  int buffer::get_c_str_accesses() {
    return buffer_c_str_accesses.read();
  }

  static atomic_t buffer_max_pipe_size;
  int update_max_pipe_size() {
#ifdef CEPH_HAVE_SETPIPE_SZ
    char buf[32];
    int r;
    std::string err;
    struct stat stat_result;
    if (::stat("/proc/sys/fs/pipe-max-size", &stat_result) == -1)
      return -errno;
    r = safe_read_file("/proc/sys/fs/", "pipe-max-size",
		       buf, sizeof(buf) - 1);
    if (r < 0)
      return r;
    buf[r] = '\0';
    size_t size = strict_strtol(buf, 10, &err);
    if (!err.empty())
      return -EIO;
    buffer_max_pipe_size.set(size);
#endif
    return 0;
  }

  size_t get_max_pipe_size() {
#ifdef CEPH_HAVE_SETPIPE_SZ
    size_t size = buffer_max_pipe_size.read();
    if (size)
      return size;
    if (update_max_pipe_size() == 0)
      return buffer_max_pipe_size.read();
#endif
    // this is the max size hardcoded in linux before 2.6.35
    return 65536;
  }

  buffer::error_code::error_code(int error) :
    buffer::malformed_input(cpp_strerror(error).c_str()), code(error) {}

  class buffer::raw {
  public:
    char *data;
    unsigned len;
    atomic_t nref;

    mutable RWLock crc_lock;
    map<pair<size_t, size_t>, pair<uint32_t, uint32_t> > crc_map;

    /*
     * embed a small number of ptrs inside the buffer descriptor.
     * this lets buffer::list avoid an extra allocation in most
     * cases.  see buffer::list::_get_raw_ptr().
     */
    Spinlock ptr_lock;  // for allocating ptrs only
    ptr ptrs[RAW_NUM_PTRS];

    void init_embed_ptrs() {
      for (unsigned i=0; i<RAW_NUM_PTRS; ++i)
	ptrs[i].embed = true;
    }

    // ctors
    raw(unsigned l)
      : data(NULL), len(l), nref(0),
	crc_lock("buffer::raw::crc_lock", false)
    {
      init_embed_ptrs();
    }
    raw(char *c, unsigned l)
      : data(c), len(l), nref(0),
	crc_lock("buffer::raw::crc_lock", false)
    {
      init_embed_ptrs();
    }
    virtual ~raw() {}

    // no copying.
    raw(const raw &other);
    const raw& operator=(const raw &other);

    virtual char *get_data() {
      return data;
    }
    virtual raw* clone_empty() = 0;
    raw *clone() {
      raw *c = clone_empty();
      memcpy(c->data, data, len);
      return c;
    }
    virtual bool can_zero_copy() const {
      return false;
    }
    virtual int zero_copy_to_fd(int fd, loff_t *offset) {
      return -ENOTSUP;
    }
    virtual bool is_page_aligned() {
      return ((long)data & ~CEPH_PAGE_MASK) == 0;
    }
    bool is_n_page_sized() {
      return (len & ~CEPH_PAGE_MASK) == 0;
    }
    virtual bool is_shareable() {
      // true if safe to reference/share the existing buffer copy
      // false if it is not safe to share the buffer, e.g., due to special
      // and/or registered memory that is scarce
      return true;
    }
    bool get_crc(const pair<size_t, size_t> &fromto,
         pair<uint32_t, uint32_t> *crc) const {
      crc_lock.get_read();
      map<pair<size_t, size_t>, pair<uint32_t, uint32_t> >::const_iterator i =
      crc_map.find(fromto);
      if (i == crc_map.end()) {
          crc_lock.unlock();
          return false;
      }
      *crc = i->second;
      crc_lock.unlock();
      return true;
    }
    void set_crc(const pair<size_t, size_t> &fromto,
         const pair<uint32_t, uint32_t> &crc) {
      crc_lock.get_write();
      crc_map[fromto] = crc;
      crc_lock.unlock();
    }
    void invalidate_crc() {
      // don't own the write lock when map is empty
      crc_lock.get_read();
      if (crc_map.size() != 0) {
        crc_lock.unlock();
        crc_lock.get_write();
        crc_map.clear();
      }
      crc_lock.unlock();
    }
  };

  class buffer::raw_combined : public buffer::raw {
    size_t alignment;
  public:
    raw_combined(char *dataptr, unsigned l, unsigned align=0)
      : raw(dataptr, l),
	alignment(align) {
      inc_total_alloc(len);
    }
    ~raw_combined() {
      dec_total_alloc(len);
    }
    raw* clone_empty() {
      return create_combined(len, alignment);
    }

    static void operator delete(void *ptr) {
      raw_combined *raw = (raw_combined *)ptr;
      ::free((void *)raw->data);
    }
  };

  class buffer::raw_malloc : public buffer::raw {
  public:
    raw_malloc(unsigned l) : raw(l) {
      if (len) {
	data = (char *)malloc(len);
        if (!data)
          throw bad_alloc();
      } else {
	data = 0;
      }
      inc_total_alloc(len);
      bdout << "raw_malloc " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    raw_malloc(unsigned l, char *b) : raw(b, l) {
      inc_total_alloc(len);
      bdout << "raw_malloc " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    ~raw_malloc() {
      free(data);
      dec_total_alloc(len);
      bdout << "raw_malloc " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() {
      return new raw_malloc(len);
    }
  };

#ifndef __CYGWIN__
  class buffer::raw_mmap_pages : public buffer::raw {
  public:
    raw_mmap_pages(unsigned l) : raw(l) {
      data = (char*)::mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
      if (!data)
	throw bad_alloc();
      inc_total_alloc(len);
      bdout << "raw_mmap " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    ~raw_mmap_pages() {
      ::munmap(data, len);
      dec_total_alloc(len);
      bdout << "raw_mmap " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() {
      return new raw_mmap_pages(len);
    }
  };

  class buffer::raw_posix_aligned : public buffer::raw {
    unsigned align;
  public:
    raw_posix_aligned(unsigned l, unsigned _align) : raw(l) {
      align = _align;
      assert((align >= sizeof(void *)) && (align & (align - 1)) == 0);
#ifdef DARWIN
      data = (char *) valloc (len);
#else
      data = 0;
      int r = ::posix_memalign((void**)(void*)&data, align, len);
      if (r)
	throw bad_alloc();
#endif /* DARWIN */
      if (!data)
	throw bad_alloc();
      inc_total_alloc(len);
      bdout << "raw_posix_aligned " << this << " alloc " << (void *)data << " l=" << l << ", align=" << align << " total_alloc=" << buffer::get_total_alloc() << bendl;
    }
    ~raw_posix_aligned() {
      ::free((void*)data);
      dec_total_alloc(len);
      bdout << "raw_posix_aligned " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() {
      return new raw_posix_aligned(len, align);
    }
  };
#endif

#ifdef __CYGWIN__
  class buffer::raw_hack_aligned : public buffer::raw {
    unsigned align;
    char *realdata;
  public:
    raw_hack_aligned(unsigned l, unsigned _align) : raw(l) {
      align = _align;
      realdata = new char[len+align-1];
      unsigned off = ((unsigned)realdata) & (align-1);
      if (off)
	data = realdata + align - off;
      else
	data = realdata;
      inc_total_alloc(len+align-1);
      //cout << "hack aligned " << (unsigned)data
      //<< " in raw " << (unsigned)realdata
      //<< " off " << off << std::endl;
      assert(((unsigned)data & (align-1)) == 0);
    }
    ~raw_hack_aligned() {
      delete[] realdata;
      dec_total_alloc(len+align-1);
    }
    raw* clone_empty() {
      return new raw_hack_aligned(len, align);
    }
  };
#endif

#ifdef CEPH_HAVE_SPLICE
  class buffer::raw_pipe : public buffer::raw {
  public:
    raw_pipe(unsigned len) : raw(len), source_consumed(false) {
      size_t max = get_max_pipe_size();
      if (len > max) {
	bdout << "raw_pipe: requested length " << len
	      << " > max length " << max << bendl;
	throw malformed_input("length larger than max pipe size");
      }
      pipefds[0] = -1;
      pipefds[1] = -1;

      int r;
      if (::pipe(pipefds) == -1) {
	r = -errno;
	bdout << "raw_pipe: error creating pipe: " << cpp_strerror(r) << bendl;
	throw error_code(r);
      }

      r = set_nonblocking(pipefds);
      if (r < 0) {
	bdout << "raw_pipe: error setting nonblocking flag on temp pipe: "
	      << cpp_strerror(r) << bendl;
	throw error_code(r);
      }

      r = set_pipe_size(pipefds, len);
      if (r < 0) {
	bdout << "raw_pipe: could not set pipe size" << bendl;
	// continue, since the pipe should become large enough as needed
      }

      inc_total_alloc(len);
      bdout << "raw_pipe " << this << " alloc " << len << " "
	    << buffer::get_total_alloc() << bendl;
    }

    ~raw_pipe() {
      if (data)
	free(data);
      close_pipe(pipefds);
      dec_total_alloc(len);
      bdout << "raw_pipe " << this << " free " << (void *)data << " "
	    << buffer::get_total_alloc() << bendl;
    }

    bool can_zero_copy() const {
      return true;
    }

    int set_source(int fd, loff_t *off) {
      int flags = SPLICE_F_NONBLOCK;
      ssize_t r = safe_splice(fd, off, pipefds[1], NULL, len, flags);
      if (r < 0) {
	bdout << "raw_pipe: error splicing into pipe: " << cpp_strerror(r)
	      << bendl;
	return r;
      }
      // update length with actual amount read
      len = r;
      return 0;
    }

    int zero_copy_to_fd(int fd, loff_t *offset) {
      assert(!source_consumed);
      int flags = SPLICE_F_NONBLOCK;
      ssize_t r = safe_splice_exact(pipefds[0], NULL, fd, offset, len, flags);
      if (r < 0) {
	bdout << "raw_pipe: error splicing from pipe to fd: "
	      << cpp_strerror(r) << bendl;
	return r;
      }
      source_consumed = true;
      return 0;
    }

    buffer::raw* clone_empty() {
      // cloning doesn't make sense for pipe-based buffers,
      // and is only used by unit tests for other types of buffers
      return NULL;
    }

    char *get_data() {
      if (data)
	return data;
      return copy_pipe(pipefds);
    }

  private:
    int set_pipe_size(int *fds, long length) {
#ifdef CEPH_HAVE_SETPIPE_SZ
      if (::fcntl(fds[1], F_SETPIPE_SZ, length) == -1) {
	int r = -errno;
	if (r == -EPERM) {
	  // pipe limit must have changed - EPERM means we requested
	  // more than the maximum size as an unprivileged user
	  update_max_pipe_size();
	  throw malformed_input("length larger than new max pipe size");
	}
	return r;
      }
#endif
      return 0;
    }

    int set_nonblocking(int *fds) {
      if (::fcntl(fds[0], F_SETFL, O_NONBLOCK) == -1)
	return -errno;
      if (::fcntl(fds[1], F_SETFL, O_NONBLOCK) == -1)
	return -errno;
      return 0;
    }

    void close_pipe(int *fds) {
      if (fds[0] >= 0)
	VOID_TEMP_FAILURE_RETRY(::close(fds[0]));
      if (fds[1] >= 0)
	VOID_TEMP_FAILURE_RETRY(::close(fds[1]));
    }
    char *copy_pipe(int *fds) {
      /* preserve original pipe contents by copying into a temporary
       * pipe before reading.
       */
      int tmpfd[2];
      int r;

      assert(!source_consumed);
      assert(fds[0] >= 0);

      if (::pipe(tmpfd) == -1) {
	r = -errno;
	bdout << "raw_pipe: error creating temp pipe: " << cpp_strerror(r)
	      << bendl;
	throw error_code(r);
      }
      r = set_nonblocking(tmpfd);
      if (r < 0) {
	bdout << "raw_pipe: error setting nonblocking flag on temp pipe: "
	      << cpp_strerror(r) << bendl;
	throw error_code(r);
      }
      r = set_pipe_size(tmpfd, len);
      if (r < 0) {
	bdout << "raw_pipe: error setting pipe size on temp pipe: "
	      << cpp_strerror(r) << bendl;
      }
      int flags = SPLICE_F_NONBLOCK;
      if (::tee(fds[0], tmpfd[1], len, flags) == -1) {
	r = errno;
	bdout << "raw_pipe: error tee'ing into temp pipe: " << cpp_strerror(r)
	      << bendl;
	close_pipe(tmpfd);
	throw error_code(r);
      }
      data = (char *)malloc(len);
      if (!data) {
	close_pipe(tmpfd);
	throw bad_alloc();
      }
      r = safe_read(tmpfd[0], data, len);
      if (r < (ssize_t)len) {
	bdout << "raw_pipe: error reading from temp pipe:" << cpp_strerror(r)
	      << bendl;
	free(data);
	data = NULL;
	close_pipe(tmpfd);
	throw error_code(r);
      }
      close_pipe(tmpfd);
      return data;
    }
    bool source_consumed;
    int pipefds[2];
  };
#endif // CEPH_HAVE_SPLICE

  /*
   * primitive buffer types
   */
  class buffer::raw_char : public buffer::raw {
  public:
    raw_char(unsigned l) : raw(l) {
      if (len)
	data = new char[len];
      else
	data = 0;
      inc_total_alloc(len);
      bdout << "raw_char " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    raw_char(unsigned l, char *b) : raw(b, l) {
      inc_total_alloc(len);
      bdout << "raw_char " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    ~raw_char() {
      delete[] data;
      dec_total_alloc(len);
      bdout << "raw_char " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() {
      return new raw_char(len);
    }
  };

  class buffer::raw_unshareable : public buffer::raw {
  public:
    raw_unshareable(unsigned l) : raw(l) {
      if (len)
	data = new char[len];
      else
	data = 0;
    }
    raw_unshareable(unsigned l, char *b) : raw(b, l) {
    }
    raw* clone_empty() {
      return buffer::create(len);
    }
    bool is_shareable() {
      return false; // !shareable, will force make_shareable()
    }
    ~raw_unshareable() {
      delete[] data;
    }
  };

  class buffer::raw_static : public buffer::raw {
  public:
    raw_static(const char *d, unsigned l) : raw((char*)d, l) { }
    ~raw_static() {}
    raw* clone_empty() {
      return buffer::create(len);
    }
  };

#if defined(HAVE_XIO)
  class buffer::xio_msg_buffer : public buffer::raw {
  private:
    XioDispatchHook* m_hook;
  public:
    xio_msg_buffer(XioDispatchHook* _m_hook, const char *d,
	unsigned l) :
      raw((char*)d, l), m_hook(_m_hook->get()) {}

    bool is_shareable() { return false; }
    static void operator delete(void *p)
    {
      xio_msg_buffer *buf = static_cast<xio_msg_buffer*>(p);
      // return hook ref (counts against pool);  it appears illegal
      // to do this in our dtor, because this fires after that
      buf->m_hook->put();
    }
    raw* clone_empty() {
      return new buffer::raw_char(len);
    }
  };

  class buffer::xio_mempool : public buffer::raw {
  public:
    struct xio_reg_mem *mp;
    xio_mempool(struct xio_reg_mem *_mp, unsigned l) :
      raw((char*)mp->addr, l), mp(_mp)
    { }
    ~xio_mempool() {}
    raw* clone_empty() {
      return buffer::create(len);
    }
  };

  struct xio_reg_mem* get_xio_mp(const buffer::ptr& bp)
  {
    buffer::xio_mempool *mb = dynamic_cast<buffer::xio_mempool*>(bp.get_raw());
    if (mb) {
      return mb->mp;
    }
    return NULL;
  }

  buffer::raw* buffer::create_msg(
      unsigned len, char *buf, XioDispatchHook* m_hook) {
    XioPool& pool = m_hook->get_pool();
    buffer::raw* bp =
      static_cast<buffer::raw*>(pool.alloc(sizeof(xio_msg_buffer)));
    new (bp) xio_msg_buffer(m_hook, buf, len);
    return bp;
  }
#endif /* HAVE_XIO */

  buffer::raw* buffer::copy(const char *c, unsigned len) {
    raw* r = buffer::create(len);
    memcpy(r->data, c, len);
    return r;
  }

  buffer::raw* buffer::create(unsigned len) {
    return buffer::create_combined(len, sizeof(size_t));
  }
  buffer::raw* buffer::claim_char(unsigned len, char *buf) {
    return new raw_char(len, buf);
  }
  buffer::raw* buffer::create_malloc(unsigned len) {
    return new raw_malloc(len);
  }
  buffer::raw* buffer::claim_malloc(unsigned len, char *buf) {
    return new raw_malloc(len, buf);
  }
  buffer::raw* buffer::create_static(unsigned len, char *buf) {
    return new raw_static(buf, len);
  }

  buffer::raw* buffer::create_aligned(unsigned len, unsigned align) {
#ifndef __CYGWIN__
    //return new raw_mmap_pages(len);
    //return new raw_posix_aligned(len, align);
    return create_combined(len, align);
#else
    return new raw_hack_aligned(len, align);
#endif
  }

  buffer::raw* buffer::create_combined(unsigned len, unsigned align) {
    assert(align);
    size_t rawlen = ROUND_UP_TO(sizeof(buffer::raw_combined), sizeof(size_t));
    size_t datalen = ROUND_UP_TO(len, sizeof(size_t));

#ifdef DARWIN
    char *ptr = (char *) valloc(rawlen + datalen);
#else
    char *ptr = 0;
    int r = ::posix_memalign((void**)(void*)&ptr, align, rawlen + datalen);
    if (r)
      throw bad_alloc();
#endif /* DARWIN */
    if (!ptr)
      throw bad_alloc();

    // actual data first, since it has presumably larger alignment restriction
    // then put the raw_combined at the end
    raw *ret = new (ptr + datalen) raw_combined(ptr, len, align);
    assert((char *)ret == ptr + datalen);
    return ret;
  }

  buffer::raw* buffer::create_page_aligned(unsigned len) {
    return create_aligned(len, CEPH_PAGE_SIZE);
  }

  buffer::raw* buffer::create_zero_copy(unsigned len, int fd, int64_t *offset) {
#ifdef CEPH_HAVE_SPLICE
    buffer::raw_pipe* buf = new raw_pipe(len);
    int r = buf->set_source(fd, (loff_t*)offset);
    if (r < 0) {
      delete buf;
      throw error_code(r);
    }
    return buf;
#else
    throw error_code(-ENOTSUP);
#endif
  }

  buffer::raw* buffer::create_unshareable(unsigned len) {
    return new raw_unshareable(len);
  }

  // no lock needed; this is an unref raw.
  buffer::ptr::ptr(raw *r) : _raw(r), _off(0), _len(r->len), embed(false)
  {
    r->nref.inc();
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(raw *r, unsigned o, unsigned l)
    : _raw(r), _off(o), _len(l), embed(false)
  {
    r->nref.inc();
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(unsigned l) : _off(0), _len(l), embed(false)
  {
    _raw = create(l);
    _raw->nref.inc();
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(const char *d, unsigned l) : _off(0), _len(l), embed(false)
  {
    _raw = copy(d, l);
    _raw->nref.inc();
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(const ptr& p)
    : _raw(p._raw), _off(p._off), _len(p._len), embed(false)
  {
    if (_raw) {
      _raw->nref.inc();
      bdout << "ptr " << this << " get " << _raw << bendl;
    }
  }
  buffer::ptr::ptr(const ptr& p, unsigned o, unsigned l)
    : _raw(p._raw), _off(p._off + o), _len(l), embed(false)
  {
    assert(o+l <= p._len);
    assert(_raw);
    _raw->nref.inc();
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr& buffer::ptr::operator= (const ptr& p)
  {
    if (p._raw) {
      p._raw->nref.inc();
      bdout << "ptr " << this << " get " << _raw << bendl;
    }
    buffer::raw *raw = p._raw; 
    release();
    if (raw) {
      _raw = raw;
      _off = p._off;
      _len = p._len;
    } else {
      _off = _len = 0;
    }
    return *this;
  }

  void buffer::ptr::reset(raw *raw, unsigned off, unsigned len) {
    bdout << "ptr::reset raw " << raw << " " << off << "~" << len << bendl;
    _raw = raw;
    _off = off;
    _len = len;
    _raw->nref.inc();
  }

  buffer::raw *buffer::ptr::clone()
  {
    return _raw->clone();
  }

  buffer::ptr& buffer::ptr::make_shareable() {
    if (_raw && !_raw->is_shareable()) {
      buffer::raw *tr = _raw;
      _raw = tr->clone();
      _raw->nref.set(1);
      if (unlikely(tr->nref.dec() == 0)) {
        delete tr;
      }
    }
    return *this;
  }

  void buffer::ptr::swap(ptr& other)
  {
    raw *r = _raw;
    unsigned o = _off;
    unsigned l = _len;
    _raw = other._raw;
    _off = other._off;
    _len = other._len;
    other._raw = r;
    other._off = o;
    other._len = l;
  }

  void buffer::ptr::release()
  {
    if (_raw) {
      bdout << "ptr " << this << " release " << _raw << bendl;
      raw *t = _raw;
      _raw = 0;
      if (t->nref.dec() == 0) {
	//cout << "hosing raw " << (void*)t << " len " << t->len << std::endl;
	delete t;  // dealloc old (if any)
      }
    }
  }

  bool buffer::ptr::at_buffer_tail() const { return _off + _len == _raw->len; }

  const char *buffer::ptr::c_str() const {
    assert(_raw);
    if (buffer_track_c_str)
      buffer_c_str_accesses.inc();
    return _raw->get_data() + _off;
  }
  char *buffer::ptr::c_str() {
    assert(_raw);
    if (buffer_track_c_str)
      buffer_c_str_accesses.inc();
    return _raw->get_data() + _off;
  }

  unsigned buffer::ptr::unused_tail_length() const
  {
    if (_raw)
      return _raw->len - (_off+_len);
    else
      return 0;
  }
  const char& buffer::ptr::operator[](unsigned n) const
  {
    assert(_raw);
    assert(n < _len);
    return _raw->get_data()[_off + n];
  }
  char& buffer::ptr::operator[](unsigned n)
  {
    assert(_raw);
    assert(n < _len);
    return _raw->get_data()[_off + n];
  }

  const char *buffer::ptr::raw_c_str() const { assert(_raw); return _raw->data; }
  unsigned buffer::ptr::raw_length() const { assert(_raw); return _raw->len; }
  int buffer::ptr::raw_nref() const { assert(_raw); return _raw->nref.read(); }

  unsigned buffer::ptr::wasted()
  {
    assert(_raw);
    return _raw->len - _len;
  }

  int buffer::ptr::cmp(const ptr& o) const
  {
    int l = _len < o._len ? _len : o._len;
    if (l) {
      int r = memcmp(c_str(), o.c_str(), l);
      if (r)
	return r;
    }
    if (_len < o._len)
      return -1;
    if (_len > o._len)
      return 1;
    return 0;
  }

  bool buffer::ptr::is_zero() const
  {
    const char *data = c_str();
    for (size_t p = 0; p < _len; p++) {
      if (data[p] != 0) {
	return false;
      }
    }
    return true;
  }

  void buffer::ptr::append(char c)
  {
    assert(_raw);
    assert(1 <= unused_tail_length());
    (c_str())[_len] = c;
    _len++;
  }
  
  void buffer::ptr::append(const char *p, unsigned l)
  {
    assert(_raw);
    assert(l <= unused_tail_length());
    memcpy(c_str() + _len, p, l);
    _len += l;
  }
    
  void buffer::ptr::copy_in(unsigned o, unsigned l, const char *src)
  {
    assert(_raw);
    assert(o <= _len);
    assert(o+l <= _len);
    _raw->invalidate_crc();
    memcpy(c_str()+o, src, l);
  }

  void buffer::ptr::zero()
  {
    _raw->invalidate_crc();
    memset(c_str(), 0, _len);
  }

  void buffer::ptr::zero(unsigned o, unsigned l)
  {
    assert(o+l <= _len);
    _raw->invalidate_crc();
    memset(c_str()+o, 0, l);
  }

  bool buffer::ptr::can_zero_copy() const
  {
    return _raw->can_zero_copy();
  }

  int buffer::ptr::zero_copy_to_fd(int fd, int64_t *offset) const
  {
    return _raw->zero_copy_to_fd(fd, (loff_t*)offset);
  }

  // -- buffer::list::iterator --
  /*
  buffer::list::iterator operator=(const buffer::list::iterator& other)
  {
    if (this != &other) {
      bl = other.bl;
      ls = other.ls;
      off = other.off;
      p = other.p;
      p_off = other.p_off;
    }
    return *this;
    }*/

  void buffer::list::iterator::advance(int o)
  {
    //cout << this << " advance " << o << " from " << off << " (p_off " << p_off << " in " << p->length() << ")" << std::endl;
    if (o > 0) {
      p_off += o;
      while (p_off > 0) {
	if (p == ls->end())
	  throw end_of_buffer();
	if (p_off >= p->length()) {
	  // skip this buffer
	  p_off -= p->length();
	  ++p;
	} else {
	  // somewhere in this buffer!
	  break;
	}
      }
      off += o;
      return;
    }
    while (o < 0) {
      if (p_off) {
	unsigned d = -o;
	if (d > p_off)
	  d = p_off;
	p_off -= d;
	off -= d;
	o += d;
      } else if (off > 0) {
	assert(p != ls->begin());
	--p;
	p_off = p->length();
      } else {
	throw end_of_buffer();
      }
    }
  }

  void buffer::list::iterator::seek(unsigned o)
  {
    //cout << this << " seek " << o << std::endl;
    p = ls->begin();
    off = p_off = 0;
    advance(o);
  }

  char buffer::list::iterator::operator*()
  {
    if (p == ls->end())
      throw end_of_buffer();
    return (*p)[p_off];
  }
  
  buffer::list::iterator& buffer::list::iterator::operator++()
  {
    if (p == ls->end())
      throw end_of_buffer();
    advance(1);
    return *this;
  }

  buffer::ptr buffer::list::iterator::get_current_ptr()
  {
    if (p == ls->end())
      throw end_of_buffer();
    return ptr(*p, p_off, p->length() - p_off);
  }
  
  // copy data out.
  // note that these all _append_ to dest!
  
  void buffer::list::iterator::copy(unsigned len, char *dest)
  {
    if (p == ls->end()) seek(off);
    while (len > 0) {
      if (p == ls->end())
	throw end_of_buffer();
      assert(p->length() > 0); 
      
      unsigned howmuch = p->length() - p_off;
      if (len < howmuch) howmuch = len;
      p->copy_out(p_off, howmuch, dest);
      dest += howmuch;

      len -= howmuch;
      advance(howmuch);
    }
  }
  
  void buffer::list::iterator::copy(unsigned len, ptr &dest)
  {
    dest = create(len);
    copy(len, dest.c_str());
  }

  void buffer::list::iterator::copy(unsigned len, list &dest)
  {
    if (p == ls->end())
      seek(off);
    while (len > 0) {
      if (p == ls->end())
	throw end_of_buffer();
      
      unsigned howmuch = p->length() - p_off;
      if (len < howmuch)
	howmuch = len;
      dest.append(*p, p_off, howmuch);
      
      len -= howmuch;
      advance(howmuch);
    }
  }

  void buffer::list::iterator::copy(unsigned len, std::string &dest)
  {
    if (p == ls->end())
      seek(off);
    while (len > 0) {
      if (p == ls->end())
	throw end_of_buffer();
      
      unsigned howmuch = p->length() - p_off;
      const char *c_str = p->c_str();
      if (len < howmuch)
	howmuch = len;
      dest.append(c_str + p_off, howmuch);

      len -= howmuch;
      advance(howmuch);
    }
  }

  void buffer::list::iterator::copy_all(list &dest)
  {
    if (p == ls->end())
      seek(off);
    while (1) {
      if (p == ls->end())
	return;
      assert(p->length() > 0);
      
      unsigned howmuch = p->length() - p_off;
      const char *c_str = p->c_str();
      dest.append(c_str + p_off, howmuch);
      
      advance(howmuch);
    }
  }
  
  // copy data in

  void buffer::list::iterator::copy_in(unsigned len, const char *src)
  {
    // copy
    if (p == ls->end())
      seek(off);
    while (len > 0) {
      if (p == ls->end())
	throw end_of_buffer();
      
      unsigned howmuch = p->length() - p_off;
      if (len < howmuch)
	howmuch = len;
      p->copy_in(p_off, howmuch, src);
	
      src += howmuch;
      len -= howmuch;
      advance(howmuch);
    }
  }
  
  void buffer::list::iterator::copy_in(unsigned len, const list& otherl)
  {
    if (p == ls->end())
      seek(off);
    unsigned left = len;
    for (ptr_list_t::const_iterator i = otherl._ptrs.begin();
	 i != otherl._ptrs.end();
	 ++i) {
      unsigned l = (*i).length();
      if (left < l)
	l = left;
      copy_in(l, i->c_str());
      left -= l;
      if (left == 0)
	break;
    }
  }


  // -- buffer::list --
  buffer::ptr *buffer::list::_get_raw_ptr(raw *r, unsigned off, unsigned len)
  {
    if (!len)
      len = r->len;

    // try for an embedded ptr
    r->ptr_lock.lock();
    for (unsigned i=0; i<RAW_NUM_PTRS; ++i) {
      if (!r->ptrs[i].have_raw()) {
	ptr *p = &r->ptrs[i];
	p->reset(r, off, len);
	bdout << "list::_get_raw_ptr " << this << " raw " << r
	      << " took ptr " << i << " " << *p << bendl;
	r->ptr_lock.unlock();
	return p;
      }
    }
    r->ptr_lock.unlock();

    // allocate a new ptr
    return new ptr(r, off, len);
  }

  void buffer::list::swap(list& other)
  {
    std::swap(_len, other._len);
    std::swap(_memcopy_count, other._memcopy_count);
    _ptrs.swap(other._ptrs);
    append_buffer.swap(other.append_buffer);
    //last_p.swap(other.last_p);
    last_p = begin();
    other.last_p = other.begin();
  }

  bool buffer::list::contents_equal(ceph::buffer::list& other)
  {
    if (length() != other.length())
      return false;

    // buffer-wise comparison
    if (true) {
      ptr_list_t::const_iterator a = _ptrs.begin();
      ptr_list_t::const_iterator b = other._ptrs.begin();
      unsigned aoff = 0, boff = 0;
      while (a != _ptrs.end()) {
	unsigned len = a->length() - aoff;
	if (len > b->length() - boff)
	  len = b->length() - boff;
	if (memcmp(a->c_str() + aoff, b->c_str() + boff, len) != 0)
	  return false;
	aoff += len;
	if (aoff == a->length()) {
	  aoff = 0;
	  ++a;
	}
	boff += len;
	if (boff == b->length()) {
	  boff = 0;
	  ++b;
	}
      }
      assert(b == other._ptrs.end());
      return true;
    }

    // byte-wise comparison
    if (false) {
      bufferlist::iterator me = begin();
      bufferlist::iterator him = other.begin();
      while (!me.end()) {
	if (*me != *him)
	  return false;
	++me;
	++him;
      }
      return true;
    }
  }

  bool buffer::list::can_zero_copy() const
  {
    for (ptr_list_t::const_iterator it = _ptrs.begin();
	 it != _ptrs.end();
	 ++it)
      if (!it->can_zero_copy())
	return false;
    return true;
  }

  bool buffer::list::is_aligned(unsigned align) const
  {
    for (ptr_list_t::const_iterator it = _ptrs.begin();
	 it != _ptrs.end();
	 ++it) 
      if (!it->is_aligned(align))
	return false;
    return true;
  }

  bool buffer::list::is_n_align_sized(unsigned align) const
  {
    for (ptr_list_t::const_iterator it = _ptrs.begin();
	 it != _ptrs.end();
	 ++it) 
      if (!it->is_n_align_sized(align))
	return false;
    return true;
  }

  bool buffer::list::is_zero() const {
    for (ptr_list_t::const_iterator it = _ptrs.begin();
	 it != _ptrs.end();
	 ++it) {
      if (!it->is_zero()) {
	return false;
      }
    }
    return true;
  }

  void buffer::list::zero()
  {
    for (ptr_list_t::iterator it = _ptrs.begin();
	 it != _ptrs.end();
	 ++it)
      it->zero();
  }

  void buffer::list::zero(unsigned o, unsigned l)
  {
    assert(o+l <= _len);
    unsigned p = 0;
    for (ptr_list_t::iterator it = _ptrs.begin();
	 it != _ptrs.end();
	 ++it) {
      if (p + it->length() > o) {
        if (p >= o && p+it->length() <= o+l) {
          // 'o'------------- l -----------|
          //      'p'-- it->length() --|
	  it->zero();
        } else if (p >= o) {
          // 'o'------------- l -----------|
          //    'p'------- it->length() -------|
	  it->zero(0, o+l-p);
        } else if (p + it->length() <= o+l) {
          //     'o'------------- l -----------|
          // 'p'------- it->length() -------|
	  it->zero(o-p, it->length()-(o-p));
        } else {
          //       'o'----------- l -----------|
          // 'p'---------- it->length() ----------|
          it->zero(o-p, l);
        }
      }
      p += it->length();
      if (o+l <= p)
	break;  // done
    }
  }
  
  bool buffer::list::is_contiguous()
  {
    return &(*_ptrs.begin()) == &(*_ptrs.rbegin());
  }

  bool buffer::list::is_n_page_sized() const
  {
    return is_n_align_sized(CEPH_PAGE_SIZE);
  }

  bool buffer::list::is_page_aligned() const
  {
    return is_aligned(CEPH_PAGE_SIZE);
  }

  void buffer::list::rebuild()
  {
    ptr nb;
    if ((_len & ~CEPH_PAGE_MASK) == 0)
      nb = buffer::create_page_aligned(_len);
    else
      nb = buffer::create(_len);
    rebuild(nb);
  }

  void buffer::list::rebuild(ptr& nb)
  {
    unsigned pos = 0;
    for (ptr_list_t::iterator it = _ptrs.begin();
	 it != _ptrs.end();
	 ++it) {
      nb.copy_in(pos, it->length(), it->c_str());
      pos += it->length();
    }
    _memcopy_count += pos;
    clear();
    append(nb);
  }

void buffer::list::rebuild_aligned(unsigned align)
{
  rebuild_aligned_size_and_memory(align, align);
}

void buffer::list::rebuild_aligned_size_and_memory(unsigned align_size,
						   unsigned align_memory)
{
  ptr_list_t::iterator p = _ptrs.begin();
  while (p != _ptrs.end()) {
    // keep anything that's already align and sized aligned
    if (p->is_aligned(align_memory) && p->is_n_align_sized(align_size)) {
      /*cout << " segment " << (void*)p->c_str()
	     << " offset " << ((unsigned long)p->c_str() & (align - 1))
	     << " length " << p->length()
	     << " " << (p->length() & (align - 1)) << " ok" << std::endl;
      */
      ++p;
      continue;
    }
    
    // consolidate unaligned items, until we get something that is sized+aligned
    list unaligned;
    unsigned offset = 0;
    do {
      /*cout << " segment " << (void*)p->c_str()
             << " offset " << ((unsigned long)p->c_str() & (align - 1))
             << " length " << p->length() << " " << (p->length() & (align - 1))
             << " overall offset " << offset << " " << (offset & (align - 1))
	     << " not ok" << std::endl;
      */
      offset += p->length();
      unaligned.push_back(*p);
      ptr *t = &(*p);
      _ptrs.erase(p++);
      t->unlinked_from_list();
    } while (p != _ptrs.end() &&
	     (!p->is_aligned(align_memory) ||
	      !p->is_n_align_sized(align_size) ||
	      (offset % align_size)));
    if (!(unaligned.is_contiguous() &&
	  unaligned.front().is_aligned(align_memory))) {
      ptr nb(buffer::create_aligned(unaligned._len, align_memory));
      unaligned.rebuild(nb);
      _memcopy_count += unaligned._len;
    }
    _ptrs.splice(p, unaligned._ptrs);
  }
}

void buffer::list::rebuild_page_aligned()
{
  rebuild_aligned(CEPH_PAGE_SIZE);
}

  // sort-of-like-assignment-op
  void buffer::list::claim(list& bl, unsigned int flags)
  {
    // free my buffers
    clear();
    claim_append(bl, flags);
  }

  void buffer::list::claim_append(list& bl, unsigned int flags)
  {
    // steal the other guy's buffers
    _len += bl._len;
    if (!(flags & CLAIM_ALLOW_NONSHAREABLE))
      bl.make_shareable();
    _ptrs.splice(_ptrs.end(), bl._ptrs);
    bl._len = 0;
    bl.last_p = bl.begin();
  }

  void buffer::list::claim_prepend(list& bl, unsigned int flags)
  {
    // steal the other guy's buffers
    _len += bl._len;
    if (!(flags & CLAIM_ALLOW_NONSHAREABLE))
      bl.make_shareable();
    _ptrs.splice(_ptrs.begin(), bl._ptrs);
    bl._len = 0;
    bl.last_p = bl.begin();
  }

  void buffer::list::copy(unsigned off, unsigned len, char *dest) const
  {
    if (off + len > length())
      throw end_of_buffer();
    if (last_p.get_off() != off) 
      last_p.seek(off);
    last_p.copy(len, dest);
  }

  void buffer::list::copy(unsigned off, unsigned len, list &dest) const
  {
    if (off + len > length())
      throw end_of_buffer();
    if (last_p.get_off() != off) 
      last_p.seek(off);
    last_p.copy(len, dest);
  }

  void buffer::list::copy(unsigned off, unsigned len, std::string& dest) const
  {
    if (last_p.get_off() != off) 
      last_p.seek(off);
    return last_p.copy(len, dest);
  }
    
  void buffer::list::copy_in(unsigned off, unsigned len, const char *src)
  {
    if (off + len > length())
      throw end_of_buffer();
    
    if (last_p.get_off() != off) 
      last_p.seek(off);
    last_p.copy_in(len, src);
  }

  void buffer::list::copy_in(unsigned off, unsigned len, const list& src)
  {
    if (last_p.get_off() != off) 
      last_p.seek(off);
    last_p.copy_in(len, src);
  }

  void buffer::list::append(char c)
  {
    // put what we can into the existing append_buffer.
    unsigned gap = append_buffer.unused_tail_length();
    if (!gap) {
      // make a new append_buffer!
      append_buffer = create_aligned(CEPH_BUFFER_APPEND_SIZE, CEPH_BUFFER_APPEND_SIZE);
      append_buffer.set_length(0);   // unused, so far.
    }
    append_buffer.append(c);
    append(append_buffer, append_buffer.end() - 1, 1);	// add segment to the list
  }
  
  void buffer::list::append(const char *data, unsigned len)
  {
    while (len > 0) {
      // put what we can into the existing append_buffer.
      unsigned gap = append_buffer.unused_tail_length();
      if (gap > 0) {
	if (gap > len) gap = len;
	//cout << "append first char is " << data[0] << ", last char is " << data[len-1] << std::endl;
	append_buffer.append(data, gap);
	append(append_buffer, append_buffer.end() - gap, gap);	// add segment to the list
	len -= gap;
	data += gap;
      }
      if (len == 0)
	break;  // done!
      
      // make a new append_buffer!
      unsigned alen = CEPH_BUFFER_APPEND_SIZE * (((len-1) / CEPH_BUFFER_APPEND_SIZE) + 1);
      append_buffer.reset(create_aligned(alen, CEPH_BUFFER_APPEND_SIZE), 0, 0);
    }
  }

  void buffer::list::append(const ptr& bp, unsigned off, unsigned len)
  {
    assert(len+off <= bp.length());
    if (!_ptrs.empty()) {
      ptr &l = _ptrs.back();
      if (l.get_raw() == bp.get_raw() &&
	  l.end() == bp.start() + off) {
	// yay contiguous with tail bp!
	l.set_length(l.length()+len);
	_len += len;
	return;
      }
    }
    // add new item to list
    push_back(bp.get_raw(), bp.offset() + off, len);
  }

  void buffer::list::append(const list& bl)
  {
    for (ptr_list_t::const_iterator p = bl._ptrs.begin();
	 p != bl._ptrs.end();
	 ++p)
      push_back(*p);
  }

  void buffer::list::append(std::istream& in)
  {
    while (!in.eof()) {
      std::string s;
      getline(in, s);
      append(s.c_str(), s.length());
      if (s.length())
	append("\n", 1);
    }
  }
  
  void buffer::list::append_zero(unsigned len)
  {
    ptr *t = new ptr(len);
    t->zero();
    _ptrs.push_back(*t);
    _len += len;
  }

  
  /*
   * get a char
   */
  const char& buffer::list::operator[](unsigned n) const
  {
    if (n >= _len)
      throw end_of_buffer();
    
    for (ptr_list_t::const_iterator p = _ptrs.begin();
	 p != _ptrs.end();
	 ++p) {
      if (n >= p->length()) {
	n -= p->length();
	continue;
      }
      return (*p)[n];
    }
    assert(0);
  }

  /*
   * return a contiguous ptr to whole bufferlist contents.
   */
  char *buffer::list::c_str()
  {
    if (_ptrs.empty())
      return 0;                         // no buffers

    ptr_list_t::const_iterator iter = _ptrs.begin();
    ++iter;

    if (iter != _ptrs.end())
      rebuild();
    return _ptrs.front().c_str();  // good, we're already contiguous.
  }

  char *buffer::list::get_contiguous(unsigned orig_off, unsigned len)
  {
    if (orig_off + len > length())
      throw end_of_buffer();

    if (len == 0) {
      return 0;
    }

    unsigned off = orig_off;
    ptr_list_t::iterator curbuf = _ptrs.begin();
    while (off > 0 && off >= curbuf->length()) {
      off -= curbuf->length();
      ++curbuf;
    }

    if (off + len > curbuf->length()) {
      bufferlist tmp;
      unsigned l = off + len;

      do {
	if (l >= curbuf->length())
	  l -= curbuf->length();
	else
	  l = 0;
	// more ptr to tmp
	ptr *t = &(*curbuf);
	curbuf = _ptrs.erase(curbuf);
	tmp._ptrs.push_back(*t);
	tmp._len += t->length();
      } while (curbuf != _ptrs.end() && l > 0);

      assert(l == 0);

      tmp.rebuild();
      ptr *front = &tmp._ptrs.front();
      _ptrs.splice(curbuf, tmp._ptrs);
      return front->c_str() + off;
    }

    return curbuf->c_str() + off;
  }

  void buffer::list::substr_of(const list& other, unsigned off, unsigned len)
  {
    if (off + len > other.length())
      throw end_of_buffer();

    clear();

    // skip off
    ptr_list_t::const_iterator curbuf = other._ptrs.begin();
    while (off > 0 &&
	   off >= curbuf->length()) {
      // skip this buffer
      //cout << "skipping over " << *curbuf << std::endl;
      off -= (*curbuf).length();
      ++curbuf;
    }
    assert(len == 0 || curbuf != other._ptrs.end());
    
    while (len > 0) {
      // partial?
      if (off + len < curbuf->length()) {
	//cout << "copying partial of " << *curbuf << std::endl;
	_ptrs.push_back(*(new ptr(*curbuf, off, len)));
	_len += len;
	break;
      }
      
      // through end
      //cout << "copying end (all?) of " << *curbuf << std::endl;
      unsigned howmuch = curbuf->length() - off;
      _ptrs.push_back(*(new ptr(*curbuf, off, howmuch)));
      _len += howmuch;
      len -= howmuch;
      off = 0;
      ++curbuf;
    }
  }

  // funky modifer
  void buffer::list::splice(unsigned off, unsigned len, list *claim_by /*, bufferlist& replace_with */)
  {    // fixme?
    if (len == 0)
      return;

    if (off >= length())
      throw end_of_buffer();

    assert(len > 0);
    bdout << "splice off " << off << " len " << len << " ... mylen = " << length() << bendl;
      
    // skip off
    ptr_list_t::iterator curbuf = _ptrs.begin();
    while (off > 0) {
      assert(curbuf != _ptrs.end());
      if (off >= (*curbuf).length()) {
	// skip this buffer
	bdout << "off = " << off << " skipping over " << *curbuf << bendl;
	off -= (*curbuf).length();
	++curbuf;
      } else {
	// somewhere in this buffer!
	bdout << "off = " << off << " somewhere in " << *curbuf << bendl;
	break;
      }
    }
    
    if (off) {
      // add a reference to the front bit
      //  insert it before curbuf (which we'll hose)
      bdout << "keeping front " << off << " of " << *curbuf << bendl;
      _ptrs.insert(curbuf, *(new ptr(*curbuf, 0, off)));
      _len += off;
    }
    
    while (len > 0) {
      // partial?
      if (off + len < (*curbuf).length()) {
	bdout << "keeping end of " << *curbuf << ", losing first " << off+len << bendl;
	if (claim_by) 
	  claim_by->append( *curbuf, off, len );
	(*curbuf).set_offset( off+len + (*curbuf).offset() );    // ignore beginning big
	(*curbuf).set_length( (*curbuf).length() - (len+off) );
	_len -= off+len;
	bdout << " now " << *curbuf << bendl;
	break;
      }
      
      // hose though the end
      unsigned howmuch = (*curbuf).length() - off;
      bdout << "discarding " << howmuch << " of " << *curbuf << bendl;
      if (claim_by) 
	claim_by->append( *curbuf, off, howmuch );
      _len -= (*curbuf).length();
      ptr *t = &(*curbuf);
      curbuf = _ptrs.erase(curbuf);
      t->unlinked_from_list();
      len -= howmuch;
      off = 0;
    }
      
    // splice in *replace (implement me later?)
    
    last_p = begin();  // just in case we were in the removed region.
    bdout << "splice done" << bendl;
  }

  void buffer::list::write(int off, int len, std::ostream& out) const
  {
    list s;
    s.substr_of(*this, off, len);
    for (ptr_list_t::const_iterator it = s._ptrs.begin();
	 it != s._ptrs.end();
	 ++it)
      if (it->length())
	out.write(it->c_str(), it->length());
    /*iterator p(this, off);
      while (len > 0 && !p.end()) {
      int l = p.left_in_this_buf();
      if (l > len)
      l = len;
      out.write(p.c_str(), l);
      len -= l;
      }*/
  }
  
void buffer::list::encode_base64(buffer::list& o)
{
  bufferptr bp(length() * 4 / 3 + 3);
  int l = ceph_armor(bp.c_str(), bp.c_str() + bp.length(), c_str(), c_str() + length());
  bp.set_length(l);
  o.push_back(bp);
}

void buffer::list::decode_base64(buffer::list& e)
{
  bufferptr bp(4 + ((e.length() * 3) / 4));
  int l = ceph_unarmor(bp.c_str(), bp.c_str() + bp.length(), e.c_str(), e.c_str() + e.length());
  if (l < 0) {
    std::ostringstream oss;
    oss << "decode_base64: decoding failed:\n";
    hexdump(oss);
    throw buffer::malformed_input(oss.str().c_str());
  }
  assert(l <= (int)bp.length());
  bp.set_length(l);
  push_back(bp);
}

  

int buffer::list::read_file(const char *fn, std::string *error)
{
  int fd = TEMP_FAILURE_RETRY(::open(fn, O_RDONLY));
  if (fd < 0) {
    int err = errno;
    std::ostringstream oss;
    oss << "can't open " << fn << ": " << cpp_strerror(err);
    *error = oss.str();
    return -err;
  }

  struct stat st;
  memset(&st, 0, sizeof(st));
  ::fstat(fd, &st);

  ssize_t ret = read_fd(fd, st.st_size);
  if (ret < 0) {
    std::ostringstream oss;
    oss << "bufferlist::read_file(" << fn << "): read error:"
	<< cpp_strerror(ret);
    *error = oss.str();
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }
  else if (ret != st.st_size) {
    // Premature EOF.
    // Perhaps the file changed between stat() and read()?
    std::ostringstream oss;
    oss << "bufferlist::read_file(" << fn << "): warning: got premature EOF.";
    *error = oss.str();
    // not actually an error, but weird
  }
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  return 0;
}

ssize_t buffer::list::read_fd(int fd, size_t len)
{
  // try zero copy first
  if (false && read_fd_zero_copy(fd, len) == 0) {
    // TODO fix callers to not require correct read size, which is not
    // available for raw_pipe until we actually inspect the data
    return 0;
  }
  int s = ROUND_UP_TO(len, CEPH_BUFFER_APPEND_SIZE);
  bufferptr bp = buffer::create_aligned(s, CEPH_BUFFER_APPEND_SIZE);
  ssize_t ret = safe_read(fd, (void*)bp.c_str(), len);
  if (ret >= 0) {
    bp.set_length(ret);
    append(bp);
  }
  return ret;
}

int buffer::list::read_fd_zero_copy(int fd, size_t len)
{
#ifdef CEPH_HAVE_SPLICE
  try {
    bufferptr bp = buffer::create_zero_copy(len, fd, NULL);
    append(bp);
  } catch (buffer::error_code &e) {
    return e.code;
  } catch (buffer::malformed_input &e) {
    return -EIO;
  }
  return 0;
#else
  return -ENOTSUP;
#endif
}

int buffer::list::write_file(const char *fn, int mode)
{
  int fd = TEMP_FAILURE_RETRY(::open(fn, O_WRONLY|O_CREAT|O_TRUNC, mode));
  if (fd < 0) {
    int err = errno;
    cerr << "bufferlist::write_file(" << fn << "): failed to open file: "
	 << cpp_strerror(err) << std::endl;
    return -err;
  }
  int ret = write_fd(fd);
  if (ret) {
    cerr << "bufferlist::write_fd(" << fn << "): write_fd error: "
	 << cpp_strerror(ret) << std::endl;
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }
  if (TEMP_FAILURE_RETRY(::close(fd))) {
    int err = errno;
    cerr << "bufferlist::write_file(" << fn << "): close error: "
	 << cpp_strerror(err) << std::endl;
    return -err;
  }
  return 0;
}

int buffer::list::write_fd(int fd) const
{
  if (can_zero_copy())
    return write_fd_zero_copy(fd);

  // use writev!
  iovec iov[IOV_MAX];
  int iovlen = 0;
  ssize_t bytes = 0;

  ptr_list_t::const_iterator p = _ptrs.begin();
  while (p != _ptrs.end()) {
    if (p->length() > 0) {
      iov[iovlen].iov_base = (void *)p->c_str();
      iov[iovlen].iov_len = p->length();
      bytes += p->length();
      iovlen++;
    }
    ++p;

    if (iovlen == IOV_MAX-1 ||
	p == _ptrs.end()) {
      iovec *start = iov;
      int num = iovlen;
      ssize_t wrote;
    retry:
      wrote = ::writev(fd, start, num);
      if (wrote < 0) {
	int err = errno;
	if (err == EINTR)
	  goto retry;
	return -err;
      }
      if (wrote < bytes) {
	// partial write, recover!
	while ((size_t)wrote >= start[0].iov_len) {
	  wrote -= start[0].iov_len;
	  bytes -= start[0].iov_len;
	  start++;
	  num--;
	}
	if (wrote > 0) {
	  start[0].iov_len -= wrote;
	  start[0].iov_base = (char *)start[0].iov_base + wrote;
	  bytes -= wrote;
	}
	goto retry;
      }
      iovlen = 0;
      bytes = 0;
    }
  }
  return 0;
}

int buffer::list::write_fd_zero_copy(int fd) const
{
  if (!can_zero_copy())
    return -ENOTSUP;
  /* pass offset to each call to avoid races updating the fd seek
   * position, since the I/O may be non-blocking
   */
  int64_t offset = ::lseek(fd, 0, SEEK_CUR);
  int64_t *off_p = &offset;
  if (offset < 0 && offset != ESPIPE)
    return (int) offset;
  if (offset == ESPIPE)
    off_p = NULL;
  for (ptr_list_t::const_iterator it = _ptrs.begin();
       it != _ptrs.end(); ++it) {
    int r = it->zero_copy_to_fd(fd, off_p);
    if (r < 0)
      return r;
    if (off_p)
      offset += it->length();
  }
  return 0;
}

__u32 buffer::list::crc32c(__u32 crc) const
{
  for (ptr_list_t::const_iterator it = _ptrs.begin();
       it != _ptrs.end();
       ++it) {
    if (it->length()) {
      raw *r = it->get_raw();
      pair<size_t, size_t> ofs(it->offset(), it->offset() + it->length());
      pair<uint32_t, uint32_t> ccrc;
      if (r->get_crc(ofs, &ccrc)) {
	if (ccrc.first == crc) {
	  // got it already
	  crc = ccrc.second;
	  if (buffer_track_crc)
	    buffer_cached_crc.inc();
	} else {
	  /* If we have cached crc32c(buf, v) for initial value v,
	   * we can convert this to a different initial value v' by:
	   * crc32c(buf, v') = crc32c(buf, v) ^ adjustment
	   * where adjustment = crc32c(0*len(buf), v ^ v')
	   *
	   * http://crcutil.googlecode.com/files/crc-doc.1.0.pdf
	   * note, u for our crc32c implementation is 0
	   */
	  crc = ccrc.second ^ ceph_crc32c(ccrc.first ^ crc, NULL, it->length());
	  if (buffer_track_crc)
	    buffer_cached_crc_adjusted.inc();
	}
      } else {
	uint32_t base = crc;
	crc = ceph_crc32c(crc, (unsigned char*)it->c_str(), it->length());
	r->set_crc(ofs, make_pair(base, crc));
      }
    }
  }
  return crc;
}

void buffer::list::invalidate_crc()
{
  for (ptr_list_t::const_iterator p = _ptrs.begin(); p != _ptrs.end(); ++p) {
    raw *r = p->get_raw();
    if (r) {
      r->invalidate_crc();
    }
  }
}

/**
 * Binary write all contents to a C++ stream
 */
void buffer::list::write_stream(std::ostream &out) const
{
  for (ptr_list_t::const_iterator p = _ptrs.begin(); p != _ptrs.end(); ++p) {
    if (p->length() > 0) {
      out.write(p->c_str(), p->length());
    }
  }
}


void buffer::list::hexdump(std::ostream &out) const
{
  std::ios_base::fmtflags original_flags = out.flags();

  out.setf(std::ios::right);
  out.fill('0');

  unsigned per = 16;

  for (unsigned o=0; o<length(); o += per) {
    out << std::hex << std::setw(4) << o << " :";

    unsigned i;
    for (i=0; i<per && o+i<length(); i++) {
      out << " " << std::setw(2) << ((unsigned)(*this)[o+i] & 0xff);
    }
    for (; i<per; i++)
      out << "   ";
    
    out << " : ";
    for (i=0; i<per && o+i<length(); i++) {
      char c = (*this)[o+i];
      if (isupper(c) || islower(c) || isdigit(c) || c == ' ' || ispunct(c))
	out << c;
      else
	out << '.';
    }
    out << std::dec << std::endl;
  }

  out.flags(original_flags);
}

std::ostream& operator<<(std::ostream& out, const buffer::raw &r) {
  return out << "buffer::raw(" << (void*)r.data << " len " << r.len << " nref " << r.nref.read() << ")";
}

std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp) {
  if (bp.have_raw())
    out << "buffer::ptr(" << bp.offset() << "~" << bp.length()
	<< " " << (void*)bp.c_str()
	<< " in raw " << (void*)bp.raw_c_str()
	<< " len " << bp.raw_length()
	<< " nref " << bp.raw_nref() << ")";
  else
    out << "buffer:ptr(" << bp.offset() << "~" << bp.length() << " no raw)";
  return out;
}

std::ostream& operator<<(std::ostream& out, const buffer::list& bl) {
  out << "buffer::list(len=" << bl.length() << "," << std::endl;

  const buffer::list::ptr_list_t& ls = bl.get_raw_ptr_list();
  buffer::list::ptr_list_t::const_iterator it = ls.begin();
  while (it != ls.end()) {
    out << "\t" << *it;
    if (++it == ls.end())
      break;
    out << "," << std::endl;
  }
  out << std::endl << ")";
  return out;
}

std::ostream& operator<<(std::ostream& out, const buffer::error& e)
{
  return out << e.what();
}

}
