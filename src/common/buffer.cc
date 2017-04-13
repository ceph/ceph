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

#include "include/compat.h"
#include "include/mempool.h"
#include "armor.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/simple_spin.h"
#include "common/strtol.h"
#include "common/likely.h"
#include "common/valgrind.h"
#include "common/deleter.h"
#include "include/atomic.h"
#include "common/RWLock.h"
#include "include/types.h"
#include "include/compat.h"
#include "include/inline_memory.h"
#include "include/scope_guard.h"
#if defined(HAVE_XIO)
#include "msg/xio/XioMsg.h"
#endif

#include <errno.h>
#include <fstream>
#include <sstream>
#include <sys/uio.h>
#include <limits.h>
#include <poll.h>

#include <atomic>
#include <ostream>
#include <sys/ioctl.h>
#ifdef CEPH_HAVE_SPLICE
#include "sys/socket.h"
#include "linux/if_alg.h"
#include "fcntl.h"
#endif

#define CEPH_BUFFER_ALLOC_UNIT  (MIN(CEPH_PAGE_SIZE, 4096))
#define CEPH_BUFFER_APPEND_SIZE (CEPH_BUFFER_ALLOC_UNIT - sizeof(raw_combined))

#ifdef BUFFER_DEBUG
static std::atomic_flag buffer_debug_lock = ATOMIC_FLAG_INIT;
# define bdout { simple_spin_lock(&buffer_debug_lock); std::cout
# define bendl std::endl; simple_spin_unlock(&buffer_debug_lock); }
#else
# define bdout if (0) { std::cout
# define bendl std::endl; }
#endif

  static atomic_t buffer_total_alloc;
  static atomic64_t buffer_history_alloc_bytes;
  static atomic64_t buffer_history_alloc_num;
  const bool buffer_track_alloc = get_env_bool("CEPH_BUFFER_TRACK");

  namespace {
  void inc_total_alloc(unsigned len) {
    if (buffer_track_alloc)
      buffer_total_alloc.add(len);
  }

  void dec_total_alloc(unsigned len) {
    if (buffer_track_alloc)
      buffer_total_alloc.sub(len);
  }

  void inc_history_alloc(uint64_t len) {
    if (buffer_track_alloc) {
      buffer_history_alloc_bytes.add(len);
      buffer_history_alloc_num.inc();
    }
  }
    int lclose(int fd) {
      return TEMP_FAILURE_RETRY(::close(fd));
    }
  }

  int buffer::get_total_alloc() {
    return buffer_total_alloc.read();
  }
  uint64_t buffer::get_history_alloc_bytes() {
    return buffer_history_alloc_bytes.read();
  }
  uint64_t buffer::get_history_alloc_num() {
    return buffer_history_alloc_num.read();
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

  static atomic_t buffer_max_pipe_size(0);
  int update_max_pipe_size() {
#ifdef CEPH_HAVE_SETPIPE_SZ
    char buf[32];
    char* end_ptr;
    int r;
    struct stat stat_result;
    if (::stat(PROCPREFIX "/proc/sys/fs/pipe-max-size", &stat_result) == -1)
      return -errno;
    r = safe_read_file(PROCPREFIX "/proc/sys/fs/", "pipe-max-size",
		       buf, sizeof(buf) - 1);
    if (r < 0)
      return r;
    buf[r] = '\0';
    size_t size = strtoul(buf, &end_ptr, 10);
    if (end_ptr - buf > 0 && size > 4096)
      buffer_max_pipe_size.set(size);
    else
      buffer_max_pipe_size.set(4096); //set something non-zero
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

  const char * buffer::error::what() const throw () {
    return "buffer::exception";
  }
  const char * buffer::bad_alloc::what() const throw () {
    return "buffer::bad_alloc";
  }
  const char * buffer::end_of_buffer::what() const throw () {
    return "buffer::end_of_buffer";
  }
  const char * buffer::malformed_input::what() const throw () {
    return buf;
  }
  buffer::error_code::error_code(int error) :
    buffer::malformed_input(cpp_strerror(error).c_str()), code(error) {}

  class buffer::raw {
  protected:
    char *data;
  public:
    unsigned len;
    atomic_t nref;

    mutable std::atomic_flag crc_spinlock = ATOMIC_FLAG_INIT;
    map<pair<size_t, size_t>, pair<uint32_t, uint32_t> > crc_map;

    explicit raw(unsigned l)
      : data(NULL), len(l), nref(0)
    { }
    raw(char *c, unsigned l)
      : data(c), len(l), nref(0)
    { }
    virtual ~raw() {}

    // no copying.
    // cppcheck-suppress noExplicitConstructor
    raw(const raw &other);
    const raw& operator=(const raw &other);

    virtual char *get_data() {
      return data;
    }
    virtual raw* clone_empty() = 0;
    virtual raw *clone() {
      raw *c = clone_empty();
      memcpy(c->data, data, len);
      return c;
    }
    virtual bool is_data_local() const {
      return true;
    }
    /*
     * Write buffer contents to file.
     *
     * \note This is default implementation. It is used for all, except zero-copy based buffers.
     *
     * \param fd file to write to
     * \param src_pos start streaming from this byte
     * \param count amount of bytes to write
     * \param dst_offset offset in \ref fd to write to. use -1 to append to current file position.
     */
    virtual ssize_t copy_to_fd(int fd, off_t src_pos, ssize_t count, off_t dst_offset = -1) {
      if (src_pos + count > len) {
        throw end_of_buffer();
      }
      ssize_t r;
      if (dst_offset == -1) {
        r = safe_write(fd, data + src_pos, count);
        if (r == 0)
          r = count;
      }
      else
        r = safe_pwrite(fd, data + src_pos, count, dst_offset);
      return r;
    }

    /*
     * Read from file to buffer.
     *
     * \note This is default implementation. It is used for all, except zero-copy based buffers.
     *
     * \param dst_pos position in buffer to write to
     * \param count amount of bytes to read
     * \param fd file to read from
     * \param src_offset offset in file to read from. use -1 to read from current file position.
     */
    virtual ssize_t insert_from_fd(size_t dst_pos, size_t count, int fd, off_t src_offset = -1)
    {
      if (dst_pos + count > len) {
        throw end_of_buffer();
      }
      ssize_t w;
      if (src_offset == -1)
        w = safe_read(fd, data + dst_pos, count);
      else
        w = safe_pread(fd, data + dst_pos, count, src_offset);
      return w;
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
    virtual uint32_t crc32c(unsigned int off, unsigned int len,uint32_t init_crc) {
      uint32_t crc;
      crc = ceph_crc32c(init_crc, reinterpret_cast<unsigned char*>(data) + off, len);
      set_crc(make_pair(off, off + len), make_pair(init_crc, crc));
      return crc;
    }
    bool get_crc(const pair<size_t, size_t> &fromto,
         pair<uint32_t, uint32_t> *crc) const {
      simple_spin_lock(&crc_spinlock);
      map<pair<size_t, size_t>, pair<uint32_t, uint32_t> >::const_iterator i =
      crc_map.find(fromto);
      if (i == crc_map.end()) {
          simple_spin_unlock(&crc_spinlock);
          return false;
      }
      *crc = i->second;
      simple_spin_unlock(&crc_spinlock);
      return true;
    }
    void set_crc(const pair<size_t, size_t> &fromto,
         const pair<uint32_t, uint32_t> &crc) {
      simple_spin_lock(&crc_spinlock);
      crc_map[fromto] = crc;
      simple_spin_unlock(&crc_spinlock);
    }
    void invalidate_crc() {
      simple_spin_lock(&crc_spinlock);
      if (crc_map.size() != 0) {
        crc_map.clear();
      }
      simple_spin_unlock(&crc_spinlock);
    }
  };

  MEMPOOL_DEFINE_FACTORY(char, char, buffer_data);

  ssize_t buffer::insert_from_fd(buffer::raw* _raw, size_t position, size_t count, int fd, off_t src_offset) {
    return _raw->insert_from_fd(position, count, fd, src_offset);
  }

  /*
   * raw_combined is always placed within a single allocation along
   * with the data buffer.  the data goes at the beginning, and
   * raw_combined at the end.
   */
  class buffer::raw_combined : public buffer::raw {
    size_t alignment;
  public:
    raw_combined(char *dataptr, unsigned l, unsigned align=0)
      : raw(dataptr, l),
	alignment(align) {
      inc_total_alloc(len);
      inc_history_alloc(len);
    }
    ~raw_combined() override {
      dec_total_alloc(len);
    }
    raw* clone_empty() override {
      return create(len, alignment);
    }

    static raw_combined *create(unsigned len, unsigned align=0) {
      if (!align)
	align = sizeof(size_t);
      size_t rawlen = ROUND_UP_TO(sizeof(buffer::raw_combined),
				  alignof(buffer::raw_combined));
      size_t datalen = ROUND_UP_TO(len, alignof(buffer::raw_combined));

      char *ptr = mempool::buffer_data::alloc_char.allocate_aligned(
	rawlen + datalen, align);
      if (!ptr)
	throw bad_alloc();

      // actual data first, since it has presumably larger alignment restriction
      // then put the raw_combined at the end
      return new (ptr + datalen) raw_combined(ptr, len, align);
    }

    static void operator delete(void *ptr) {
      raw_combined *raw = (raw_combined *)ptr;
      size_t rawlen = ROUND_UP_TO(sizeof(buffer::raw_combined),
				  alignof(buffer::raw_combined));
      size_t datalen = ROUND_UP_TO(raw->len, alignof(buffer::raw_combined));
      mempool::buffer_data::alloc_char.deallocate_aligned(
	raw->data, rawlen + datalen);
    }
  };

  class buffer::raw_malloc : public buffer::raw {
  public:
    MEMPOOL_CLASS_HELPERS();

    explicit raw_malloc(unsigned l) : raw(l) {
      if (len) {
	data = (char *)malloc(len);
        if (!data)
          throw bad_alloc();
      } else {
	data = 0;
      }
      inc_total_alloc(len);
      inc_history_alloc(len);
      bdout << "raw_malloc " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    raw_malloc(unsigned l, char *b) : raw(b, l) {
      inc_total_alloc(len);
      bdout << "raw_malloc " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    ~raw_malloc() override {
      free(data);
      dec_total_alloc(len);
      bdout << "raw_malloc " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() override {
      return new raw_malloc(len);
    }
  };

#ifndef __CYGWIN__
  class buffer::raw_mmap_pages : public buffer::raw {
  public:
    MEMPOOL_CLASS_HELPERS();

    explicit raw_mmap_pages(unsigned l) : raw(l) {
      data = (char*)::mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
      if (!data)
	throw bad_alloc();
      inc_total_alloc(len);
      inc_history_alloc(len);
      bdout << "raw_mmap " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    ~raw_mmap_pages() override {
      ::munmap(data, len);
      dec_total_alloc(len);
      bdout << "raw_mmap " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() override {
      return new raw_mmap_pages(len);
    }
  };

  class buffer::raw_posix_aligned : public buffer::raw {
    unsigned align;
  public:
    MEMPOOL_CLASS_HELPERS();

    raw_posix_aligned(unsigned l, unsigned _align) : raw(l) {
      align = _align;
      assert((align >= sizeof(void *)) && (align & (align - 1)) == 0);
      data = mempool::buffer_data::alloc_char.allocate_aligned(len, align);
      if (!data)
	throw bad_alloc();
      inc_total_alloc(len);
      inc_history_alloc(len);
      bdout << "raw_posix_aligned " << this << " alloc " << (void *)data << " l=" << l << ", align=" << align << " total_alloc=" << buffer::get_total_alloc() << bendl;
    }
    ~raw_posix_aligned() override {
      mempool::buffer_data::alloc_char.deallocate_aligned(data, len);
      dec_total_alloc(len);
      bdout << "raw_posix_aligned " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() override {
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
      inc_history_alloc(len+align-1);
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
/*
 * Specialization of buffer::raw that implements zero-copy techniques.
 * It is created for linux kernel. Uses syscalls and behaviours specific to it.
 *
 * Data is stored in series of linux pipes.
 * If not specifically required by invoking \ref get_data(), data will remain in kernel space.
 *
 * \note For non-linux builds this class is not compiled and buffer::create_zero_copy produces regular buffer.
 */
class buffer::raw_splice_kcrypt : public buffer::raw {
private:
  static constexpr int read_end = 0;
  static constexpr int write_end = 1;
  static constexpr int min_pipe_size = 1024*256; // < drop from using zero-copy if pipes are too small
  struct pipe_elem
  {
    size_t size;
    int fd;
    //necessary for emplace_back() and resize()
    pipe_elem() = default;
    pipe_elem(size_t size, int fd): size(size), fd(fd) {};
    pipe_elem& operator=(const pipe_elem& o) = default;
  };
  std::vector<pipe_elem> pipe_read_elements;
  int pipe_curr_write_fd; //this changes as writing progresses to next pipes
  size_t actual_content_size;

  int close(int fd) {
    return TEMP_FAILURE_RETRY(::close(fd));
  }

  static const size_t min_useful_splice_buffer = 1 << 16;
public:
  MEMPOOL_CLASS_HELPERS();
  explicit raw_splice_kcrypt(unsigned len) : raw(len),
      pipe_curr_write_fd(-1),
      actual_content_size(0) {
    if (get_max_pipe_size()<min_pipe_size) {
      data = new char[len];
      return;
    }
    int no_pipes = len / get_max_pipe_size() + 1; //this is estimated #pipes to be needed
    pipe_read_elements.reserve(no_pipes);
  }

  ~raw_splice_kcrypt() {
    if (data)
      delete[] data;

    if (pipe_curr_write_fd != -1) {
      lclose(pipe_curr_write_fd);
    }
    for (auto& elem : pipe_read_elements) {
      lclose(elem.fd);
    }
  }

  buffer::raw* create_zero_copy(size_t capacity) {
    if (capacity >= raw_splice_kcrypt::min_useful_splice_buffer) {
      return new raw_splice_kcrypt(capacity);
    } else {
      return create_page_aligned(capacity);
    }
  }


#ifndef AF_ALG
#define AF_ALG 38
#endif
#ifndef SOL_ALG
#define SOL_ALG 279
#endif


  uint32_t crc32c(unsigned int off, unsigned int len, uint32_t init_crc) {
    uint32_t crc;
    int serv = -1;
    int targ = -1;
    int r;
    if (!is_data_local()) {
      try {
        serv = socket(AF_ALG, SOCK_SEQPACKET, 0);
        if (serv == -1) {
          throw __LINE__;
        }
        struct sockaddr_alg addr = {0};
        addr.salg_family = AF_ALG;
        memcpy(addr.salg_type, "hash", 5);
        memcpy(addr.salg_name, "crc32c", 7);
        r = bind(serv, (struct sockaddr *)&addr, sizeof(addr));
        if (r != 0) {
          throw __LINE__;
        }
        uint32_t swapped_crc = htonl(init_crc);
        r = setsockopt(serv, SOL_ALG, ALG_SET_KEY, &swapped_crc, 4);
        if (r != 0) {
          throw __LINE__;
        }
        targ = accept(serv, nullptr, 0);
        if (targ < 0) {
          throw __LINE__;
        }

        ssize_t l = len;
        ssize_t w;
        do {
          w = copy_to_fd(targ, off, l, -1);
          if (w <= 0) {
            throw __LINE__;
          }
          off += w;
          l -= w;
        } while (l > 0);
        r = read(targ, &crc, 4);
        if (r != 4) {
          throw __LINE__;
        }
        crc = ~crc;
        set_crc(make_pair(off, off + len), make_pair(init_crc, crc));
        return crc;
      } catch (int line) {
        if (serv != -1) {
          close(serv);
        }
        if (targ != -1) {
          close(targ);
        }
        get_data(); //make 'data' meaningfull
      }
    }
    //fallback when data in buffer
    crc = ceph_crc32c(init_crc, reinterpret_cast<unsigned char*>(data) + off, len);
    set_crc(make_pair(off, off + len), make_pair(init_crc, crc));
    return crc;
  }

  bool append_pipe() {
    if (pipe_curr_write_fd != -1) {
      lclose(pipe_curr_write_fd);
      pipe_curr_write_fd = -1;
    }
    int r;
    int fds[2];
    if (::pipe(fds) == -1) {
      return false;
    }
    r = ::fcntl(fds[write_end], F_SETFL, O_NONBLOCK);
    if (r != -1)
      r = ::fcntl(fds[read_end], F_SETFL, O_NONBLOCK);
    if (r < 0) {
      lclose(fds[write_end]);
      lclose(fds[read_end]);
      return false;
    }
    pipe_curr_write_fd = fds[write_end];
    set_pipe_size(pipe_curr_write_fd);
    pipe_read_elements.emplace_back(0, fds[read_end]);
    return true;
  }


  bool is_data_local() const {
    return data != nullptr;
  }

  ssize_t consume_pipe(int fd, size_t len)
  {
    int devnull;
    devnull = open("/dev/null", O_WRONLY);
    if (devnull == -1)
      return -1;
    ssize_t r;
    r = splice(fd, nullptr, devnull, nullptr, len, SPLICE_F_MOVE);
    assert(r == (ssize_t)len);
    ::close(devnull);
    return r;
  }

  bool is_pipe_full(int fd)
  {
    struct pollfd fds;
    fds.fd = fd;
    fds.events = POLLOUT;
    fds.revents = 0;
    int r = poll(&fds, 1, 0);
    //when no fds.revents are set, means we are blocked
    return (r == 0);
  }

  /*
   * Streams data from buffer to specified file.
   * If destination file is blocking, function will block until all data is transferred.
   * If destination file is non-blocking, function will transfer as much data as possible.
   *
   * \note Transferred data is not lost from buffer.
   *
   * \param fd file to write to
   * \param src_pos start streaming from this byte
   * \param count amount of bytes to write
   * \param dst_offset offset in \ref fd to write to. use -1 to append to current file position.
   *
   * \return amount of bytes transferred, or -1 if operation would block
   *
   * \throws end_of_buffer if request is outside buffer's range
   */
  ssize_t copy_to_fd(int fd, off_t ofs_from, ssize_t req_size, off_t dst_offset) {
    if (ofs_from + req_size > len) {
      throw end_of_buffer();
    }
    if (data != nullptr) {
      return buffer::raw::copy_to_fd(fd, ofs_from, req_size, dst_offset);
    }
    //move to proper pipe
    size_t i = 0;
    off_t ofs = ofs_from;
    for (i = 0; i < pipe_read_elements.size() && ofs >= (off_t)pipe_read_elements[i].size; i++ ) {
      ofs -= pipe_read_elements[i].size;
    }
    assert(i < pipe_read_elements.size());
    assert(ofs < (off_t)pipe_read_elements[i].size);
    ssize_t total_moved = 0;
    while (req_size > 0) {
      assert(i < pipe_read_elements.size());
      int fds[2];
      if (::pipe(fds) == -1) {
        get_data();
        return buffer::raw::copy_to_fd(fd, ofs_from, req_size, dst_offset);
     }

      set_pipe_size(fds[write_end]);
      //we do not check if pipe size is large enough
      //we will check if it is ok by watching result of ::tee()
      //in any case, for some file types (e.g. tcp sockets), linux kernel allows
      //tee() to clone more than is capacity of pipe

      ssize_t s;
      s = ::tee(pipe_read_elements[i].fd, fds[write_end],
                pipe_read_elements[i].size, SPLICE_F_MOVE);
      lclose(fds[write_end]);
      if (s < (ssize_t)pipe_read_elements[i].size) {
        lclose(fds[read_end]);
        get_data();
        return buffer::raw::copy_to_fd(fd, ofs_from, req_size, dst_offset);
      }

      ssize_t tomove_size = pipe_read_elements[i].size;
      if (ofs > 0) {
        if (consume_pipe(fds[read_end], ofs) != ofs) {
          lclose(fds[read_end]);
          get_data();
          return buffer::raw::copy_to_fd(fd, ofs_from, req_size, dst_offset);
        }
        tomove_size -= ofs;
        ofs = 0;
      }

      if (req_size < tomove_size)
        tomove_size = req_size;

      loff_t off_out = dst_offset;
      ssize_t moved_cnt;
      moved_cnt = splice(fds[read_end], nullptr,
                  fd, dst_offset == -1 ? nullptr : &off_out,
                  tomove_size, SPLICE_F_NONBLOCK | SPLICE_F_MOVE);
      if (moved_cnt == -1) {
        lclose(fds[read_end]);
        if (total_moved >0) {
          break;
        } else {
          if (errno == EAGAIN) {
            return -EAGAIN;
          } else {
            get_data();
            return buffer::raw::copy_to_fd(fd, ofs_from, req_size, dst_offset);
          }
        }
      }
      if (dst_offset != -1)
        dst_offset += moved_cnt;
      req_size -= moved_cnt;
      total_moved += moved_cnt;
      ofs_from += moved_cnt; //fix it in case we have to continue after error

      lclose(fds[read_end]);
      if (moved_cnt != tomove_size) {
        //there is something left not moved. it means we were blocked.
        break;
      }
      i++;
    }
    return total_moved;
  }

  /*
   * Read from file to buffer.
   *
   * \note This is special implementation for pipe-based zero-copy buffer.
   * If any problem arises fallback to regular memory buffer is applied.
   *
   * \param dst_pos position in buffer to write to
   * \param count amount of bytes to read
   * \param fd file to read from
   * \param src_offset offset in file to read from. use -1 to read from current file position.
   */
  ssize_t insert_from_fd(size_t dst_pos, size_t count, int fd, off_t src_offset)
  {
    if (data != nullptr) {
      return buffer::raw::insert_from_fd(dst_pos, count, fd, src_offset);
    }
    if (dst_pos != actual_content_size) {
      //convert and do anyway
      get_data();
      return buffer::raw::insert_from_fd(dst_pos, count, fd, src_offset);
    }
    ssize_t total_moved = 0;
    while (count > 0)
    {
      if (pipe_curr_write_fd == -1) {
        if(!append_pipe()) {
          get_data();
	  if (total_moved != 0) return total_moved ;
          return buffer::raw::insert_from_fd(dst_pos, count, fd, src_offset);
        }
      }
      int to_move = count;
      ssize_t r;
      int data_avail;
      if (src_offset == -1) {
        if (ioctl(fd, FIONREAD, &data_avail) < 0)
          return -EBADF;
      } else {
        struct stat f_status;
        if (fstat(fd, &f_status) < 0)
          return -EBADF;
        data_avail = f_status.st_size - src_offset;
      }

      if (data_avail == 0) {
        //no data, check if we work in nonblocking mode
        if (fcntl(fd, F_GETFL, 0) & O_NONBLOCK) {
          return -EAGAIN;
        } else {
          //wait for data
          struct pollfd fds;
          fds.fd = fd;
          fds.events = POLLIN;
          fds.revents = 0;
          poll(&fds, 1, -1);
          //when no fds.revents are set, means we are blocked
          if (src_offset == -1) {
            if (ioctl(fd, FIONREAD, &data_avail) < 0)
              return -EBADF;
          } else {
            struct stat f_status;
            if (fstat(fd, &f_status) < 0)
              return -EBADF;
            data_avail = f_status.st_size - src_offset;
          }
        }
      }

      if (((unsigned)data_avail >= (CEPH_PAGE_SIZE * 2)) && (to_move >= 4096)) {
        if (to_move > data_avail)
          to_move = data_avail;
        to_move = to_move & ~(CEPH_PAGE_SIZE - 1);

        if (src_offset == -1)
          r = splice(fd, nullptr, pipe_curr_write_fd, nullptr, to_move, SPLICE_F_NONBLOCK);
        else
        {
          loff_t offset = src_offset;
          r = splice(fd, &offset, pipe_curr_write_fd, nullptr, to_move, SPLICE_F_NONBLOCK);
        }
        if (r > 0) {
          pipe_read_elements.back().size += r;
          total_moved += r;
          dst_pos += r;
          actual_content_size += r;
          if (src_offset != -1)
            src_offset += r;
          count -= r;
        } else if (r == 0) {
          //something got closed...
          break;
        } else {
          //on linux, if you try to splice to pipe, you can get EAGAIN
          //if you started from offset that was unaligned, last page cannot be filled
          if (is_pipe_full(pipe_curr_write_fd)) {
            //it is just pipe blocked
            lclose(pipe_curr_write_fd);
            pipe_curr_write_fd = -1;
          } else {
            int e = -errno;
            if (e == -EAGAIN) {
              if (total_moved == 0)
                return e;
              else
                break;
            } else {
              lclose(pipe_curr_write_fd);
              pipe_curr_write_fd = -1;
              get_data();
              if (total_moved!=0)
                return total_moved;
              return buffer::raw::insert_from_fd(dst_pos, count, fd, src_offset);
            }
          }
        }
      } else {
        if (total_moved>0)
          return total_moved;

        //nothing moved; have to resort to read/write
        char buffer[CEPH_PAGE_SIZE*2];
        ssize_t r, w;
        if (data_avail < to_move)
          to_move = data_avail;
        //to_move is always <= 2*CEPH_PAGE_SIZE
        if (src_offset == -1) {
          r = read(fd, &buffer, to_move);
        } else {
          r = pread(fd, &buffer, to_move, src_offset);
        }
        if (r > 0) {
          total_moved += r;
          dst_pos += r;
          actual_content_size += r;
          if (src_offset != -1)
            src_offset += r;
          count -= r;
          w = write(pipe_curr_write_fd, &buffer, r);
          if (w == r) {
            pipe_read_elements.back().size += r;
          } else {
            if (w == -1) {
              w = 0;
            }
            lclose(pipe_curr_write_fd);
            append_pipe();
            ssize_t wf = write(pipe_curr_write_fd, &buffer + w, r - w);
            pipe_read_elements.back().size += wf;
            //we expect that in NEW pipe there will be enough space for CEPH_PAGE_SIZE*2
            assert(wf == r - w);
          }
        } else if (r==0) {
          return 0;
        } else {
          return -errno;
        }
      }
    }
   return total_moved;
  }

  buffer::raw* clone_empty() {
    assert(false&&"raw_splice_kcrypt::clone_empty should never be invoked");
    return nullptr;
  }

  buffer::raw* clone() {
    buffer::raw_splice_kcrypt* c;
    c = new raw_splice_kcrypt(len);
    if (data != nullptr) {
      c->data = new char[len];
      memcpy(c->data, data, len);
      return c;
    }

    ssize_t i;
    for (i=0; i<(ssize_t)pipe_read_elements.size(); i++) {
      int fds[2];
      if (pipe2(fds, O_NONBLOCK) == 0) {
        ssize_t moved_cnt;
        set_pipe_size(fds[write_end]);
        moved_cnt = tee(pipe_read_elements[i].fd, fds[write_end], pipe_read_elements[i].size, SPLICE_F_NONBLOCK);
        if (moved_cnt == (ssize_t)pipe_read_elements[i].size) {
          c->pipe_read_elements.emplace_back(moved_cnt, fds[read_end]);
          c->actual_content_size += moved_cnt;
          close(fds[write_end]);
        } else {
          close(fds[read_end]);
          close(fds[write_end]);
          goto cleanup_and_convert;
        }
      } else {
        goto cleanup_and_convert;
      }
    }
    return c;

    cleanup_and_convert:
    while (--i >= 0) {
      close(c->pipe_read_elements[i].fd);
    }
    get_data();
    c->data = new char[len];
    memcpy(c->data, data, len);
    return c;
  }

  char *get_data() {
    if (data)
      return data;

    if (pipe_curr_write_fd != -1) {
      lclose(pipe_curr_write_fd);
      pipe_curr_write_fd = -1;
    }
    data = new char[len];
    //not checking data, it will throw std::bad_alloc
    size_t pos = 0;
    for (auto& elem : pipe_read_elements) {
      assert(elem.fd >= 0 && "expected proper file descriptors");
      if (elem.fd < 0) continue;
      ssize_t r = safe_read(elem.fd, data + pos, len - pos);
      lclose(elem.fd);
      if (r >=0) {
        pos += r;
      } else {
        assert(0 && "unexpected error when reading pipes");
        //fallback for assertless compilation;
        //data will be garbled, but at least we cleanup properly
        continue;
      }
    }
    pipe_read_elements.resize(0);
    return data;
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

  ssize_t set_pipe_size(int pipe_fd) {
#ifdef CEPH_HAVE_SETPIPE_SZ
    int length;
    length = buffer_max_pipe_size.read();
    if (::fcntl(pipe_fd, F_SETPIPE_SZ, length) == -1) {
      int r = -errno;
      if (r == -EPERM) {
        // pipe limit must have changed - EPERM means we requested
        // more than the maximum size as an unprivileged user
        // let's try to rectify
        update_max_pipe_size();
        length = buffer_max_pipe_size.read();
        if (::fcntl(pipe_fd, F_SETPIPE_SZ, length) == -1)
          return -errno;
      }
      else
        return r;
    }
    if (::fcntl(pipe_fd, F_GETPIPE_SZ, length) == -1)
      return -errno;
    return length;
#endif
    return 0;
  }
};
#endif //CEPH_HAVE_SPLICE

  /*
   * primitive buffer types
   */
  class buffer::raw_char : public buffer::raw {
  public:
    MEMPOOL_CLASS_HELPERS();

    explicit raw_char(unsigned l) : raw(l) {
      if (len)
	data = mempool::buffer_data::alloc_char.allocate(len);
      else
	data = 0;
      inc_total_alloc(len);
      inc_history_alloc(len);
      bdout << "raw_char " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    raw_char(unsigned l, char *b) : raw(b, l) {
      inc_total_alloc(len);
      bdout << "raw_char " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    ~raw_char() override {
      if (data)
	mempool::buffer_data::alloc_char.deallocate(data, len);
      dec_total_alloc(len);
      bdout << "raw_char " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() override {
      return new raw_char(len);
    }
  };

  class buffer::raw_unshareable : public buffer::raw {
  public:
    MEMPOOL_CLASS_HELPERS();

    explicit raw_unshareable(unsigned l) : raw(l) {
      if (len)
	data = new char[len];
      else
	data = 0;
    }
    raw_unshareable(unsigned l, char *b) : raw(b, l) {
    }
    raw* clone_empty() override {
      return new raw_char(len);
    }
    bool is_shareable() override {
      return false; // !shareable, will force make_shareable()
    }
    ~raw_unshareable() override {
      delete[] data;
    }
  };

  class buffer::raw_static : public buffer::raw {
  public:
    MEMPOOL_CLASS_HELPERS();

    raw_static(const char *d, unsigned l) : raw((char*)d, l) { }
    ~raw_static() override {}
    raw* clone_empty() override {
      return new buffer::raw_char(len);
    }
  };

  class buffer::raw_claim_buffer : public buffer::raw {
    deleter del;
   public:
    raw_claim_buffer(const char *b, unsigned l, deleter d)
        : raw((char*)b, l), del(std::move(d)) { }
    ~raw_claim_buffer() override {}
    raw* clone_empty() override {
      return new buffer::raw_char(len);
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
      raw((char*)_mp->addr, l), mp(_mp)
    { }
    ~xio_mempool() {}
    raw* clone_empty() {
      return new buffer::raw_char(len);
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
    raw* r = buffer::create_aligned(len, sizeof(size_t));
    memcpy(r->get_data(), c, len);
    return r;
  }

  buffer::raw* buffer::create(unsigned len) {
    return buffer::create_aligned(len, sizeof(size_t));
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
  buffer::raw* buffer::claim_buffer(unsigned len, char *buf, deleter del) {
    return new raw_claim_buffer(buf, len, std::move(del));
  }

  buffer::raw* buffer::create_aligned(unsigned len, unsigned align) {
    // If alignment is a page multiple, use a separate buffer::raw to
    // avoid fragmenting the heap.
    //
    // Somewhat unexpectedly, I see consistently better performance
    // from raw_combined than from raw even when the allocation size is
    // a page multiple (but alignment is not).
    //
    // I also see better performance from a separate buffer::raw once the
    // size passes 8KB.
    if ((align & ~CEPH_PAGE_MASK) == 0 ||
	len >= CEPH_PAGE_SIZE * 2) {
#ifndef __CYGWIN__
      return new raw_posix_aligned(len, align);
#else
      return new raw_hack_aligned(len, align);
#endif
    }
    return raw_combined::create(len, align);
  }

  buffer::raw* buffer::create_page_aligned(unsigned len) {
    return create_aligned(len, CEPH_PAGE_SIZE);
  }

  /*
   * Create zero-copy bufferlist.
   * If not supported, fallback to page-aligned variant.
   *
   * After creation buffer is ready to perform \ref insert_from_fd.
   *
   * \param capacity size of buffer
   */
  buffer::raw* buffer::create_zero_copy(size_t capacity) {
#ifdef CEPH_HAVE_SPLICE
    return new raw_splice_kcrypt(capacity);
#else
    return create_page_aligned(capacity);
#endif
  }

  /*
   * Create zero-copy bufferlist and immediatelly fill from file.
   * If zero-copy not supported, fallback to page-aligned variant.
   *
   * After creation buffer is ready to perform \ref insert_from_fd.
   *
   * \param count size of buffer to create and fill
   * \param fd file to read from
   * \param offset position in \ref fd to read from. Value -1 means to read from current file offset.
   */
  buffer::raw* buffer::create_zero_copy(size_t count, int fd, off_t offset) {
#ifdef CEPH_HAVE_SPLICE
    buffer::raw* buf = new raw_splice_kcrypt(count);
#else
    buffer::raw* buf = new create_page_aligned(count);
#endif
    ssize_t r;
    size_t dst_ofs = 0;
    while (dst_ofs < count)
    {
      r = buf->insert_from_fd(dst_ofs, count - dst_ofs, fd, offset);
      if (r>0) {
        dst_ofs += r;
        if (offset !=-1) {
          offset += r;
        }
        continue;
      }

      if (r==-EAGAIN) continue;
      if (r<=0)
        throw error_code(-EINVAL);
    }
    return buf;
  }

  buffer::raw* buffer::create_unshareable(unsigned len) {
    return new raw_unshareable(len);
  }

  buffer::ptr::ptr(raw *r) : _raw(r), _off(0), _len(r->len)   // no lock needed; this is an unref raw.
  {
    r->nref.inc();
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(unsigned l) : _off(0), _len(l)
  {
    _raw = create(l);
    _raw->nref.inc();
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(const char *d, unsigned l) : _off(0), _len(l)    // ditto.
  {
    _raw = copy(d, l);
    _raw->nref.inc();
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(const ptr& p) : _raw(p._raw), _off(p._off), _len(p._len)
  {
    if (_raw) {
      _raw->nref.inc();
      bdout << "ptr " << this << " get " << _raw << bendl;
    }
  }
  buffer::ptr::ptr(ptr&& p) noexcept : _raw(p._raw), _off(p._off), _len(p._len)
  {
    p._raw = nullptr;
    p._off = p._len = 0;
  }
  buffer::ptr::ptr(const ptr& p, unsigned o, unsigned l)
    : _raw(p._raw), _off(p._off + o), _len(l)
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
  buffer::ptr& buffer::ptr::operator= (ptr&& p) noexcept
  {
    release();
    buffer::raw *raw = p._raw;
    if (raw) {
      _raw = raw;
      _off = p._off;
      _len = p._len;
      p._raw = nullptr;
      p._off = p._len = 0;
    } else {
      _off = _len = 0;
    }
    return *this;
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
        ANNOTATE_HAPPENS_AFTER(&tr->nref);
        ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&tr->nref);
        delete tr;
      } else {
        ANNOTATE_HAPPENS_BEFORE(&tr->nref);
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
      if (_raw->nref.dec() == 0) {
	//cout << "hosing raw " << (void*)_raw << " len " << _raw->len << std::endl;
        ANNOTATE_HAPPENS_AFTER(&_raw->nref);
        ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&_raw->nref);
	delete _raw;  // dealloc old (if any)
      } else {
        ANNOTATE_HAPPENS_BEFORE(&_raw->nref);
      }
      _raw = 0;
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
  const char *buffer::ptr::end_c_str() const {
    assert(_raw);
    if (buffer_track_c_str)
      buffer_c_str_accesses.inc();
    return _raw->get_data() + _off + _len;
  }
  char *buffer::ptr::end_c_str() {
    assert(_raw);
    if (buffer_track_c_str)
      buffer_c_str_accesses.inc();
    return _raw->get_data() + _off + _len;
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

  const char *buffer::ptr::raw_c_str() const { assert(_raw); return _raw->get_data(); }
  unsigned buffer::ptr::raw_length() const { assert(_raw); return _raw->len; }
  int buffer::ptr::raw_nref() const { assert(_raw); return _raw->nref.read(); }

  uint32_t buffer::ptr::crc32c(uint32_t crc) const {
    if (_raw == nullptr) return crc;
    return _raw->crc32c(_off,_len, crc);
  }
  void buffer::ptr::copy_out(unsigned o, unsigned l, char *dest) const {
    assert(_raw);
    if (o+l > _len)
        throw end_of_buffer();
    char* src =  _raw->get_data() + _off + o;
    maybe_inline_memcpy(dest, src, l, 8);
  }

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
    return mem_is_zero(c_str(), _len);
  }

  unsigned buffer::ptr::append(char c)
  {
    assert(_raw);
    assert(1 <= unused_tail_length());
    char* ptr = _raw->get_data() + _off + _len;
    *ptr = c;
    _len++;
    return _len + _off;
  }

  unsigned buffer::ptr::append(const char *p, unsigned l)
  {
    assert(_raw);
    assert(l <= unused_tail_length());
    char* c = _raw->get_data() + _off + _len;
    maybe_inline_memcpy(c, p, l, 32);
    _len += l;
    return _len + _off;
  }

  void buffer::ptr::copy_in(unsigned o, unsigned l, const char *src)
  {
    copy_in(o, l, src, true);
  }

  void buffer::ptr::copy_in(unsigned o, unsigned l, const char *src, bool crc_reset)
  {
    assert(_raw);
    assert(o <= _len);
    assert(o+l <= _len);
    char* dest = _raw->get_data() + _off + o;
    if (crc_reset)
        _raw->invalidate_crc();
    maybe_inline_memcpy(dest, src, l, 64);
  }

  void buffer::ptr::zero()
  {
    zero(true);
  }

  void buffer::ptr::zero(bool crc_reset)
  {
    if (crc_reset)
        _raw->invalidate_crc();
    memset(c_str(), 0, _len);
  }

  void buffer::ptr::zero(unsigned o, unsigned l)
  {
    zero(o, l, true);
  }

  void buffer::ptr::zero(unsigned o, unsigned l, bool crc_reset)
  {
    assert(o+l <= _len);
    if (crc_reset)
        _raw->invalidate_crc();
    memset(c_str()+o, 0, l);
  }
  bool buffer::ptr::is_data_local() const
  {
    return _raw->is_data_local();
  }

  ssize_t buffer::ptr::copy_to_fd(int fd, off_t ofs_from, ssize_t len, off_t dst_offset) const
  {
    return _raw->copy_to_fd(fd, ofs_from, len, dst_offset);
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

  template<bool is_const>
  buffer::list::iterator_impl<is_const>::iterator_impl(bl_t *l, unsigned o)
    : bl(l), ls(&bl->_buffers), off(0), p(ls->begin()), p_off(0)
  {
    advance(o);
  }

  template<bool is_const>
  buffer::list::iterator_impl<is_const>::iterator_impl(const buffer::list::iterator& i)
    : iterator_impl<is_const>(i.bl, i.off, i.p, i.p_off) {}

  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::advance(int o)
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
	  p++;
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
	p--;
	p_off = p->length();
      } else {
	throw end_of_buffer();
      }
    }
  }

  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::seek(unsigned o)
  {
    p = ls->begin();
    off = p_off = 0;
    advance(o);
  }

  template<bool is_const>
  char buffer::list::iterator_impl<is_const>::operator*() const
  {
    if (p == ls->end())
      throw end_of_buffer();
    return (*p)[p_off];
  }

  template<bool is_const>
  buffer::list::iterator_impl<is_const>&
  buffer::list::iterator_impl<is_const>::operator++()
  {
    if (p == ls->end())
      throw end_of_buffer();
    advance(1);
    return *this;
  }

  template<bool is_const>
  buffer::ptr buffer::list::iterator_impl<is_const>::get_current_ptr() const
  {
    if (p == ls->end())
      throw end_of_buffer();
    return ptr(*p, p_off, p->length() - p_off);
  }

  // copy data out.
  // note that these all _append_ to dest!
  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::copy(unsigned len, char *dest)
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

  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::copy(unsigned len, ptr &dest)
  {
    copy_deep(len, dest);
  }

  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::copy_deep(unsigned len, ptr &dest)
  {
    if (!len) {
      return;
    }
    if (p == ls->end())
      throw end_of_buffer();
    assert(p->length() > 0);
    dest = create(len);
    copy(len, dest.c_str());
  }
  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::copy_shallow(unsigned len,
							   ptr &dest)
  {
    if (!len) {
      return;
    }
    if (p == ls->end())
      throw end_of_buffer();
    assert(p->length() > 0);
    unsigned howmuch = p->length() - p_off;
    if (howmuch < len) {
      dest = create(len);
      copy(len, dest.c_str());
    } else {
      dest = ptr(*p, p_off, len);
      advance(len);
    }
  }

  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::copy(unsigned len, list &dest)
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

  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::copy(unsigned len, std::string &dest)
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

  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::copy_all(list &dest)
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

  template<bool is_const>
  ssize_t buffer::list::iterator_impl<is_const>::copy(unsigned len, int fd, off_t dst_offset)
  {
    ssize_t total_written = 0;
    while (len > 0) {
      if (p == ls->end())
        throw end_of_buffer();

      unsigned howmuch = p->length() - p_off;
      if (len < howmuch)
        howmuch = len;
      ssize_t r = p->copy_to_fd(fd, p_off, howmuch, dst_offset);
      if (r>0)
      {
        len -= r;
        advance(r);
        total_written += r;
        if (dst_offset == -1)
          dst_offset += r;
      }
      else
      {
        if (total_written > 0)
          return total_written;
        else
          return r;
      }
    }
    return total_written;
  }


  template<bool is_const>
  size_t buffer::list::iterator_impl<is_const>::get_ptr_and_advance(
    size_t want, const char **data)
  {
    if (p == ls->end()) {
      seek(off);
      if (p == ls->end()) {
	return 0;
      }
    }
    *data = p->c_str() + p_off;
    size_t l = MIN(p->length() - p_off, want);
    p_off += l;
    if (p_off == p->length()) {
      ++p;
      p_off = 0;
    }
    off += l;
    return l;
  }

  template<bool is_const>
  uint32_t buffer::list::iterator_impl<is_const>::crc32c(
    size_t length, uint32_t crc)
  {
    length = MIN( length, get_remaining());
    while (length > 0) {
      const char *p;
      size_t l = get_ptr_and_advance(length, &p);
      crc = ceph_crc32c(crc, (unsigned char*)p, l);
      length -= l;
    }
    return crc;
  }

  // explicitly instantiate only the iterator types we need, so we can hide the
  // details in this compilation unit without introducing unnecessary link time
  // dependencies.
  template class buffer::list::iterator_impl<true>;
  template class buffer::list::iterator_impl<false>;

  buffer::list::iterator::iterator(bl_t *l, unsigned o)
    : iterator_impl(l, o)
  {}

  buffer::list::iterator::iterator(bl_t *l, unsigned o, list_iter_t ip, unsigned po)
    : iterator_impl(l, o, ip, po)
  {}

  void buffer::list::iterator::advance(int o)
  {
    buffer::list::iterator_impl<false>::advance(o);
  }

  void buffer::list::iterator::seek(unsigned o)
  {
    buffer::list::iterator_impl<false>::seek(o);
  }

  char buffer::list::iterator::operator*()
  {
    if (p == ls->end()) {
      throw end_of_buffer();
    }
    return (*p)[p_off];
  }

  buffer::list::iterator& buffer::list::iterator::operator++()
  {
    buffer::list::iterator_impl<false>::operator++();
    return *this;
  }

  buffer::ptr buffer::list::iterator::get_current_ptr()
  {
    if (p == ls->end()) {
      throw end_of_buffer();
    }
    return ptr(*p, p_off, p->length() - p_off);
  }

  void buffer::list::iterator::copy(unsigned len, char *dest)
  {
    return buffer::list::iterator_impl<false>::copy(len, dest);
  }

  void buffer::list::iterator::copy(unsigned len, ptr &dest)
  {
    return buffer::list::iterator_impl<false>::copy_deep(len, dest);
  }

  void buffer::list::iterator::copy_deep(unsigned len, ptr &dest)
  {
    buffer::list::iterator_impl<false>::copy_deep(len, dest);
  }

  void buffer::list::iterator::copy_shallow(unsigned len, ptr &dest)
  {
    buffer::list::iterator_impl<false>::copy_shallow(len, dest);
  }

  void buffer::list::iterator::copy(unsigned len, list &dest)
  {
    buffer::list::iterator_impl<false>::copy(len, dest);
  }

  void buffer::list::iterator::copy(unsigned len, std::string &dest)
  {
    buffer::list::iterator_impl<false>::copy(len, dest);
  }

  ssize_t buffer::list::iterator::copy(unsigned len, int fd, off_t dst_offset)
  {
    return buffer::list::iterator_impl<false>::copy(len, fd, dst_offset);
  }

  void buffer::list::iterator::copy_all(list &dest)
  {
    buffer::list::iterator_impl<false>::copy_all(dest);
  }

  void buffer::list::iterator::copy_in(unsigned len, const char *src)
  {
    copy_in(len, src, true);
  }

  // copy data in
  void buffer::list::iterator::copy_in(unsigned len, const char *src, bool crc_reset)
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
      p->copy_in(p_off, howmuch, src, crc_reset);
	
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
    for (std::list<ptr>::const_iterator i = otherl._buffers.begin();
	 i != otherl._buffers.end();
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

  buffer::list::list(list&& other)
    : _buffers(std::move(other._buffers)),
      _len(other._len),
      _memcopy_count(other._memcopy_count),
      last_p(this) {
    append_buffer.swap(other.append_buffer);
    other.clear();
  }

  void buffer::list::swap(list& other)
  {
    std::swap(_len, other._len);
    std::swap(_memcopy_count, other._memcopy_count);
    _buffers.swap(other._buffers);
    append_buffer.swap(other.append_buffer);
    //last_p.swap(other.last_p);
    last_p = begin();
    other.last_p = other.begin();
  }

  bool buffer::list::contents_equal(buffer::list& other)
  {
    return static_cast<const buffer::list*>(this)->contents_equal(other);
  }

  bool buffer::list::contents_equal(const ceph::buffer::list& other) const
  {
    if (length() != other.length())
      return false;

    // buffer-wise comparison
    if (true) {
      std::list<ptr>::const_iterator a = _buffers.begin();
      std::list<ptr>::const_iterator b = other._buffers.begin();
      unsigned aoff = 0, boff = 0;
      while (a != _buffers.end()) {
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
      assert(b == other._buffers.end());
      return true;
    }

    // byte-wise comparison
    if (false) {
      bufferlist::const_iterator me = begin();
      bufferlist::const_iterator him = other.begin();
      while (!me.end()) {
	if (*me != *him)
	  return false;
	++me;
	++him;
      }
      return true;
    }
  }

  /*
   * Reformulated aggregation.
   * Now all buffers have to be local to return true
   */
  bool buffer::list::is_data_local() const
  {
    for (std::list<ptr>::const_iterator it = _buffers.begin();
        it != _buffers.end();
        ++it)
      if (!it->is_data_local())
        return false;
    return true;
  }

  bool buffer::list::is_provided_buffer(const char *dst) const
  {
    if (_buffers.empty())
      return false;
    return (is_contiguous() && (_buffers.front().c_str() == dst));
  }

  bool buffer::list::is_aligned(unsigned align) const
  {
    for (std::list<ptr>::const_iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it) 
      if (!it->is_aligned(align))
	return false;
    return true;
  }

  bool buffer::list::is_n_align_sized(unsigned align) const
  {
    for (std::list<ptr>::const_iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it) 
      if (!it->is_n_align_sized(align))
	return false;
    return true;
  }

  bool buffer::list::is_aligned_size_and_memory(unsigned align_size,
						  unsigned align_memory) const
  {
    for (std::list<ptr>::const_iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it) {
      if (!it->is_aligned(align_memory) || !it->is_n_align_sized(align_size))
	return false;
    }
    return true;
  }

  bool buffer::list::is_zero() const {
    for (std::list<ptr>::const_iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it) {
      if (!it->is_zero()) {
	return false;
      }
    }
    return true;
  }

  void buffer::list::zero()
  {
    for (std::list<ptr>::iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it)
      it->zero();
  }

  void buffer::list::zero(unsigned o, unsigned l)
  {
    assert(o+l <= _len);
    unsigned p = 0;
    for (std::list<ptr>::iterator it = _buffers.begin();
	 it != _buffers.end();
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

  bool buffer::list::is_contiguous() const
  {
    return &(*_buffers.begin()) == &(*_buffers.rbegin());
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
    if (_len == 0) {
      _buffers.clear();
      return;
    }
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
    for (std::list<ptr>::iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it) {
      nb.copy_in(pos, it->length(), it->c_str(), false);
      pos += it->length();
    }
    _memcopy_count += pos;
    _buffers.clear();
    if (nb.length())
      _buffers.push_back(nb);
    invalidate_crc();
    last_p = begin();
  }

  bool buffer::list::rebuild_aligned(unsigned align)
  {
    return rebuild_aligned_size_and_memory(align, align);
  }
  
  bool buffer::list::rebuild_aligned_size_and_memory(unsigned align_size,
  						   unsigned align_memory)
  {
    unsigned old_memcopy_count = _memcopy_count;
    std::list<ptr>::iterator p = _buffers.begin();
    while (p != _buffers.end()) {
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
        _buffers.erase(p++);
      } while (p != _buffers.end() &&
  	     (!p->is_aligned(align_memory) ||
  	      !p->is_n_align_sized(align_size) ||
  	      (offset % align_size)));
      if (!(unaligned.is_contiguous() && unaligned._buffers.front().is_aligned(align_memory))) {
        ptr nb(buffer::create_aligned(unaligned._len, align_memory));
        unaligned.rebuild(nb);
        _memcopy_count += unaligned._len;
      }
      _buffers.insert(p, unaligned._buffers.front());
    }
    last_p = begin();

    return  (old_memcopy_count != _memcopy_count);
  }
  
  bool buffer::list::rebuild_page_aligned()
  {
   return  rebuild_aligned(CEPH_PAGE_SIZE);
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
    _buffers.splice(_buffers.end(), bl._buffers );
    bl._len = 0;
    bl.last_p = bl.begin();
  }

  void buffer::list::claim_prepend(list& bl, unsigned int flags)
  {
    // steal the other guy's buffers
    _len += bl._len;
    if (!(flags & CLAIM_ALLOW_NONSHAREABLE))
      bl.make_shareable();
    _buffers.splice(_buffers.begin(), bl._buffers );
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
    copy_in(off, len, src, true);
  }

  void buffer::list::copy_in(unsigned off, unsigned len, const char *src, bool crc_reset)
  {
    if (off + len > length())
      throw end_of_buffer();
    
    if (last_p.get_off() != off) 
      last_p.seek(off);
    last_p.copy_in(len, src, crc_reset);
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
      append_buffer = raw_combined::create(CEPH_BUFFER_APPEND_SIZE);
      append_buffer.set_length(0);   // unused, so far.
    }
    append(append_buffer, append_buffer.append(c) - 1, 1);	// add segment to the list
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
        append(append_buffer, append_buffer.length() - gap, gap);	// add segment to the list
        len -= gap;
        data += gap;
      }
      if (len == 0)
        break;  // done!
      
      // make a new append_buffer.  fill out a complete page, factoring in the
      // raw_combined overhead.
      size_t need = ROUND_UP_TO(len, sizeof(size_t)) + sizeof(raw_combined);
      size_t alen = ROUND_UP_TO(need, CEPH_BUFFER_ALLOC_UNIT) -
	sizeof(raw_combined);
      append_buffer = raw_combined::create(alen);
      append_buffer.set_length(0);   // unused, so far.
    }
  }

  void buffer::list::append(const ptr& bp)
  {
    if (bp.length())
      push_back(bp);
  }

  void buffer::list::append(ptr&& bp)
  {
    if (bp.length())
      push_back(std::move(bp));
  }

  void buffer::list::append(const ptr& bp, unsigned off, unsigned len)
  {
    assert(len+off <= bp.length());
    if (!_buffers.empty()) {
      ptr &l = _buffers.back();
      if (l.get_raw() == bp.get_raw() &&
	  l.end() == bp.start() + off) {
	// yay contiguous with tail bp!
	l.set_length(l.length()+len);
	_len += len;
	return;
      }
    }
    // add new item to list
    push_back(ptr(bp, off, len));
  }

  void buffer::list::append(const list& bl)
  {
    _len += bl._len;
    for (std::list<ptr>::const_iterator p = bl._buffers.begin();
	 p != bl._buffers.end();
	 ++p) 
      _buffers.push_back(*p);
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

  void buffer::list::prepend_zero(unsigned len)
  {
    ptr bp(len);
    bp.zero(false);
    _len += len;
    _buffers.emplace_front(std::move(bp));
  }
  
  void buffer::list::append_zero(unsigned len)
  {
    ptr bp(len);
    bp.zero(false);
    append(std::move(bp));
  }

  
  /*
   * get a char
   */
  const char& buffer::list::operator[](unsigned n) const
  {
    if (n >= _len)
      throw end_of_buffer();
    
    for (std::list<ptr>::const_iterator p = _buffers.begin();
	 p != _buffers.end();
	 ++p) {
      if (n >= p->length()) {
	n -= p->length();
	continue;
      }
      return (*p)[n];
    }
    ceph_abort();
  }

  /*
   * return a contiguous ptr to whole bufferlist contents.
   */
  char *buffer::list::c_str()
  {
    if (_buffers.empty())
      return 0;                         // no buffers

    std::list<ptr>::const_iterator iter = _buffers.begin();
    ++iter;

    if (iter != _buffers.end())
      rebuild();
    return _buffers.front().c_str();  // good, we're already contiguous.
  }

  string buffer::list::to_str() const {
    string s;
    s.reserve(length());
    for (std::list<ptr>::const_iterator p = _buffers.begin();
	 p != _buffers.end();
	 ++p) {
      if (p->length()) {
	s.append(p->c_str(), p->length());
      }
    }
    return s;
  }

  char *buffer::list::get_contiguous(unsigned orig_off, unsigned len)
  {
    if (orig_off + len > length())
      throw end_of_buffer();

    if (len == 0) {
      return 0;
    }

    unsigned off = orig_off;
    std::list<ptr>::iterator curbuf = _buffers.begin();
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
	tmp.append(*curbuf);
	curbuf = _buffers.erase(curbuf);

      } while (curbuf != _buffers.end() && l > 0);

      assert(l == 0);

      tmp.rebuild();
      _buffers.insert(curbuf, tmp._buffers.front());
      return tmp.c_str() + off;
    }

    last_p = begin();  // we modified _buffers

    return curbuf->c_str() + off;
  }

  void buffer::list::substr_of(const list& other, unsigned off, unsigned len)
  {
    if (off + len > other.length())
      throw end_of_buffer();

    clear();

    // skip off
    std::list<ptr>::const_iterator curbuf = other._buffers.begin();
    while (off > 0 &&
	   off >= curbuf->length()) {
      // skip this buffer
      //cout << "skipping over " << *curbuf << std::endl;
      off -= (*curbuf).length();
      ++curbuf;
    }
    assert(len == 0 || curbuf != other._buffers.end());
    
    while (len > 0) {
      // partial?
      if (off + len < curbuf->length()) {
	//cout << "copying partial of " << *curbuf << std::endl;
	_buffers.push_back( ptr( *curbuf, off, len ) );
	_len += len;
	break;
      }
      
      // through end
      //cout << "copying end (all?) of " << *curbuf << std::endl;
      unsigned howmuch = curbuf->length() - off;
      _buffers.push_back( ptr( *curbuf, off, howmuch ) );
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
    //cout << "splice off " << off << " len " << len << " ... mylen = " << length() << std::endl;
      
    // skip off
    std::list<ptr>::iterator curbuf = _buffers.begin();
    while (off > 0) {
      assert(curbuf != _buffers.end());
      if (off >= (*curbuf).length()) {
	// skip this buffer
	//cout << "off = " << off << " skipping over " << *curbuf << std::endl;
	off -= (*curbuf).length();
	++curbuf;
      } else {
	// somewhere in this buffer!
	//cout << "off = " << off << " somewhere in " << *curbuf << std::endl;
	break;
      }
    }
    
    if (off) {
      // add a reference to the front bit
      //  insert it before curbuf (which we'll hose)
      //cout << "keeping front " << off << " of " << *curbuf << std::endl;
      _buffers.insert( curbuf, ptr( *curbuf, 0, off ) );
      _len += off;
    }
    
    while (len > 0) {
      // partial?
      if (off + len < (*curbuf).length()) {
	//cout << "keeping end of " << *curbuf << ", losing first " << off+len << std::endl;
	if (claim_by) 
	  claim_by->append( *curbuf, off, len );
	(*curbuf).set_offset( off+len + (*curbuf).offset() );    // ignore beginning big
	(*curbuf).set_length( (*curbuf).length() - (len+off) );
	_len -= off+len;
	//cout << " now " << *curbuf << std::endl;
	break;
      }
      
      // hose though the end
      unsigned howmuch = (*curbuf).length() - off;
      //cout << "discarding " << howmuch << " of " << *curbuf << std::endl;
      if (claim_by) 
	claim_by->append( *curbuf, off, howmuch );
      _len -= (*curbuf).length();
      _buffers.erase( curbuf++ );
      len -= howmuch;
      off = 0;
    }
      
    // splice in *replace (implement me later?)
    
    last_p = begin();  // just in case we were in the removed region.
  }

  void buffer::list::write(int off, int len, std::ostream& out) const
  {
    list s;
    s.substr_of(*this, off, len);
    for (std::list<ptr>::const_iterator it = s._buffers.begin(); 
	 it != s._buffers.end(); 
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
  o.push_back(std::move(bp));
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
  push_back(std::move(bp));
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
  if (::fstat(fd, &st) < 0) {
    int err = errno;
    std::ostringstream oss;
    oss << "bufferlist::read_file(" << fn << "): stat error: "
        << cpp_strerror(err);
    *error = oss.str();
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return -err;
  }

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
  if (fd < 0) 
    return -EBADF;
  // try zero copy first
  if (len >= CEPH_PAGE_SIZE * 4)
  {
    try {
      buffer::raw* x = buffer::create_zero_copy(len);
      size_t res = 0;
      int r;
      do {
	r = x->insert_from_fd(res, len-res, fd);
	if (r == -EAGAIN) { continue; }
	if (r <= 0) break;
	if (r >= 0) 
	  res += r;
      } while (res < len);
      buffer::ptr p(x);
      p.set_length(res);
      append(p);
      return res;
    } catch (buffer::error_code &e) {
      return e.code;
    } catch (buffer::malformed_input &e) {
      return -EIO;
    }
    return 0;
  }
  bufferptr bp = buffer::create(len);
  ssize_t ret = safe_read(fd, (void*)bp.c_str(), len);
  if (ret >= 0) {
    bp.set_length(ret);
    append(std::move(bp));
  }
  return ret;
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

static int do_writev(int fd, struct iovec *vec, uint64_t offset, unsigned veclen, unsigned bytes)
{
  ssize_t r = 0;
  while (bytes > 0) {
#ifdef HAVE_PWRITEV
    r = ::pwritev(fd, vec, veclen, offset);
#else
    r = ::lseek64(fd, offset, SEEK_SET);
    if (r != offset) {
      r = -errno;
      return r;
    }
    r = ::writev(fd, vec, veclen);
#endif
    if (r < 0) {
      if (errno == EINTR)
        continue;
      return -errno;
    }

    bytes -= r;
    offset += r;
    if (bytes == 0) break;

    while (r > 0) {
      if (vec[0].iov_len <= (size_t)r) {
        // drain this whole item
        r -= vec[0].iov_len;
        ++vec;
        --veclen;
      } else {
        vec[0].iov_base = (char *)vec[0].iov_base + r;
        vec[0].iov_len -= r;
        break;
      }
    }
  }
  return 0;
}

int buffer::list::write_fd(int fd) const
{
  if (!is_data_local()) {
    std::list<ptr>::const_iterator p = _buffers.begin();
    while (p != _buffers.end()){
      int r;
      ssize_t pos = 0;
      while (pos < p->length()) {
        r = p->copy_to_fd(fd, pos, p->length()-pos);
        if (r>0)
          pos += r;
        if (r==-1) {
          //tiny sleep to prevent hard spin
          usleep(1000);
        }
      }
    }
    return 0;
  }

  // use writev!
  iovec iov[IOV_MAX];
  int iovlen = 0;
  ssize_t bytes = 0;

  std::list<ptr>::const_iterator p = _buffers.begin();
  while (p != _buffers.end()) {
    if (p->length() > 0) {
      iov[iovlen].iov_base = (void *)p->c_str();
      iov[iovlen].iov_len = p->length();
      bytes += p->length();
      iovlen++;
    }
    ++p;

    if (iovlen == IOV_MAX-1 ||
	p == _buffers.end()) {
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

int buffer::list::write_fd(int fd, uint64_t offset) const
{
  iovec iov[IOV_MAX];

  std::list<ptr>::const_iterator p = _buffers.begin();
  uint64_t left_pbrs = _buffers.size();
  while (left_pbrs) {
    ssize_t bytes = 0;
    unsigned iovlen = 0;
    uint64_t size = MIN(left_pbrs, IOV_MAX);
    left_pbrs -= size;
    while (size > 0) {
      iov[iovlen].iov_base = (void *)p->c_str();
      iov[iovlen].iov_len = p->length();
      iovlen++;
      bytes += p->length();
      ++p;
      size--;
    }

    int r = do_writev(fd, iov, offset, iovlen, bytes);
    if (r < 0)
      return r;
    offset += bytes;
  }
  return 0;
}

void buffer::list::prepare_iov(std::vector<iovec> *piov) const
{
  assert(_buffers.size() <= IOV_MAX);
  piov->resize(_buffers.size());
  unsigned n = 0;
  for (std::list<buffer::ptr>::const_iterator p = _buffers.begin();
       p != _buffers.end();
       ++p, ++n) {
    (*piov)[n].iov_base = (void *)p->c_str();
    (*piov)[n].iov_len = p->length();
  }
}


__u32 buffer::list::crc32c(__u32 crc) const
{
  for (std::list<ptr>::const_iterator it = _buffers.begin();
       it != _buffers.end();
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
	crc = it->crc32c(crc);
      }
    }
  }
  return crc;
}

void buffer::list::invalidate_crc()
{
  for (std::list<ptr>::const_iterator p = _buffers.begin(); p != _buffers.end(); ++p) {
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
  for (std::list<ptr>::const_iterator p = _buffers.begin(); p != _buffers.end(); ++p) {
    if (p->length() > 0) {
      out.write(p->c_str(), p->length());
    }
  }
}


void buffer::list::hexdump(std::ostream &out, bool trailing_newline) const
{
  if (!length())
    return;

  std::ios_base::fmtflags original_flags = out.flags();

  // do our best to match the output of hexdump -C, for better
  // diff'ing!

  out.setf(std::ios::right);
  out.fill('0');

  unsigned per = 16;
  bool was_zeros = false, did_star = false;
  for (unsigned o=0; o<length(); o += per) {
    bool row_is_zeros = false;
    if (o + per < length()) {
      row_is_zeros = true;
      for (unsigned i=0; i<per && o+i<length(); i++) {
	if ((*this)[o+i]) {
	  row_is_zeros = false;
	}
      }
      if (row_is_zeros) {
	if (was_zeros) {
	  if (!did_star) {
	    out << "\n*";
	    did_star = true;
	  }
	  continue;
	}
	was_zeros = true;
      } else {
	was_zeros = false;
	did_star = false;
      }
    }
    if (o)
      out << "\n";
    out << std::hex << std::setw(8) << o << " ";

    unsigned i;
    for (i=0; i<per && o+i<length(); i++) {
      if (i == 8)
	out << ' ';
      out << " " << std::setw(2) << ((unsigned)(*this)[o+i] & 0xff);
    }
    for (; i<per; i++) {
      if (i == 8)
	out << ' ';
      out << "   ";
    }
    
    out << "  |";
    for (i=0; i<per && o+i<length(); i++) {
      char c = (*this)[o+i];
      if (isupper(c) || islower(c) || isdigit(c) || c == ' ' || ispunct(c))
	out << c;
      else
	out << '.';
    }
    out << '|' << std::dec;
  }
  if (trailing_newline) {
    out << "\n" << std::hex << std::setw(8) << length();
    out << "\n";
  }

  out.flags(original_flags);
}

std::ostream& buffer::operator<<(std::ostream& out, const buffer::raw &r) {
  const char* rd = const_cast<buffer::raw&>(r).get_data();
  return out << "buffer::raw(" << (void*)rd << 
    " len " << r.len << " nref " << r.nref.read() << ")";
  
}

std::ostream& buffer::operator<<(std::ostream& out, const buffer::ptr& bp) {
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

std::ostream& buffer::operator<<(std::ostream& out, const buffer::list& bl) {
  out << "buffer::list(len=" << bl.length() << "," << std::endl;

  std::list<buffer::ptr>::const_iterator it = bl.buffers().begin();
  while (it != bl.buffers().end()) {
    out << "\t" << *it;
    if (++it == bl.buffers().end()) break;
    out << "," << std::endl;
  }
  out << std::endl << ")";
  return out;
}

std::ostream& buffer::operator<<(std::ostream& out, const buffer::error& e)
{
  return out << e.what();
}

MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_malloc, buffer_raw_malloc,
			      buffer_meta);
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_mmap_pages, buffer_raw_mmap_pagse,
			      buffer_meta);
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_posix_aligned,
			      buffer_raw_posix_aligned, buffer_meta);
#ifdef CEPH_HAVE_SPLICE
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_splice_kcrypt, buffer_raw_splice_kcrypt, buffer_meta);
#endif
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_char, buffer_raw_char, buffer_meta);
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_unshareable, buffer_raw_unshareable,
			      buffer_meta);
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_static, buffer_raw_static,
			      buffer_meta);

