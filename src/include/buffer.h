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
#ifndef CEPH_BUFFER_H
#define CEPH_BUFFER_H

#if defined(__linux__) || defined(__FreeBSD__)
#include <stdlib.h>
#endif

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 600
#endif

#include <stdio.h>
#include <sys/uio.h>

#if defined(__linux__)	// For malloc(2).
#include <malloc.h>
#endif

#include <inttypes.h>
#include <stdint.h>
#include <string.h>

#ifndef __CYGWIN__
# include <sys/mman.h>
#endif

#include <iosfwd>
#include <iomanip>
#include <list>
#include <vector>
#include <string>
#include <exception>
#include <type_traits>

#include "page.h"
#include "crc32c.h"
#include "buffer_fwd.h"

#ifdef __CEPH__
# include "include/assert.h"
#else
# include <assert.h>
#endif

#if __GNUC__ >= 4
  #define CEPH_BUFFER_API  __attribute__ ((visibility ("default")))
  #define CEPH_BUFFER_DETAILS __attribute__ ((visibility ("hidden")))
#else
  #define CEPH_BUFFER_API
  #define CEPH_BUFFER_DETAILS
#endif

#if defined(HAVE_XIO)
struct xio_reg_mem;
class XioDispatchHook;
#endif

namespace ceph {

namespace buffer CEPH_BUFFER_API {
  /*
   * exceptions
   */

  struct error : public std::exception{
    const char *what() const throw ();
  };
  struct bad_alloc : public error {
    const char *what() const throw ();
  };
  struct end_of_buffer : public error {
    const char *what() const throw ();
  };
  struct malformed_input : public error {
    explicit malformed_input(const std::string& w) {
      snprintf(buf, sizeof(buf), "buffer::malformed_input: %s", w.c_str());
    }
    const char *what() const throw ();
  private:
    char buf[256];
  };
  struct error_code : public malformed_input {
    explicit error_code(int error);
    int code;
  };


  /// total bytes allocated
  int get_total_alloc();

  /// history total bytes allocated
  uint64_t get_history_alloc_bytes();

  /// total num allocated
  uint64_t get_history_alloc_num();

  /// enable/disable alloc tracking
  void track_alloc(bool b);

  /// count of cached crc hits (matching input)
  int get_cached_crc();
  /// count of cached crc hits (mismatching input, required adjustment)
  int get_cached_crc_adjusted();
  /// enable/disable tracking of cached crcs
  void track_cached_crc(bool b);

  /// count of calls to buffer::ptr::c_str()
  int get_c_str_accesses();
  /// enable/disable tracking of buffer::ptr::c_str() calls
  void track_c_str(bool b);

  /*
   * an abstract raw buffer.  with a reference count.
   */
  class raw;
  class raw_malloc;
  class raw_static;
  class raw_mmap_pages;
  class raw_posix_aligned;
  class raw_hack_aligned;
  class raw_char;
  class raw_pipe;
  class raw_unshareable; // diagnostic, unshareable char buffer
  class raw_combined;


  class xio_mempool;
  class xio_msg_buffer;

  /*
   * named constructors 
   */
  raw* copy(const char *c, unsigned len);
  raw* create(unsigned len);
  raw* claim_char(unsigned len, char *buf);
  raw* create_malloc(unsigned len);
  raw* claim_malloc(unsigned len, char *buf);
  raw* create_static(unsigned len, char *buf);
  raw* create_aligned(unsigned len, unsigned align);
  raw* create_page_aligned(unsigned len);
  raw* create_zero_copy(unsigned len, int fd, int64_t *offset);
  raw* create_unshareable(unsigned len);

#if defined(HAVE_XIO)
  raw* create_msg(unsigned len, char *buf, XioDispatchHook *m_hook);
#endif

  /*
   * a buffer pointer.  references (a subsequence of) a raw buffer.
   */
  class CEPH_BUFFER_API ptr {
    raw *_raw;
    unsigned _off, _len;

    void release();

  public:
    ptr() : _raw(0), _off(0), _len(0) {}
    // cppcheck-suppress noExplicitConstructor
    ptr(raw *r);
    // cppcheck-suppress noExplicitConstructor
    ptr(unsigned l);
    ptr(const char *d, unsigned l);
    ptr(const ptr& p);
    ptr(ptr&& p);
    ptr(const ptr& p, unsigned o, unsigned l);
    ptr& operator= (const ptr& p);
    ptr& operator= (ptr&& p);
    ~ptr() {
      release();
    }

    bool have_raw() const { return _raw ? true:false; }

    raw *clone();
    void swap(ptr& other);
    ptr& make_shareable();

    // misc
    bool at_buffer_head() const { return _off == 0; }
    bool at_buffer_tail() const;

    bool is_aligned(unsigned align) const {
      return ((long)c_str() & (align-1)) == 0;
    }
    bool is_page_aligned() const { return is_aligned(CEPH_PAGE_SIZE); }
    bool is_n_align_sized(unsigned align) const
    {
      return (length() % align) == 0;
    }
    bool is_n_page_sized() const { return is_n_align_sized(CEPH_PAGE_SIZE); }
    bool is_partial() const {
      return have_raw() && (start() > 0 || end() < raw_length());
    }

    // accessors
    raw *get_raw() const { return _raw; }
    const char *c_str() const;
    char *c_str();
    unsigned length() const { return _len; }
    unsigned offset() const { return _off; }
    unsigned start() const { return _off; }
    unsigned end() const { return _off + _len; }
    unsigned unused_tail_length() const;
    const char& operator[](unsigned n) const;
    char& operator[](unsigned n);

    const char *raw_c_str() const;
    unsigned raw_length() const;
    int raw_nref() const;

    void copy_out(unsigned o, unsigned l, char *dest) const;

    bool can_zero_copy() const;
    int zero_copy_to_fd(int fd, int64_t *offset) const;

    unsigned wasted();

    int cmp(const ptr& o) const;
    bool is_zero() const;

    // modifiers
    void set_offset(unsigned o) {
      assert(raw_length() >= o);
      _off = o;
    }
    void set_length(unsigned l) {
      assert(raw_length() >= l);
      _len = l;
    }

    unsigned append(char c);
    unsigned append(const char *p, unsigned l);
    void copy_in(unsigned o, unsigned l, const char *src);
    void copy_in(unsigned o, unsigned l, const char *src, bool crc_reset);
    void zero();
    void zero(bool crc_reset);
    void zero(unsigned o, unsigned l);
    void zero(unsigned o, unsigned l, bool crc_reset);

  };


  /*
   * list - the useful bit!
   */

  class CEPH_BUFFER_API list {
    // my private bits
    std::list<ptr> _buffers;
    unsigned _len;
    unsigned _memcopy_count; //the total of memcopy using rebuild().
    ptr append_buffer;  // where i put small appends.

  public:
    class iterator;

  private:
    template <bool is_const>
    class CEPH_BUFFER_DETAILS iterator_impl
      : public std::iterator<std::forward_iterator_tag, char> {
    protected:
      typedef typename std::conditional<is_const,
					const list,
					list>::type bl_t;
      typedef typename std::conditional<is_const,
					const std::list<ptr>,
					std::list<ptr> >::type list_t;
      typedef typename std::conditional<is_const,
					typename std::list<ptr>::const_iterator,
					typename std::list<ptr>::iterator>::type list_iter_t;
      bl_t* bl;
      list_t* ls;  // meh.. just here to avoid an extra pointer dereference..
      unsigned off; // in bl
      list_iter_t p;
      unsigned p_off;   // in *p
      friend class iterator_impl<true>;

    public:
      // constructor.  position.
      iterator_impl()
	: bl(0), ls(0), off(0), p_off(0) {}
      iterator_impl(bl_t *l, unsigned o=0);
      iterator_impl(bl_t *l, unsigned o, list_iter_t ip, unsigned po)
	: bl(l), ls(&bl->_buffers), off(o), p(ip), p_off(po) {}
      iterator_impl(const list::iterator& i);

      /// get current iterator offset in buffer::list
      unsigned get_off() const { return off; }
      
      /// get number of bytes remaining from iterator position to the end of the buffer::list
      unsigned get_remaining() const { return bl->length() - off; }

      /// true if iterator is at the end of the buffer::list
      bool end() const {
	return p == ls->end();
	//return off == bl->length();
      }

      void advance(int o);
      void seek(unsigned o);
      char operator*() const;
      iterator_impl& operator++();
      ptr get_current_ptr() const;

      bl_t& get_bl() const { return *bl; }

      // copy data out.
      // note that these all _append_ to dest!
      void copy(unsigned len, char *dest);
      void copy(unsigned len, ptr &dest);
      void copy(unsigned len, list &dest);
      void copy(unsigned len, std::string &dest);
      void copy_all(list &dest);

      // get a pointer to the currenet iterator position, return the
      // number of bytes we can read from that position (up to want),
      // and advance the iterator by that amount.
      size_t get_ptr_and_advance(size_t want, const char **p);

      friend bool operator==(const iterator_impl& lhs,
			     const iterator_impl& rhs) {
	return &lhs.get_bl() == &rhs.get_bl() && lhs.get_off() == rhs.get_off();
      }
      friend bool operator!=(const iterator_impl& lhs,
			     const iterator_impl& rhs) {
	return &lhs.get_bl() != &rhs.get_bl() || lhs.get_off() != rhs.get_off();
      }
    };

  public:
    typedef iterator_impl<true> const_iterator;

    class CEPH_BUFFER_API iterator : public iterator_impl<false> {
    public:
      iterator() = default;
      iterator(bl_t *l, unsigned o=0);
      iterator(bl_t *l, unsigned o, list_iter_t ip, unsigned po);

      void advance(int o);
      void seek(unsigned o);
      char operator*();
      iterator& operator++();
      ptr get_current_ptr();

      // copy data out
      void copy(unsigned len, char *dest);
      void copy(unsigned len, ptr &dest);
      void copy(unsigned len, list &dest);
      void copy(unsigned len, std::string &dest);
      void copy_all(list &dest);

      // copy data in
      void copy_in(unsigned len, const char *src);
      void copy_in(unsigned len, const char *src, bool crc_reset);
      void copy_in(unsigned len, const list& otherl);

      bool operator==(const iterator& rhs) const {
	return bl == rhs.bl && off == rhs.off;
      }
      bool operator!=(const iterator& rhs) const {
	return bl != rhs.bl || off != rhs.off;
      }
    };

  private:
    mutable iterator last_p;
    int zero_copy_to_fd(int fd) const;

  public:
    // cons/des
    list() : _len(0), _memcopy_count(0), last_p(this) {}
    // cppcheck-suppress noExplicitConstructor
    list(unsigned prealloc) : _len(0), _memcopy_count(0), last_p(this) {
      append_buffer = buffer::create(prealloc);
      append_buffer.set_length(0);   // unused, so far.
    }

    list(const list& other) : _buffers(other._buffers), _len(other._len),
			      _memcopy_count(other._memcopy_count), last_p(this) {
      make_shareable();
    }
    list(list&& other);
    list& operator= (const list& other) {
      if (this != &other) {
        _buffers = other._buffers;
        _len = other._len;
	make_shareable();
      }
      return *this;
    }

    list& operator= (list&& other) {
      _buffers = std::move(other._buffers);
      _len = other._len;
      _memcopy_count = other._memcopy_count;
      last_p = begin();
      append_buffer.swap(other.append_buffer);
      other.clear();
      return *this;
    }

    unsigned get_num_buffers() const { return _buffers.size(); }
    const ptr& front() const { return _buffers.front(); }
    const ptr& back() const { return _buffers.back(); }

    unsigned get_memcopy_count() const {return _memcopy_count; }
    const std::list<ptr>& buffers() const { return _buffers; }
    void swap(list& other);
    unsigned length() const {
#if 0
      // DEBUG: verify _len
      unsigned len = 0;
      for (std::list<ptr>::const_iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++) {
	len += (*it).length();
      }
      assert(len == _len);
#endif
      return _len;
    }

    bool contents_equal(buffer::list& other);
    bool contents_equal(const buffer::list& other) const;

    bool can_zero_copy() const;
    bool is_provided_buffer(const char *dst) const;
    bool is_aligned(unsigned align) const;
    bool is_page_aligned() const;
    bool is_n_align_sized(unsigned align) const;
    bool is_n_page_sized() const;

    bool is_zero() const;

    // modifiers
    void clear() {
      _buffers.clear();
      _len = 0;
      _memcopy_count = 0;
      last_p = begin();
    }
    void push_front(ptr& bp) {
      if (bp.length() == 0)
	return;
      _buffers.push_front(bp);
      _len += bp.length();
    }
    void push_front(ptr&& bp) {
      if (bp.length() == 0)
	return;
      _len += bp.length();
      _buffers.push_front(std::move(bp));
    }
    void push_front(raw *r) {
      push_front(ptr(r));
    }
    void push_back(const ptr& bp) {
      if (bp.length() == 0)
	return;
      _buffers.push_back(bp);
      _len += bp.length();
    }
    void push_back(ptr&& bp) {
      if (bp.length() == 0)
	return;
      _len += bp.length();
      _buffers.push_back(std::move(bp));
    }
    void push_back(raw *r) {
      push_back(ptr(r));
    }

    void zero();
    void zero(unsigned o, unsigned l);

    bool is_contiguous() const;
    void rebuild();
    void rebuild(ptr& nb);
    void rebuild_aligned(unsigned align);
    void rebuild_aligned_size_and_memory(unsigned align_size,
					 unsigned align_memory);
    void rebuild_page_aligned();

    // assignment-op with move semantics
    const static unsigned int CLAIM_DEFAULT = 0;
    const static unsigned int CLAIM_ALLOW_NONSHAREABLE = 1;

    void claim(list& bl, unsigned int flags = CLAIM_DEFAULT);
    void claim_append(list& bl, unsigned int flags = CLAIM_DEFAULT);
    void claim_prepend(list& bl, unsigned int flags = CLAIM_DEFAULT);

    // clone non-shareable buffers (make shareable)
    void make_shareable() {
      std::list<buffer::ptr>::iterator pb;
      for (pb = _buffers.begin(); pb != _buffers.end(); ++pb) {
        (void) pb->make_shareable();
      }
    }

    // copy with explicit volatile-sharing semantics
    void share(const list& bl)
    {
      if (this != &bl) {
        clear();
        std::list<buffer::ptr>::const_iterator pb;
        for (pb = bl._buffers.begin(); pb != bl._buffers.end(); ++pb) {
          push_back(*pb);
        }
      }
    }

    iterator begin() {
      return iterator(this, 0);
    }
    iterator end() {
      return iterator(this, _len, _buffers.end(), 0);
    }

    const_iterator begin() const {
      return const_iterator(this, 0);
    }
    const_iterator end() const {
      return const_iterator(this, _len, _buffers.end(), 0);
    }

    // crope lookalikes.
    // **** WARNING: this are horribly inefficient for large bufferlists. ****
    void copy(unsigned off, unsigned len, char *dest) const;
    void copy(unsigned off, unsigned len, list &dest) const;
    void copy(unsigned off, unsigned len, std::string& dest) const;
    void copy_in(unsigned off, unsigned len, const char *src);
    void copy_in(unsigned off, unsigned len, const char *src, bool crc_reset);
    void copy_in(unsigned off, unsigned len, const list& src);

    void append(char c);
    void append(const char *data, unsigned len);
    void append(const std::string& s) {
      append(s.data(), s.length());
    }
    void append(const ptr& bp);
    void append(ptr&& bp);
    void append(const ptr& bp, unsigned off, unsigned len);
    void append(const list& bl);
    void append(std::istream& in);
    void append_zero(unsigned len);
    
    /*
     * get a char
     */
    const char& operator[](unsigned n) const;
    char *c_str();
    std::string to_str() const;

    void substr_of(const list& other, unsigned off, unsigned len);

    /// return a pointer to a contiguous extent of the buffer,
    /// reallocating as needed
    char *get_contiguous(unsigned off,  ///< offset
			 unsigned len); ///< length

    // funky modifer
    void splice(unsigned off, unsigned len, list *claim_by=0 /*, bufferlist& replace_with */);
    void write(int off, int len, std::ostream& out) const;

    void encode_base64(list& o);
    void decode_base64(list& o);

    void write_stream(std::ostream &out) const;
    void hexdump(std::ostream &out) const;
    int read_file(const char *fn, std::string *error);
    ssize_t read_fd(int fd, size_t len);
    int read_fd_zero_copy(int fd, size_t len);
    int write_file(const char *fn, int mode=0644);
    int write_fd(int fd) const;
    int write_fd(int fd, uint64_t offset) const;
    int write_fd_zero_copy(int fd) const;
    void prepare_iov(std::vector<iovec> *piov) const;
    uint32_t crc32c(uint32_t crc) const;
	void invalidate_crc();
  };

  /*
   * efficient hash of one or more bufferlists
   */

  class hash {
    uint32_t crc;

  public:
    hash() : crc(0) { }
    // cppcheck-suppress noExplicitConstructor
    hash(uint32_t init) : crc(init) { }

    void update(buffer::list& bl) {
      crc = bl.crc32c(crc);
    }

    uint32_t digest() {
      return crc;
    }
  };

inline bool operator>(bufferlist& l, bufferlist& r) {
  for (unsigned p = 0; ; p++) {
    if (l.length() > p && r.length() == p) return true;
    if (l.length() == p) return false;
    if (l[p] > r[p]) return true;
    if (l[p] < r[p]) return false;
  }
}
inline bool operator>=(bufferlist& l, bufferlist& r) {
  for (unsigned p = 0; ; p++) {
    if (l.length() > p && r.length() == p) return true;
    if (r.length() == p && l.length() == p) return true;
    if (l.length() == p && r.length() > p) return false;
    if (l[p] > r[p]) return true;
    if (l[p] < r[p]) return false;
  }
}

inline bool operator==(const bufferlist &l, const bufferlist &r) {
  if (l.length() != r.length())
    return false;
  for (unsigned p = 0; p < l.length(); p++) {
    if (l[p] != r[p])
      return false;
  }
  return true;
}
inline bool operator<(bufferlist& l, bufferlist& r) {
  return r > l;
}
inline bool operator<=(bufferlist& l, bufferlist& r) {
  return r >= l;
}


std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp);

std::ostream& operator<<(std::ostream& out, const raw &r);

std::ostream& operator<<(std::ostream& out, const buffer::list& bl);

std::ostream& operator<<(std::ostream& out, const buffer::error& e);

inline bufferhash& operator<<(bufferhash& l, bufferlist &r) {
  l.update(r);
  return l;
}

}

#if defined(HAVE_XIO)
xio_reg_mem* get_xio_mp(const buffer::ptr& bp);
#endif

}

#endif
