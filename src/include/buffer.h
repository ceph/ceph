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
#include <string>
#include <exception>

#include "page.h"
#include "crc32c.h"

#ifdef __CEPH__
# include "include/assert.h"
#else
# include <assert.h>
#endif

#if __GNUC__ >= 4
  #define CEPH_BUFFER_API  __attribute__ ((visibility ("default")))
#else
  #define CEPH_BUFFER_API
#endif

#if defined(HAVE_XIO)
struct xio_mempool_obj;
class XioDispatchHook;
#endif

namespace ceph {

class CEPH_BUFFER_API buffer {
  /*
   * exceptions
   */

public:
  struct error : public std::exception{
    const char *what() const throw () {
      return "buffer::exception";
    }
  };
  struct bad_alloc : public error {
    const char *what() const throw () {
      return "buffer::bad_alloc";
    }
  };
  struct end_of_buffer : public error {
    const char *what() const throw () {
      return "buffer::end_of_buffer";
    }
  };
  struct malformed_input : public error {
    explicit malformed_input(const std::string& w) {
      snprintf(buf, sizeof(buf), "buffer::malformed_input: %s", w.c_str());
    }
    const char *what() const throw () {
      return buf;
    }
  private:
    char buf[256];
  };
  struct error_code : public malformed_input {
    explicit error_code(int error);
    int code;
  };


  /// total bytes allocated
  static int get_total_alloc();

  /// enable/disable alloc tracking
  static void track_alloc(bool b);

  /// count of cached crc hits (matching input)
  static int get_cached_crc();
  /// count of cached crc hits (mismatching input, required adjustment)
  static int get_cached_crc_adjusted();
  /// enable/disable tracking of cached crcs
  static void track_cached_crc(bool b);

  /// count of calls to buffer::ptr::c_str()
  static int get_c_str_accesses();
  /// enable/disable tracking of buffer::ptr::c_str() calls
  static void track_c_str(bool b);

private:
 
  /* hack for memory utilization debugging. */
  static void inc_total_alloc(unsigned len);
  static void dec_total_alloc(unsigned len);

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

  friend std::ostream& operator<<(std::ostream& out, const raw &r);

public:
  class xio_mempool;
  class xio_msg_buffer;

  /*
   * named constructors 
   */
  static raw* copy(const char *c, unsigned len);
  static raw* create(unsigned len);
  static raw* claim_char(unsigned len, char *buf);
  static raw* create_malloc(unsigned len);
  static raw* claim_malloc(unsigned len, char *buf);
  static raw* create_static(unsigned len, char *buf);
  static raw* create_aligned(unsigned len, unsigned align);
  static raw* create_page_aligned(unsigned len);
  static raw* create_zero_copy(unsigned len, int fd, int64_t *offset);
  static raw* create_unshareable(unsigned len);

#if defined(HAVE_XIO)
  static raw* create_msg(unsigned len, char *buf, XioDispatchHook *m_hook);
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
    ptr(raw *r);
    ptr(unsigned l);
    ptr(const char *d, unsigned l);
    ptr(const ptr& p);
    ptr(const ptr& p, unsigned o, unsigned l);
    ptr& operator= (const ptr& p);
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

    void copy_out(unsigned o, unsigned l, char *dest) const {
      assert(_raw);
      if (!((o <= _len) && (o+l <= _len)))
	throw end_of_buffer();
      memcpy(dest, c_str()+o, l);
    }

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

    void append(char c);
    void append(const char *p, unsigned l);
    void copy_in(unsigned o, unsigned l, const char *src);
    void zero();
    void zero(unsigned o, unsigned l);

  };

  friend std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp);

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
    class CEPH_BUFFER_API iterator {
      list *bl;
      std::list<ptr> *ls; // meh.. just here to avoid an extra pointer dereference..
      unsigned off;  // in bl
      std::list<ptr>::iterator p;
      unsigned p_off; // in *p
    public:
      // constructor.  position.
      iterator() :
	bl(0), ls(0), off(0), p_off(0) {}
      iterator(list *l, unsigned o=0) : 
	bl(l), ls(&bl->_buffers), off(0), p(ls->begin()), p_off(0) {
	advance(o);
      }
      iterator(list *l, unsigned o, std::list<ptr>::iterator ip, unsigned po) : 
	bl(l), ls(&bl->_buffers), off(o), p(ip), p_off(po) { }

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
      char operator*();
      iterator& operator++();
      ptr get_current_ptr();

      list& get_bl() { return *bl; }

      // copy data out.
      // note that these all _append_ to dest!
      void copy(unsigned len, char *dest);
      void copy(unsigned len, ptr &dest);
      void copy(unsigned len, list &dest);
      void copy(unsigned len, std::string &dest);
      void copy_all(list &dest);

      // copy data in
      void copy_in(unsigned len, const char *src);
      void copy_in(unsigned len, const list& otherl);

    };

  private:
    mutable iterator last_p;
    int zero_copy_to_fd(int fd) const;

  public:
    // cons/des
    list() : _len(0), _memcopy_count(0), last_p(this) {}
    list(unsigned prealloc) : _len(0), _memcopy_count(0), last_p(this) {
      append_buffer = buffer::create(prealloc);
      append_buffer.set_length(0);   // unused, so far.
    }
    ~list() {}
    list(const list& other) : _buffers(other._buffers), _len(other._len),
			      _memcopy_count(other._memcopy_count), last_p(this) {
      make_shareable();
    }
    list& operator= (const list& other) {
      if (this != &other) {
        _buffers = other._buffers;
        _len = other._len;
	make_shareable();
      }
      return *this;
    }

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

    bool can_zero_copy() const;
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
    void push_front(raw *r) {
      ptr bp(r);
      push_front(bp);
    }
    void push_back(const ptr& bp) {
      if (bp.length() == 0)
	return;
      _buffers.push_back(bp);
      _len += bp.length();
    }
    void push_back(raw *r) {
      ptr bp(r);
      push_back(bp);
    }

    void zero();
    void zero(unsigned o, unsigned l);

    bool is_contiguous();
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

    // crope lookalikes.
    // **** WARNING: this are horribly inefficient for large bufferlists. ****
    void copy(unsigned off, unsigned len, char *dest) const;
    void copy(unsigned off, unsigned len, list &dest) const;
    void copy(unsigned off, unsigned len, std::string& dest) const;
    void copy_in(unsigned off, unsigned len, const char *src);
    void copy_in(unsigned off, unsigned len, const list& src);

    void append(char c);
    void append(const char *data, unsigned len);
    void append(const std::string& s) {
      append(s.data(), s.length());
    }
    void append(const ptr& bp);
    void append(const ptr& bp, unsigned off, unsigned len);
    void append(const list& bl);
    void append(std::istream& in);
    void append_zero(unsigned len);
    
    /*
     * get a char
     */
    const char& operator[](unsigned n) const;
    char *c_str();
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
    int write_fd_zero_copy(int fd) const;
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
    hash(uint32_t init) : crc(init) { }

    void update(buffer::list& bl) {
      crc = bl.crc32c(crc);
    }

    uint32_t digest() {
      return crc;
    }
  };
};

#if defined(HAVE_XIO)
xio_mempool_obj* get_xio_mp(const buffer::ptr& bp);
#endif

typedef buffer::ptr bufferptr;
typedef buffer::list bufferlist;
typedef buffer::hash bufferhash;


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


std::ostream& operator<<(std::ostream& out, const buffer::list& bl);

std::ostream& operator<<(std::ostream& out, const buffer::error& e);

inline bufferhash& operator<<(bufferhash& l, bufferlist &r) {
  l.update(r);
  return l;
}
}

#endif
