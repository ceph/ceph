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

#include <atomic>
#include <errno.h>
#include <limits.h>

#include <sys/uio.h>

#include "include/ceph_assert.h"
#include "include/types.h"
#include "include/buffer_raw.h"
#include "include/compat.h"
#include "include/mempool.h"
#include "armor.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/strtol.h"
#include "common/likely.h"
#include "common/valgrind.h"
#include "common/deleter.h"
#include "common/RWLock.h"
#include "include/spinlock.h"
#include "include/scope_guard.h"

#if defined(HAVE_XIO)
#include "msg/xio/XioMsg.h"
#endif

using namespace ceph;

#define CEPH_BUFFER_ALLOC_UNIT  4096u
#define CEPH_BUFFER_APPEND_SIZE (CEPH_BUFFER_ALLOC_UNIT - sizeof(raw_combined))

#ifdef BUFFER_DEBUG
static ceph::spinlock debug_lock;
# define bdout { std::lock_guard<ceph::spinlock> lg(debug_lock); std::cout
# define bendl std::endl; }
#else
# define bdout if (0) { std::cout
# define bendl std::endl; }
#endif

  static std::atomic<unsigned> buffer_cached_crc { 0 };
  static std::atomic<unsigned> buffer_cached_crc_adjusted { 0 };
  static std::atomic<unsigned> buffer_missed_crc { 0 };

  static bool buffer_track_crc = get_env_bool("CEPH_BUFFER_TRACK");

  void buffer::track_cached_crc(bool b) {
    buffer_track_crc = b;
  }
  int buffer::get_cached_crc() {
    return buffer_cached_crc;
  }
  int buffer::get_cached_crc_adjusted() {
    return buffer_cached_crc_adjusted;
  }

  int buffer::get_missed_crc() {
    return buffer_missed_crc;
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

  /*
   * raw_combined is always placed within a single allocation along
   * with the data buffer.  the data goes at the beginning, and
   * raw_combined at the end.
   */
  class buffer::raw_combined : public buffer::raw {
    size_t alignment;
  public:
    raw_combined(char *dataptr, unsigned l, unsigned align,
		 int mempool)
      : raw(dataptr, l, mempool),
	alignment(align) {
    }
    raw* clone_empty() override {
      return create(len, alignment);
    }

    static raw_combined *create(unsigned len,
				unsigned align,
				int mempool = mempool::mempool_buffer_anon) {
      if (!align)
	align = sizeof(size_t);
      size_t rawlen = round_up_to(sizeof(buffer::raw_combined),
				  alignof(buffer::raw_combined));
      size_t datalen = round_up_to(len, alignof(buffer::raw_combined));

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
      return new (ptr + datalen) raw_combined(ptr, len, align, mempool);
    }

    static void operator delete(void *ptr) {
      raw_combined *raw = (raw_combined *)ptr;
      ::free((void *)raw->data);
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
      bdout << "raw_malloc " << this << " alloc " << (void *)data << " " << l << bendl;
    }
    raw_malloc(unsigned l, char *b) : raw(b, l) {
      bdout << "raw_malloc " << this << " alloc " << (void *)data << " " << l << bendl;
    }
    ~raw_malloc() override {
      free(data);
      bdout << "raw_malloc " << this << " free " << (void *)data << " " << bendl;
    }
    raw* clone_empty() override {
      return new raw_malloc(len);
    }
  };

#ifndef __CYGWIN__
  class buffer::raw_posix_aligned : public buffer::raw {
    unsigned align;
  public:
    MEMPOOL_CLASS_HELPERS();

    raw_posix_aligned(unsigned l, unsigned _align) : raw(l) {
      align = _align;
      ceph_assert((align >= sizeof(void *)) && (align & (align - 1)) == 0);
#ifdef DARWIN
      data = (char *) valloc(len);
#else
      int r = ::posix_memalign((void**)(void*)&data, align, len);
      if (r)
	throw bad_alloc();
#endif /* DARWIN */
      if (!data)
	throw bad_alloc();
      bdout << "raw_posix_aligned " << this << " alloc " << (void *)data
	    << " l=" << l << ", align=" << align << bendl;
    }
    ~raw_posix_aligned() override {
      ::free(data);
      bdout << "raw_posix_aligned " << this << " free " << (void *)data << bendl;
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
      //cout << "hack aligned " << (unsigned)data
      //<< " in raw " << (unsigned)realdata
      //<< " off " << off << std::endl;
      ceph_assert(((unsigned)data & (align-1)) == 0);
    }
    ~raw_hack_aligned() {
      delete[] realdata;
    }
    raw* clone_empty() {
      return new raw_hack_aligned(len, align);
    }
  };
#endif

  /*
   * primitive buffer types
   */
  class buffer::raw_char : public buffer::raw {
  public:
    MEMPOOL_CLASS_HELPERS();

    explicit raw_char(unsigned l) : raw(l) {
      if (len)
	data = new char[len];
      else
	data = 0;
      bdout << "raw_char " << this << " alloc " << (void *)data << " " << l << bendl;
    }
    raw_char(unsigned l, char *b) : raw(b, l) {
      bdout << "raw_char " << this << " alloc " << (void *)data << " " << l << bendl;
    }
    ~raw_char() override {
      delete[] data;
      bdout << "raw_char " << this << " free " << (void *)data << bendl;
    }
    raw* clone_empty() override {
      return new raw_char(len);
    }
  };

  class buffer::raw_claimed_char : public buffer::raw {
  public:
    MEMPOOL_CLASS_HELPERS();

    explicit raw_claimed_char(unsigned l, char *b) : raw(b, l) {
      bdout << "raw_claimed_char " << this << " alloc " << (void *)data
	    << " " << l << bendl;
    }
    ~raw_claimed_char() override {
      bdout << "raw_claimed_char " << this << " free " << (void *)data
	    << bendl;
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
    bool is_shareable() const override {
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

    bool is_shareable() const override { return false; }
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

  ceph::unique_leakable_ptr<buffer::raw> buffer::copy(const char *c, unsigned len) {
    auto r = buffer::create_aligned(len, sizeof(size_t));
    memcpy(r->data, c, len);
    return r;
  }

  ceph::unique_leakable_ptr<buffer::raw> buffer::create(unsigned len) {
    return buffer::create_aligned(len, sizeof(size_t));
  }
  ceph::unique_leakable_ptr<buffer::raw> buffer::create_in_mempool(unsigned len, int mempool) {
    return buffer::create_aligned_in_mempool(len, sizeof(size_t), mempool);
  }
  buffer::raw* buffer::claim_char(unsigned len, char *buf) {
    return new raw_claimed_char(len, buf);
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

  ceph::unique_leakable_ptr<buffer::raw> buffer::create_aligned_in_mempool(
    unsigned len, unsigned align, int mempool) {
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
      return ceph::unique_leakable_ptr<buffer::raw>(new raw_posix_aligned(len, align));
#else
      return ceph::unique_leakable_ptr<buffer::raw>(new raw_hack_aligned(len, align));
#endif
    }
    return ceph::unique_leakable_ptr<buffer::raw>(
      raw_combined::create(len, align, mempool));
  }
  ceph::unique_leakable_ptr<buffer::raw> buffer::create_aligned(
    unsigned len, unsigned align) {
    return create_aligned_in_mempool(len, align,
				     mempool::mempool_buffer_anon);
  }

  ceph::unique_leakable_ptr<buffer::raw> buffer::create_page_aligned(unsigned len) {
    return create_aligned(len, CEPH_PAGE_SIZE);
  }
  ceph::unique_leakable_ptr<buffer::raw> buffer::create_small_page_aligned(unsigned len) {
    if (len < CEPH_PAGE_SIZE) {
      return create_aligned(len, CEPH_BUFFER_ALLOC_UNIT);
    } else
      return create_aligned(len, CEPH_PAGE_SIZE);
  }

  buffer::raw* buffer::create_unshareable(unsigned len) {
    return new raw_unshareable(len);
  }

  buffer::ptr::ptr(raw* r) : _raw(r), _off(0), _len(r->len)   // no lock needed; this is an unref raw.
  {
    r->nref++;
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(ceph::unique_leakable_ptr<raw> r)
    : _raw(r.release()),
      _off(0),
      _len(_raw->len)
  {
    _raw->nref.store(1, std::memory_order_release);
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(unsigned l) : _off(0), _len(l)
  {
    _raw = buffer::create(l).release();
    _raw->nref.store(1, std::memory_order_release);
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(const char *d, unsigned l) : _off(0), _len(l)    // ditto.
  {
    _raw = buffer::copy(d, l).release();
    _raw->nref.store(1, std::memory_order_release);
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(const ptr& p) : _raw(p._raw), _off(p._off), _len(p._len)
  {
    if (_raw) {
      _raw->nref++;
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
    ceph_assert(o+l <= p._len);
    ceph_assert(_raw);
    _raw->nref++;
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr::ptr(const ptr& p, ceph::unique_leakable_ptr<raw> r)
    : _raw(r.release()),
      _off(p._off),
      _len(p._len)
  {
    _raw->nref.store(1, std::memory_order_release);
    bdout << "ptr " << this << " get " << _raw << bendl;
  }
  buffer::ptr& buffer::ptr::operator= (const ptr& p)
  {
    if (p._raw) {
      p._raw->nref++;
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

  ceph::unique_leakable_ptr<buffer::raw> buffer::ptr::clone()
  {
    return _raw->clone();
  }

  void buffer::ptr::swap(ptr& other) noexcept
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
      const bool last_one = (1 == _raw->nref.load(std::memory_order_acquire));
      if (likely(last_one) || --_raw->nref == 0) {
        // BE CAREFUL: this is called also for hypercombined ptr_node. After
        // freeing underlying raw, `*this` can become inaccessible as well!
        const auto* delete_raw = _raw;
        _raw = nullptr;
	//cout << "hosing raw " << (void*)_raw << " len " << _raw->len << std::endl;
        ANNOTATE_HAPPENS_AFTER(&_raw->nref);
        ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&_raw->nref);
	delete delete_raw;  // dealloc old (if any)
      } else {
        ANNOTATE_HAPPENS_BEFORE(&_raw->nref);
        _raw = nullptr;
      }
    }
  }

  int buffer::ptr::get_mempool() const {
    if (_raw) {
      return _raw->mempool;
    }
    return mempool::mempool_buffer_anon;
  }

  void buffer::ptr::reassign_to_mempool(int pool) {
    if (_raw) {
      _raw->reassign_to_mempool(pool);
    }
  }
  void buffer::ptr::try_assign_to_mempool(int pool) {
    if (_raw) {
      _raw->try_assign_to_mempool(pool);
    }
  }

  const char *buffer::ptr::c_str() const {
    ceph_assert(_raw);
    return _raw->get_data() + _off;
  }
  char *buffer::ptr::c_str() {
    ceph_assert(_raw);
    return _raw->get_data() + _off;
  }
  const char *buffer::ptr::end_c_str() const {
    ceph_assert(_raw);
    return _raw->get_data() + _off + _len;
  }
  char *buffer::ptr::end_c_str() {
    ceph_assert(_raw);
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
    ceph_assert(_raw);
    ceph_assert(n < _len);
    return _raw->get_data()[_off + n];
  }
  char& buffer::ptr::operator[](unsigned n)
  {
    ceph_assert(_raw);
    ceph_assert(n < _len);
    return _raw->get_data()[_off + n];
  }

  const char *buffer::ptr::raw_c_str() const { ceph_assert(_raw); return _raw->data; }
  unsigned buffer::ptr::raw_length() const { ceph_assert(_raw); return _raw->len; }
  int buffer::ptr::raw_nref() const { ceph_assert(_raw); return _raw->nref; }

  void buffer::ptr::copy_out(unsigned o, unsigned l, char *dest) const {
    ceph_assert(_raw);
    if (o+l > _len)
        throw end_of_buffer();
    char* src =  _raw->data + _off + o;
    maybe_inline_memcpy(dest, src, l, 8);
  }

  unsigned buffer::ptr::wasted() const
  {
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
    ceph_assert(_raw);
    ceph_assert(1 <= unused_tail_length());
    char* ptr = _raw->data + _off + _len;
    *ptr = c;
    _len++;
    return _len + _off;
  }

  unsigned buffer::ptr::append(const char *p, unsigned l)
  {
    ceph_assert(_raw);
    ceph_assert(l <= unused_tail_length());
    char* c = _raw->data + _off + _len;
    maybe_inline_memcpy(c, p, l, 32);
    _len += l;
    return _len + _off;
  }

  unsigned buffer::ptr::append_zeros(unsigned l)
  {
    ceph_assert(_raw);
    ceph_assert(l <= unused_tail_length());
    char* c = _raw->data + _off + _len;
    memset(c, 0, l);
    _len += l;
    return _len + _off;
  }

  void buffer::ptr::copy_in(unsigned o, unsigned l, const char *src, bool crc_reset)
  {
    ceph_assert(_raw);
    ceph_assert(o <= _len);
    ceph_assert(o+l <= _len);
    char* dest = _raw->data + _off + o;
    if (crc_reset)
        _raw->invalidate_crc();
    maybe_inline_memcpy(dest, src, l, 64);
  }

  void buffer::ptr::zero(bool crc_reset)
  {
    if (crc_reset)
        _raw->invalidate_crc();
    memset(c_str(), 0, _len);
  }

  void buffer::ptr::zero(unsigned o, unsigned l, bool crc_reset)
  {
    ceph_assert(o+l <= _len);
    if (crc_reset)
        _raw->invalidate_crc();
    memset(c_str()+o, 0, l);
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
    : bl(l), ls(&bl->_buffers), p(ls->begin()), off(0), p_off(0)
  {
    advance(o);
  }

  template<bool is_const>
  buffer::list::iterator_impl<is_const>::iterator_impl(const buffer::list::iterator& i)
    : iterator_impl<is_const>(i.bl, i.off, i.p, i.p_off) {}

  template<bool is_const>
  void buffer::list::iterator_impl<is_const>::advance(unsigned o)
  {
    //cout << this << " advance " << o << " from " << off
    //     << " (p_off " << p_off << " in " << p->length() << ")"
    //     << std::endl;

    p_off +=o;
    while (p != ls->end()) {
      if (p_off >= p->length()) {
        // skip this buffer
        p_off -= p->length();
        p++;
      } else {
        // somewhere in this buffer!
        break;
      }
    }
    if (p == ls->end() && p_off) {
      throw end_of_buffer();
    }
    off += o;
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
    advance(1u);
    return *this;
  }

  template<bool is_const>
  buffer::ptr buffer::list::iterator_impl<is_const>::get_current_ptr() const
  {
    if (p == ls->end())
      throw end_of_buffer();
    return ptr(*p, p_off, p->length() - p_off);
  }

  template<bool is_const>
  bool buffer::list::iterator_impl<is_const>::is_pointing_same_raw(
    const ptr& other) const
  {
    if (p == ls->end())
      throw end_of_buffer();
    return p->get_raw() == other.get_raw();
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

      unsigned howmuch = p->length() - p_off;
      const char *c_str = p->c_str();
      dest.append(c_str + p_off, howmuch);

      advance(howmuch);
    }
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
    size_t l = std::min<size_t>(p->length() - p_off, want);
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
    length = std::min<size_t>(length, get_remaining());
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
    for (const auto& node : otherl._buffers) {
      unsigned l = node.length();
      if (left < l)
	l = left;
      copy_in(l, node.c_str());
      left -= l;
      if (left == 0)
	break;
    }
  }

  // -- buffer::list --

  buffer::list::list(list&& other) noexcept
    : _buffers(std::move(other._buffers)),
      _carriage(&always_empty_bptr),
      _len(other._len),
      _memcopy_count(other._memcopy_count),
      last_p(this) {
    other.clear();
  }

  void buffer::list::swap(list& other) noexcept
  {
    std::swap(_len, other._len);
    std::swap(_memcopy_count, other._memcopy_count);
    std::swap(_carriage, other._carriage);
    _buffers.swap(other._buffers);
    //last_p.swap(other.last_p);
    last_p = begin();
    other.last_p = other.begin();
  }

  bool buffer::list::contents_equal(const ceph::buffer::list& other) const
  {
    if (length() != other.length())
      return false;

    // buffer-wise comparison
    if (true) {
      auto a = std::cbegin(_buffers);
      auto b = std::cbegin(other._buffers);
      unsigned aoff = 0, boff = 0;
      while (a != std::cend(_buffers)) {
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

  bool buffer::list::is_provided_buffer(const char* const dst) const
  {
    if (_buffers.empty()) {
      return false;
    }
    return (is_contiguous() && (_buffers.front().c_str() == dst));
  }

  bool buffer::list::is_aligned(const unsigned align) const
  {
    for (const auto& node : _buffers) {
      if (!node.is_aligned(align)) {
	return false;
      }
    }
    return true;
  }

  bool buffer::list::is_n_align_sized(const unsigned align) const
  {
    for (const auto& node : _buffers) {
      if (!node.is_n_align_sized(align)) {
	return false;
      }
    }
    return true;
  }

  bool buffer::list::is_aligned_size_and_memory(
    const unsigned align_size,
    const unsigned align_memory) const
  {
    for (const auto& node : _buffers) {
      if (!node.is_aligned(align_memory) || !node.is_n_align_sized(align_size)) {
	return false;
      }
    }
    return true;
  }

  bool buffer::list::is_zero() const {
    for (const auto& node : _buffers) {
      if (!node.is_zero()) {
	return false;
      }
    }
    return true;
  }

  void buffer::list::zero()
  {
    for (auto& node : _buffers) {
      node.zero();
    }
  }

  void buffer::list::zero(const unsigned o, const unsigned l)
  {
    ceph_assert(o+l <= _len);
    unsigned p = 0;
    for (auto& node : _buffers) {
      if (p + node.length() > o) {
        if (p >= o && p+node.length() <= o+l) {
          // 'o'------------- l -----------|
          //      'p'-- node.length() --|
	  node.zero();
        } else if (p >= o) {
          // 'o'------------- l -----------|
          //    'p'------- node.length() -------|
	  node.zero(0, o+l-p);
        } else if (p + node.length() <= o+l) {
          //     'o'------------- l -----------|
          // 'p'------- node.length() -------|
	  node.zero(o-p, node.length()-(o-p));
        } else {
          //       'o'----------- l -----------|
          // 'p'---------- node.length() ----------|
          node.zero(o-p, l);
        }
      }
      p += node.length();
      if (o+l <= p) {
	break;  // done
      }
    }
  }

  bool buffer::list::is_contiguous() const
  {
    return _buffers.size() <= 1;
  }

  bool buffer::list::is_n_page_sized() const
  {
    return is_n_align_sized(CEPH_PAGE_SIZE);
  }

  bool buffer::list::is_page_aligned() const
  {
    return is_aligned(CEPH_PAGE_SIZE);
  }

  int buffer::list::get_mempool() const
  {
    if (_buffers.empty()) {
      return mempool::mempool_buffer_anon;
    }
    return _buffers.back().get_mempool();
  }

  void buffer::list::reassign_to_mempool(int pool)
  {
    for (auto& p : _buffers) {
      p.get_raw()->reassign_to_mempool(pool);
    }
  }

  void buffer::list::try_assign_to_mempool(int pool)
  {
    for (auto& p : _buffers) {
      p.get_raw()->try_assign_to_mempool(pool);
    }
  }

  uint64_t buffer::list::get_wasted_space() const
  {
    if (_buffers.size() == 1)
      return _buffers.back().wasted();

    std::vector<const raw*> raw_vec;
    raw_vec.reserve(_buffers.size());
    for (const auto& p : _buffers)
      raw_vec.push_back(p.get_raw());
    std::sort(raw_vec.begin(), raw_vec.end());

    uint64_t total = 0;
    const raw *last = nullptr;
    for (const auto r : raw_vec) {
      if (r == last)
	continue;
      last = r;
      total += r->len;
    }
    // If multiple buffers are sharing the same raw buffer and they overlap
    // with each other, the wasted space will be underestimated.
    if (total <= length())
      return 0;
    return total - length();
  }

  void buffer::list::rebuild()
  {
    if (_len == 0) {
      _carriage = &always_empty_bptr;
      _buffers.clear_and_dispose();
      return;
    }
    if ((_len & ~CEPH_PAGE_MASK) == 0)
      rebuild(ptr_node::create(buffer::create_page_aligned(_len)));
    else
      rebuild(ptr_node::create(buffer::create(_len)));
  }

  void buffer::list::rebuild(
    std::unique_ptr<buffer::ptr_node, buffer::ptr_node::disposer> nb)
  {
    unsigned pos = 0;
    for (auto& node : _buffers) {
      nb->copy_in(pos, node.length(), node.c_str(), false);
      pos += node.length();
    }
    _memcopy_count += pos;
    _carriage = &always_empty_bptr;
    _buffers.clear_and_dispose();
    if (likely(nb->length())) {
      _carriage = nb.get();
      _buffers.push_back(*nb.release());
    }
    invalidate_crc();
    last_p = begin();
  }

  bool buffer::list::rebuild_aligned(unsigned align)
  {
    return rebuild_aligned_size_and_memory(align, align);
  }
  
  bool buffer::list::rebuild_aligned_size_and_memory(unsigned align_size,
						    unsigned align_memory,
						    unsigned max_buffers)
  {
    unsigned old_memcopy_count = _memcopy_count;

    if (max_buffers && _buffers.size() > max_buffers
	&& _len > (max_buffers * align_size)) {
      align_size = round_up_to(round_up_to(_len, max_buffers) / max_buffers, align_size);
    }
    auto p = std::begin(_buffers);
    auto p_prev = _buffers.before_begin();
    while (p != std::end(_buffers)) {
      // keep anything that's already align and sized aligned
      if (p->is_aligned(align_memory) && p->is_n_align_sized(align_size)) {
        /*cout << " segment " << (void*)p->c_str()
  	     << " offset " << ((unsigned long)p->c_str() & (align - 1))
  	     << " length " << p->length()
  	     << " " << (p->length() & (align - 1)) << " ok" << std::endl;
        */
        p_prev = p++;
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
        // no need to reallocate, relinking is enough thankfully to bi::list.
        auto p_after = _buffers.erase_after(p_prev);
        unaligned._buffers.push_back(*p);
        unaligned._len += p->length();
        p = p_after;
      } while (p != std::end(_buffers) &&
  	     (!p->is_aligned(align_memory) ||
  	      !p->is_n_align_sized(align_size) ||
  	      (offset % align_size)));
      if (!(unaligned.is_contiguous() && unaligned._buffers.front().is_aligned(align_memory))) {
        unaligned.rebuild(
          ptr_node::create(
            buffer::create_aligned(unaligned._len, align_memory)));
        _memcopy_count += unaligned._len;
      }
      _buffers.insert_after(p_prev, *ptr_node::create(unaligned._buffers.front()).release());
      ++p_prev;
    }
    last_p = begin();

    return  (old_memcopy_count != _memcopy_count);
  }
  
  bool buffer::list::rebuild_page_aligned()
  {
   return  rebuild_aligned(CEPH_PAGE_SIZE);
  }

  void buffer::list::reserve(size_t prealloc)
  {
    if (get_append_buffer_unused_tail_length() < prealloc) {
      auto ptr = ptr_node::create(buffer::create_page_aligned(prealloc));
      ptr->set_length(0);   // unused, so far.
      _carriage = ptr.get();
      _buffers.push_back(*ptr.release());
    }
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
    if (!(flags & CLAIM_ALLOW_NONSHAREABLE)) {
      auto curbuf = bl._buffers.begin();
      auto curbuf_prev = bl._buffers.before_begin();

      while (curbuf != bl._buffers.end()) {
	const auto* const raw = curbuf->get_raw();
	if (unlikely(raw && !raw->is_shareable())) {
	  auto* clone = ptr_node::copy_hypercombined(*curbuf);
	  curbuf = bl._buffers.erase_after_and_dispose(curbuf_prev);
	  bl._buffers.insert_after(curbuf_prev++, *clone);
	} else {
	  curbuf_prev = curbuf++;
	}
      }
    }
    _buffers.splice_back(bl._buffers);
    bl._carriage = &always_empty_bptr;
    bl._buffers.clear_and_dispose();
    bl._len = 0;
    bl.last_p = bl.begin();
  }

  void buffer::list::claim_append_piecewise(list& bl)
  {
    // steal the other guy's buffers
    for (const auto& node : bl.buffers()) {
      append(node, 0, node.length());
    }
    bl.clear();
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
    unsigned gap = get_append_buffer_unused_tail_length();
    if (!gap) {
      // make a new buffer!
      auto buf = ptr_node::create(
	raw_combined::create(CEPH_BUFFER_APPEND_SIZE, 0, get_mempool()));
      buf->set_length(0);   // unused, so far.
      _carriage = buf.get();
      _buffers.push_back(*buf.release());
    } else if (unlikely(_carriage != &_buffers.back())) {
      auto bptr = ptr_node::create(*_carriage, _carriage->length(), 0);
      _carriage = bptr.get();
      _buffers.push_back(*bptr.release());
    }
    _carriage->append(c);
    _len++;
  }

  buffer::ptr buffer::list::always_empty_bptr;

  buffer::ptr_node& buffer::list::refill_append_space(const unsigned len)
  {
    // make a new buffer.  fill out a complete page, factoring in the
    // raw_combined overhead.
    size_t need = round_up_to(len, sizeof(size_t)) + sizeof(raw_combined);
    size_t alen = round_up_to(need, CEPH_BUFFER_ALLOC_UNIT) -
      sizeof(raw_combined);
    auto new_back = \
      ptr_node::create(raw_combined::create(alen, 0, get_mempool()));
    new_back->set_length(0);   // unused, so far.
    _carriage = new_back.get();
    _buffers.push_back(*new_back.release());
    return _buffers.back();
  }

  void buffer::list::append(const char *data, unsigned len)
  {
    _len += len;

    const unsigned free_in_last = get_append_buffer_unused_tail_length();
    const unsigned first_round = std::min(len, free_in_last);
    if (first_round) {
      // _buffers and carriage can desynchronize when 1) a new ptr
      // we don't own has been added into the _buffers 2) _buffers
      // has been emptied as as a result of std::move or stolen by
      // claim_append.
      if (unlikely(_carriage != &_buffers.back())) {
        auto bptr = ptr_node::create(*_carriage, _carriage->length(), 0);
	_carriage = bptr.get();
	_buffers.push_back(*bptr.release());
      }
      _carriage->append(data, first_round);
    }

    const unsigned second_round = len - first_round;
    if (second_round) {
      auto& new_back = refill_append_space(second_round);
      new_back.append(data + first_round, second_round);
    }
  }

  buffer::list::reserve_t buffer::list::obtain_contiguous_space(
    const unsigned len)
  {
    // note: if len < the normal append_buffer size it *might*
    // be better to allocate a normal-sized append_buffer and
    // use part of it.  however, that optimizes for the case of
    // old-style types including new-style types.  and in most
    // such cases, this won't be the very first thing encoded to
    // the list, so append_buffer will already be allocated.
    // OTOH if everything is new-style, we *should* allocate
    // only what we need and conserve memory.
    if (unlikely(get_append_buffer_unused_tail_length() < len)) {
      auto new_back = \
	buffer::ptr_node::create(buffer::create(len)).release();
      new_back->set_length(0);   // unused, so far.
      _buffers.push_back(*new_back);
      _carriage = new_back;
      return { new_back->c_str(), &new_back->_len, &_len };
    } else {
      if (unlikely(_carriage != &_buffers.back())) {
        auto bptr = ptr_node::create(*_carriage, _carriage->length(), 0);
	_carriage = bptr.get();
	_buffers.push_back(*bptr.release());
      }
      return { _carriage->end_c_str(), &_carriage->_len, &_len };
    }
  }

  void buffer::list::append(const ptr& bp)
  {
      push_back(bp);
  }

  void buffer::list::append(ptr&& bp)
  {
      push_back(std::move(bp));
  }

  void buffer::list::append(const ptr& bp, unsigned off, unsigned len)
  {
    ceph_assert(len+off <= bp.length());
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
    _buffers.push_back(*ptr_node::create(bp, off, len).release());
    _len += len;
  }

  void buffer::list::append(const list& bl)
  {
    _len += bl._len;
    for (const auto& node : bl._buffers) {
      _buffers.push_back(*ptr_node::create(node).release());
    }
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

  buffer::list::contiguous_filler buffer::list::append_hole(const unsigned len)
  {
    _len += len;

    if (unlikely(get_append_buffer_unused_tail_length() < len)) {
      // make a new append_buffer.  fill out a complete page, factoring in
      // the raw_combined overhead.
      auto& new_back = refill_append_space(len);
      new_back.set_length(len);
      return { new_back.c_str() };
    } else if (unlikely(_carriage != &_buffers.back())) {
      auto bptr = ptr_node::create(*_carriage, _carriage->length(), 0);
      _carriage = bptr.get();
      _buffers.push_back(*bptr.release());
    }
    _carriage->set_length(_carriage->length() + len);
    return { _carriage->end_c_str() - len };
  }

  void buffer::list::prepend_zero(unsigned len)
  {
    auto bp = ptr_node::create(len);
    bp->zero(false);
    _len += len;
    _buffers.push_front(*bp.release());
  }
  
  void buffer::list::append_zero(unsigned len)
  {
    _len += len;

    const unsigned free_in_last = get_append_buffer_unused_tail_length();
    const unsigned first_round = std::min(len, free_in_last);
    if (first_round) {
      if (unlikely(_carriage != &_buffers.back())) {
        auto bptr = ptr_node::create(*_carriage, _carriage->length(), 0);
	_carriage = bptr.get();
	_buffers.push_back(*bptr.release());
      }
      _carriage->append_zeros(first_round);
    }

    const unsigned second_round = len - first_round;
    if (second_round) {
      auto& new_back = refill_append_space(second_round);
      new_back.set_length(second_round);
      new_back.zero(false);
    }
  }

  
  /*
   * get a char
   */
  const char& buffer::list::operator[](unsigned n) const
  {
    if (n >= _len)
      throw end_of_buffer();
    
    for (const auto& node : _buffers) {
      if (n >= node.length()) {
	n -= node.length();
	continue;
      }
      return node[n];
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

    auto iter = std::cbegin(_buffers);
    ++iter;

    if (iter != std::cend(_buffers)) {
      rebuild();
    }
    return _buffers.front().c_str();  // good, we're already contiguous.
  }

  string buffer::list::to_str() const {
    string s;
    s.reserve(length());
    for (const auto& node : _buffers) {
      if (node.length()) {
	s.append(node.c_str(), node.length());
      }
    }
    return s;
  }

  void buffer::list::substr_of(const list& other, unsigned off, unsigned len)
  {
    if (off + len > other.length())
      throw end_of_buffer();

    clear();

    // skip off
    auto curbuf = std::cbegin(other._buffers);
    while (off > 0 && off >= curbuf->length()) {
      // skip this buffer
      //cout << "skipping over " << *curbuf << std::endl;
      off -= (*curbuf).length();
      ++curbuf;
    }
    ceph_assert(len == 0 || curbuf != std::cend(other._buffers));
    
    while (len > 0) {
      // partial?
      if (off + len < curbuf->length()) {
	//cout << "copying partial of " << *curbuf << std::endl;
	_buffers.push_back(*ptr_node::create( *curbuf, off, len ).release());
	_len += len;
	break;
      }
      
      // through end
      //cout << "copying end (all?) of " << *curbuf << std::endl;
      unsigned howmuch = curbuf->length() - off;
      _buffers.push_back(*ptr_node::create( *curbuf, off, howmuch ).release());
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

    ceph_assert(len > 0);
    //cout << "splice off " << off << " len " << len << " ... mylen = " << length() << std::endl;
      
    // skip off
    auto curbuf = std::begin(_buffers);
    auto curbuf_prev = _buffers.before_begin();
    while (off > 0) {
      ceph_assert(curbuf != std::end(_buffers));
      if (off >= (*curbuf).length()) {
	// skip this buffer
	//cout << "off = " << off << " skipping over " << *curbuf << std::endl;
	off -= (*curbuf).length();
	curbuf_prev = curbuf++;
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
      _buffers.insert_after(curbuf_prev,
			    *ptr_node::create(*curbuf, 0, off).release());
      _len += off;
      ++curbuf_prev;
    }
    
    _carriage = &always_empty_bptr;

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
      curbuf = _buffers.erase_after_and_dispose(curbuf_prev);
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
    for (const auto& node : s._buffers) {
      if (node.length()) {
	out.write(node.c_str(), node.length());
      }
    }
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
  ceph_assert(l <= (int)bp.length());
  bp.set_length(l);
  push_back(std::move(bp));
}

  

int buffer::list::read_file(const char *fn, std::string *error)
{
  int fd = TEMP_FAILURE_RETRY(::open(fn, O_RDONLY|O_CLOEXEC));
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
  auto bp = ptr_node::create(buffer::create(len));
  ssize_t ret = safe_read(fd, (void*)bp->c_str(), len);
  if (ret >= 0) {
    bp->set_length(ret);
    push_back(std::move(bp));
  }
  return ret;
}

int buffer::list::write_file(const char *fn, int mode)
{
  int fd = TEMP_FAILURE_RETRY(::open(fn, O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC, mode));
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
  while (bytes > 0) {
    ssize_t r = 0;
#ifdef HAVE_PWRITEV
    r = ::pwritev(fd, vec, veclen, offset);
#else
    r = ::lseek64(fd, offset, SEEK_SET);
    if (r != offset) {
      return -errno;
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
  // use writev!
  iovec iov[IOV_MAX];
  int iovlen = 0;
  ssize_t bytes = 0;

  auto p = std::cbegin(_buffers);
  while (p != std::cend(_buffers)) {
    if (p->length() > 0) {
      iov[iovlen].iov_base = (void *)p->c_str();
      iov[iovlen].iov_len = p->length();
      bytes += p->length();
      iovlen++;
    }
    ++p;

    if (iovlen == IOV_MAX ||
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

  auto p = std::cbegin(_buffers);
  uint64_t left_pbrs = std::size(_buffers);
  while (left_pbrs) {
    ssize_t bytes = 0;
    unsigned iovlen = 0;
    uint64_t size = std::min<uint64_t>(left_pbrs, IOV_MAX);
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

__u32 buffer::list::crc32c(__u32 crc) const
{
  int cache_misses = 0;
  int cache_hits = 0;
  int cache_adjusts = 0;

  for (const auto& node : _buffers) {
    if (node.length()) {
      raw* const r = node.get_raw();
      pair<size_t, size_t> ofs(node.offset(), node.offset() + node.length());
      pair<uint32_t, uint32_t> ccrc;
      if (r->get_crc(ofs, &ccrc)) {
	if (ccrc.first == crc) {
	  // got it already
	  crc = ccrc.second;
	  cache_hits++;
	} else {
	  /* If we have cached crc32c(buf, v) for initial value v,
	   * we can convert this to a different initial value v' by:
	   * crc32c(buf, v') = crc32c(buf, v) ^ adjustment
	   * where adjustment = crc32c(0*len(buf), v ^ v')
	   *
	   * http://crcutil.googlecode.com/files/crc-doc.1.0.pdf
	   * note, u for our crc32c implementation is 0
	   */
	  crc = ccrc.second ^ ceph_crc32c(ccrc.first ^ crc, NULL, node.length());
	  cache_adjusts++;
	}
      } else {
	cache_misses++;
	uint32_t base = crc;
	crc = ceph_crc32c(crc, (unsigned char*)node.c_str(), node.length());
	r->set_crc(ofs, make_pair(base, crc));
      }
    }
  }

  if (buffer_track_crc) {
    if (cache_adjusts)
      buffer_cached_crc_adjusted += cache_adjusts;
    if (cache_hits)
      buffer_cached_crc += cache_hits;
    if (cache_misses)
      buffer_missed_crc += cache_misses;
  }

  return crc;
}

void buffer::list::invalidate_crc()
{
  for (const auto& node : _buffers) {
    raw* const r = node.get_raw();
    if (r) {
      r->invalidate_crc();
    }
  }
}

#include "common/ceph_crypto.h"
using ceph::crypto::SHA1;

sha1_digest_t buffer::list::sha1()
{
  unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE];
  SHA1 sha1_gen;
  for (auto& p : _buffers) {
    sha1_gen.Update((const unsigned char *)p.c_str(), p.length());
  }
  sha1_gen.Final(fingerprint);
  return sha1_digest_t(fingerprint);
}

/**
 * Binary write all contents to a C++ stream
 */
void buffer::list::write_stream(std::ostream &out) const
{
  for (const auto& node : _buffers) {
    if (node.length() > 0) {
      out.write(node.c_str(), node.length());
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
    if (o + per < length()) {
      bool row_is_zeros = true;
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


buffer::list buffer::list::static_from_mem(char* c, size_t l) {
  list bl;
  bl.push_back(ptr_node::create(create_static(l, c)));
  return bl;
}

buffer::list buffer::list::static_from_cstring(char* c) {
  return static_from_mem(c, std::strlen(c));
}

buffer::list buffer::list::static_from_string(string& s) {
  // C++14 just has string::data return a char* from a non-const
  // string.
  return static_from_mem(const_cast<char*>(s.data()), s.length());
  // But the way buffer::list mostly doesn't work in a sane way with
  // const makes me generally sad.
}

bool buffer::ptr_node::dispose_if_hypercombined(
  buffer::ptr_node* const delete_this)
{
  const bool is_hypercombined = static_cast<void*>(delete_this) == \
    static_cast<void*>(&delete_this->get_raw()->bptr_storage);
  if (is_hypercombined) {
    ceph_assert_always("hypercombining is currently disabled" == nullptr);
    delete_this->~ptr_node();
  }
  return is_hypercombined;
}

std::unique_ptr<buffer::ptr_node, buffer::ptr_node::disposer>
buffer::ptr_node::create_hypercombined(ceph::unique_leakable_ptr<buffer::raw> r)
{
  // FIXME: we don't currently hypercombine buffers due to crashes
  // observed in the rados suite. After fixing we'll use placement
  // new to create ptr_node on buffer::raw::bptr_storage.
  return std::unique_ptr<buffer::ptr_node, buffer::ptr_node::disposer>(
    new ptr_node(std::move(r)));
}

std::unique_ptr<buffer::ptr_node, buffer::ptr_node::disposer>
buffer::ptr_node::create_hypercombined(buffer::raw* const r)
{
  if (likely(r->nref == 0)) {
    // FIXME: we don't currently hypercombine buffers due to crashes
    // observed in the rados suite. After fixing we'll use placement
    // new to create ptr_node on buffer::raw::bptr_storage.
    return std::unique_ptr<buffer::ptr_node, buffer::ptr_node::disposer>(
      new ptr_node(r));
  } else {
    return std::unique_ptr<buffer::ptr_node, buffer::ptr_node::disposer>(
      new ptr_node(r));
  }
}

buffer::ptr_node* buffer::ptr_node::copy_hypercombined(
  const buffer::ptr_node& copy_this)
{
  // FIXME: we don't currently hypercombine buffers due to crashes
  // observed in the rados suite. After fixing we'll use placement
  // new to create ptr_node on buffer::raw::bptr_storage.
  auto raw_new = copy_this.get_raw()->clone();
  return new ptr_node(copy_this, std::move(raw_new));
}

buffer::ptr_node* buffer::ptr_node::cloner::operator()(
  const buffer::ptr_node& clone_this)
{
  const raw* const raw_this = clone_this.get_raw();
  if (likely(!raw_this || raw_this->is_shareable())) {
    return new ptr_node(clone_this);
  } else {
    // clone non-shareable buffers (make shareable)
   return copy_hypercombined(clone_this);
  }
}

std::ostream& buffer::operator<<(std::ostream& out, const buffer::raw &r) {
  return out << "buffer::raw(" << (void*)r.data << " len " << r.len << " nref " << r.nref.load() << ")";
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

  for (const auto& node : bl.buffers()) {
    out << "\t" << node;
    if (&node != &bl.buffers().back()) {
      out << "," << std::endl;
    }
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
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_posix_aligned,
			      buffer_raw_posix_aligned, buffer_meta);
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_char, buffer_raw_char, buffer_meta);
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_claimed_char, buffer_raw_claimed_char,
			      buffer_meta);
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_unshareable, buffer_raw_unshareable,
			      buffer_meta);
MEMPOOL_DEFINE_OBJECT_FACTORY(buffer::raw_static, buffer_raw_static,
			      buffer_meta);

