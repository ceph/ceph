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
#include "include/atomic.h"
#include "include/types.h"
#include "include/compat.h"

#include <errno.h>
#include <fstream>
#include <sstream>
#include <sys/uio.h>
#include <limits.h>

namespace ceph {

#ifdef BUFFER_DEBUG
static uint32_t simple_spinlock_t buffer_debug_lock = SIMPLE_SPINLOCK_INITIALIZER;
# define bdout { simple_spin_lock(&buffer_debug_lock); std::cout
# define bendl std::endl; simple_spin_unlock(&buffer_debug_lock); }
#else
# define bdout if (0) { std::cout
# define bendl std::endl; }
#endif

atomic_t buffer_total_alloc;
bool buffer_track_alloc = get_env_bool("CEPH_BUFFER_TRACK");

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

  class buffer::raw {
  public:
    char *data;
    unsigned len;
    atomic_t nref;

    raw(unsigned l) : data(NULL), len(l), nref(0)
    { }
    raw(char *c, unsigned l) : data(c), len(l), nref(0)
    { }
    virtual ~raw() {};

    // no copying.
    raw(const raw &other);
    const raw& operator=(const raw &other);

    virtual raw* clone_empty() = 0;
    raw *clone() {
      raw *c = clone_empty();
      memcpy(c->data, data, len);
      return c;
    }

    bool is_page_aligned() {
      return ((long)data & ~CEPH_PAGE_MASK) == 0;
    }
    bool is_n_page_sized() {
      return (len & ~CEPH_PAGE_MASK) == 0;
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
  public:
    raw_posix_aligned(unsigned l) : raw(l) {
#ifdef DARWIN
      data = (char *) valloc (len);
#else
      data = 0;
      int r = ::posix_memalign((void**)(void*)&data, CEPH_PAGE_SIZE, len);
      if (r)
	throw bad_alloc();
#endif /* DARWIN */
      if (!data)
	throw bad_alloc();
      inc_total_alloc(len);
      bdout << "raw_posix_aligned " << this << " alloc " << (void *)data << " " << l << " " << buffer::get_total_alloc() << bendl;
    }
    ~raw_posix_aligned() {
      ::free((void*)data);
      dec_total_alloc(len);
      bdout << "raw_posix_aligned " << this << " free " << (void *)data << " " << buffer::get_total_alloc() << bendl;
    }
    raw* clone_empty() {
      return new raw_posix_aligned(len);
    }
  };
#endif

#ifdef __CYGWIN__
  class buffer::raw_hack_aligned : public buffer::raw {
    char *realdata;
  public:
    raw_hack_aligned(unsigned l) : raw(l) {
      realdata = new char[len+CEPH_PAGE_SIZE-1];
      unsigned off = ((unsigned)realdata) & ~CEPH_PAGE_MASK;
      if (off)
	data = realdata + CEPH_PAGE_SIZE - off;
      else
	data = realdata;
      inc_total_alloc(len+CEPH_PAGE_SIZE-1);
      //cout << "hack aligned " << (unsigned)data
      //<< " in raw " << (unsigned)realdata
      //<< " off " << off << std::endl;
      assert(((unsigned)data & (CEPH_PAGE_SIZE-1)) == 0);
    }
    ~raw_hack_aligned() {
      delete[] realdata;
      dec_total_alloc(len+CEPH_PAGE_SIZE-1);
    }
    raw* clone_empty() {
      return new raw_hack_aligned(len);
    }
  };
#endif

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

  class buffer::raw_static : public buffer::raw {
  public:
    raw_static(const char *d, unsigned l) : raw((char*)d, l) { }
    ~raw_static() {}
    raw* clone_empty() {
      return new buffer::raw_char(len);
    }
  };

  buffer::raw* buffer::copy(const char *c, unsigned len) {
    raw* r = new raw_char(len);
    memcpy(r->data, c, len);
    return r;
  }
  buffer::raw* buffer::create(unsigned len) {
    return new raw_char(len);
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
  buffer::raw* buffer::create_page_aligned(unsigned len) {
#ifndef __CYGWIN__
    //return new raw_mmap_pages(len);
    return new raw_posix_aligned(len);
#else
    return new raw_hack_aligned(len);
#endif
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

  buffer::raw *buffer::ptr::clone()
  {
    return _raw->clone();
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
	delete _raw;  // dealloc old (if any)
      }
      _raw = 0;
    }
  }

  bool buffer::ptr::at_buffer_tail() const { return _off + _len == _raw->len; }

  const char *buffer::ptr::c_str() const { assert(_raw); return _raw->data + _off; }
  char *buffer::ptr::c_str() { assert(_raw); return _raw->data + _off; }

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
    return _raw->data[_off + n];
  }
  char& buffer::ptr::operator[](unsigned n)
  {
    assert(_raw);
    assert(n < _len);
    return _raw->data[_off + n];
  }

  const char *buffer::ptr::raw_c_str() const { assert(_raw); return _raw->data; }
  unsigned buffer::ptr::raw_length() const { assert(_raw); return _raw->len; }
  int buffer::ptr::raw_nref() const { assert(_raw); return _raw->nref.read(); }

  unsigned buffer::ptr::wasted()
  {
    assert(_raw);
    return _raw->len - _len;
  }

  int buffer::ptr::cmp(const ptr& o)
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
    memcpy(c_str()+o, src, l);
  }

  void buffer::ptr::zero()
  {
    memset(c_str(), 0, _len);
  }

  void buffer::ptr::zero(unsigned o, unsigned l)
  {
    assert(o+l <= _len);
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

  void buffer::list::swap(list& other)
  {
    std::swap(_len, other._len);
    _buffers.swap(other._buffers);
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

  bool buffer::list::is_page_aligned() const
  {
    for (std::list<ptr>::const_iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it) 
      if (!it->is_page_aligned())
	return false;
    return true;
  }

  bool buffer::list::is_n_page_sized() const
  {
    for (std::list<ptr>::const_iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it) 
      if (!it->is_n_page_sized())
	return false;
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
	if (p >= o && p+it->length() <= o+l)
	  it->zero();                         // all
	else if (p >= o) 
	  it->zero(0, o+l-p);                 // head
	else
	  it->zero(o-p, it->length()-(o-p));  // tail
      }
      p += it->length();
      if (o+l <= p)
	break;  // done
    }
  }
  
  bool buffer::list::is_contiguous()
  {
    return &(*_buffers.begin()) == &(*_buffers.rbegin());
  }

  void buffer::list::rebuild()
  {
    ptr nb;
    if ((_len & ~CEPH_PAGE_MASK) == 0)
      nb = buffer::create_page_aligned(_len);
    else
      nb = buffer::create(_len);
    unsigned pos = 0;
    for (std::list<ptr>::iterator it = _buffers.begin();
	 it != _buffers.end();
	 ++it) {
      nb.copy_in(pos, it->length(), it->c_str());
      pos += it->length();
    }
    _buffers.clear();
    _buffers.push_back(nb);
  }

void buffer::list::rebuild_page_aligned()
{
  std::list<ptr>::iterator p = _buffers.begin();
  while (p != _buffers.end()) {
    // keep anything that's already page sized+aligned
    if (p->is_page_aligned() && p->is_n_page_sized()) {
      /*cout << " segment " << (void*)p->c_str()
	     << " offset " << ((unsigned long)p->c_str() & ~CEPH_PAGE_MASK)
	     << " length " << p->length()
	     << " " << (p->length() & ~CEPH_PAGE_MASK) << " ok" << std::endl;
      */
      ++p;
      continue;
    }
    
    // consolidate unaligned items, until we get something that is sized+aligned
    list unaligned;
    unsigned offset = 0;
    do {
      /*cout << " segment " << (void*)p->c_str()
	     << " offset " << ((unsigned long)p->c_str() & ~CEPH_PAGE_MASK)
	     << " length " << p->length() << " " << (p->length() & ~CEPH_PAGE_MASK)
	     << " overall offset " << offset << " " << (offset & ~CEPH_PAGE_MASK)
	     << " not ok" << std::endl;
      */
      offset += p->length();
      unaligned.push_back(*p);
      _buffers.erase(p++);
    } while (p != _buffers.end() &&
	     (!p->is_page_aligned() ||
	      !p->is_n_page_sized() ||
	      (offset & ~CEPH_PAGE_MASK)));
    unaligned.rebuild();
    _buffers.insert(p, unaligned._buffers.front());
  }
}

  // sort-of-like-assignment-op
  void buffer::list::claim(list& bl)
  {
    // free my buffers
    clear();
    claim_append(bl);
  }

  void buffer::list::claim_append(list& bl)
  {
    // steal the other guy's buffers
    _len += bl._len;
    _buffers.splice( _buffers.end(), bl._buffers );
    bl._len = 0;
    bl.last_p = bl.begin();
  }

  void buffer::list::claim_prepend(list& bl)
  {
    // steal the other guy's buffers
    _len += bl._len;
    _buffers.splice( _buffers.begin(), bl._buffers );
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
      unsigned alen = CEPH_PAGE_SIZE;
      append_buffer = create_page_aligned(alen);
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
      unsigned alen = CEPH_PAGE_SIZE * (((len-1) / CEPH_PAGE_SIZE) + 1);
      append_buffer = create_page_aligned(alen);
      append_buffer.set_length(0);   // unused, so far.
    }
  }

  void buffer::list::append(const ptr& bp)
  {
    if (bp.length())
      push_back(bp);
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
    ptr tempbp(bp, off, len);
    push_back(tempbp);
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
      append("\n", 1);
    }
  }
  
  void buffer::list::append_zero(unsigned len)
  {
    ptr bp(len);
    bp.zero();
    append(bp);
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
    assert(0);
  }

  /*
   * return a contiguous ptr to whole bufferlist contents.
   */
  char *buffer::list::c_str()
  {
    if (_buffers.empty())
      return 0;                         // no buffers

    std::list<ptr>::const_iterator iter = _buffers.begin();
    iter++;

    if (iter != _buffers.end())
      rebuild();
    return _buffers.front().c_str();  // good, we're already contiguous.
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
  };

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
    TEMP_FAILURE_RETRY(::close(fd));
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
  TEMP_FAILURE_RETRY(::close(fd));
  return 0;
}

ssize_t buffer::list::read_fd(int fd, size_t len) 
{
  int s = ROUND_UP_TO(len, CEPH_PAGE_SIZE);
  bufferptr bp = buffer::create_page_aligned(s);
  ssize_t ret = safe_read(fd, (void*)bp.c_str(), len);
  if (ret >= 0) {
    bp.set_length(ret);
    append(bp);
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
    TEMP_FAILURE_RETRY(::close(fd));
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


}
