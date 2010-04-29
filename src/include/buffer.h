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

#ifndef __BUFFER_H
#define __BUFFER_H

#include <linux/types.h>

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 600
#endif

#include <stdlib.h>
#ifdef DARWIN

#ifndef MAP_ANON
#define MAP_ANON 0x1000
#endif
#ifndef O_DIRECTORY
#define O_DIRECTORY 0x100000
void	*valloc(size_t);
#endif



#else

#include <malloc.h>
#endif
#include <stdint.h>
#include <string.h>

#ifndef __CYGWIN__
# include <sys/mman.h>
#endif

#include <iostream>
#include <istream>
#include <iomanip>
#include <list>
#include <string>
#include <exception>

using std::istream;
using std::string;

#include "atomic.h"
#include "page.h"
#include "crc32c.h"

#ifndef assert
# include <assert.h>
#endif


//#define BUFFER_DEBUG

#ifdef BUFFER_DEBUG
#include "Spinlock.h"
extern Spinlock buffer_lock;
# define bdout { buffer_lock.lock(); std::cout
# define bendl std::endl; buffer_lock.unlock(); }
#else
# define bdout if (0) { std::cout
# define bendl std::endl; }
#endif

namespace ceph {

extern atomic_t buffer_total_alloc;

class buffer {
  /*
   * exceptions
   */

public:
  struct error : public std::exception{
    const char *what() {
      return "buffer::exception";
    }
  };
  struct bad_alloc : public error {
    const char *what() { return "buffer::bad_alloc"; }
  };
  struct end_of_buffer : public error {
    const char *what() { return "buffer::end_of_buffer"; }
  };


private:
 
  /* hack for memory utilization debugging. */
  static void inc_total_alloc(unsigned len) {
    buffer_total_alloc.add(len);
  }
  static void dec_total_alloc(unsigned len) {
    buffer_total_alloc.sub(len);
  }

 

  /*
   * an abstract raw buffer.  with a reference count.
   */
  class raw {
  public:
    char *data;
    unsigned len;
    atomic_t nref;

    raw(unsigned l) : len(l), nref(0)
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
      return ((long)data & ~PAGE_MASK) == 0;
    }
    bool is_n_page_sized() {
      return (len & ~PAGE_MASK) == 0;
    }
  };

  friend std::ostream& operator<<(std::ostream& out, const raw &r);

  /*
   * primitive buffer types
   */
  class raw_char : public raw {
  public:
    raw_char(unsigned l) : raw(l) {
      if (len)
	data = new char[len];
      else
	data = 0;
      inc_total_alloc(len);
      bdout << "raw_char alloc " << (void *)data << " " << l << " " << buffer_total_alloc.read() << bendl;
    }
    raw_char(unsigned l, char *b) : raw(b, l) {
      inc_total_alloc(len);
      bdout << "raw_char alloc " << (void *)data << " " << l << " " << buffer_total_alloc.read() << bendl;
    }
    ~raw_char() {
      delete[] data;
      dec_total_alloc(len);      
      bdout << "raw_char free " << (void *)data << " " << buffer_total_alloc.read() << bendl;
    }
    raw* clone_empty() {
      return new raw_char(len);
    }
  };

  class raw_malloc : public raw {
  public:
    raw_malloc(unsigned l) : raw(l) {
      if (len)
	data = (char *)malloc(len);
      else
	data = 0;
      inc_total_alloc(len);
      bdout << "raw_malloc alloc " << (void *)data << " " << l << " " << buffer_total_alloc.read() << bendl;
    }
    raw_malloc(unsigned l, char *b) : raw(b, l) {
      inc_total_alloc(len);
      bdout << "raw_malloc alloc " << (void *)data << " " << l << " " << buffer_total_alloc.read() << bendl;
    }
    ~raw_malloc() {
      free(data);
      dec_total_alloc(len);      
      bdout << "raw_malloc free " << (void *)data << " " << buffer_total_alloc.read() << bendl;
    }
    raw* clone_empty() {
      return new raw_malloc(len);
    }
  };

  class raw_static : public raw {
  public:
    raw_static(const char *d, unsigned l) : raw((char*)d, l) { }
    ~raw_static() {}
    raw* clone_empty() {
      return new raw_char(len);
    }
  };

#ifndef __CYGWIN__
  class raw_mmap_pages : public raw {
  public:
    raw_mmap_pages(unsigned l) : raw(l) {
      data = (char*)::mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANON, -1, 0);
      if (!data)
	throw new bad_alloc;
      inc_total_alloc(len);
      bdout << "raw_mmap alloc " << (void *)data << " " << l << " " << buffer_total_alloc.read() << bendl;
    }
    ~raw_mmap_pages() {
      ::munmap(data, len);
      dec_total_alloc(len);
      bdout << "raw_mmap free " << (void *)data << " " << buffer_total_alloc.read() << bendl;
    }
    raw* clone_empty() {
      return new raw_mmap_pages(len);
    }
  };

  class raw_posix_aligned : public raw {
  public:
    raw_posix_aligned(unsigned l) : raw(l) {
#ifdef DARWIN
      data = (char *) valloc (len);
#else
      data = 0;
      int r = ::posix_memalign((void**)(void*)&data, PAGE_SIZE, len);
      if (r)
	throw new bad_alloc;
#endif /* DARWIN */
      if (!data)
	throw new bad_alloc;
      inc_total_alloc(len);
      bdout << "raw_posix_aligned alloc " << (void *)data << " " << l << " " << buffer_total_alloc.read() << bendl;
    }
    ~raw_posix_aligned() {
      ::free((void*)data);
      dec_total_alloc(len);
      bdout << "raw_posix_aligned free " << (void *)data << " " << buffer_total_alloc.read() << bendl;
    }
    raw* clone_empty() {
      return new raw_posix_aligned(len);
    }
  };
#endif

#ifdef __CYGWIN__
  class raw_hack_aligned : public raw {
    char *realdata;
  public:
    raw_hack_aligned(unsigned l) : raw(l) {
      realdata = new char[len+PAGE_SIZE-1];
      unsigned off = ((unsigned)realdata) & ~PAGE_MASK;
      if (off) 
	data = realdata + PAGE_SIZE - off;
      else
	data = realdata;
      inc_total_alloc(len+PAGE_SIZE-1);
      //cout << "hack aligned " << (unsigned)data 
      //<< " in raw " << (unsigned)realdata
      //<< " off " << off << std::endl;
      assert(((unsigned)data & (PAGE_SIZE-1)) == 0);
    }
    ~raw_hack_aligned() {
      delete[] realdata;
      dec_total_alloc(len+PAGE_SIZE-1);
    }
    raw* clone_empty() {
      return new raw_hack_aligned(len);
    }
  };
#endif

public:

  /*
   * named constructors 
   */

  static raw* copy(const char *c, unsigned len) {
    raw* r = new raw_char(len);
    memcpy(r->data, c, len);
    return r;
  }
  static raw* create(unsigned len) {
    return new raw_char(len);
  }
  static raw* claim_char(unsigned len, char *buf) {
    return new raw_char(len, buf);
  }
  static raw* create_malloc(unsigned len) {
    return new raw_malloc(len);
  }
  static raw* claim_malloc(unsigned len, char *buf) {
    return new raw_malloc(len, buf);
  }
  static raw* create_static(unsigned len, char *buf) {
    return new raw_static(buf, len);
  }

  static raw* create_page_aligned(unsigned len) {
#ifndef __CYGWIN__
    //return new raw_mmap_pages(len);
    return new raw_posix_aligned(len);
#else
    return new raw_hack_aligned(len);
#endif
  }
  
  
  /*
   * a buffer pointer.  references (a subsequence of) a raw buffer.
   */
  class ptr {
    raw *_raw;
    unsigned _off, _len;

  public:
    ptr() : _raw(0), _off(0), _len(0) {}
    ptr(raw *r) : _raw(r), _off(0), _len(r->len) {   // no lock needed; this is an unref raw.
      r->nref.inc();
    }
    ptr(unsigned l) : _off(0), _len(l) {
      _raw = create(l);
      _raw->nref.inc();
    }
    ptr(const char *d, unsigned l) : _off(0), _len(l) {    // ditto.
      _raw = copy(d, l);
      _raw->nref.inc();
    }
    ptr(const ptr& p) : _raw(p._raw), _off(p._off), _len(p._len) {
      if (_raw) {
	_raw->nref.inc();
      }
    }
    ptr(const ptr& p, unsigned o, unsigned l) : _raw(p._raw), _off(p._off + o), _len(l) {
      assert(o+l <= p._len);
      assert(_raw);
      _raw->nref.inc();
    }
    ptr& operator= (const ptr& p) {
      // be careful -- we need to properly handle self-assignment.
      if (p._raw)
	p._raw->nref.inc();                      // inc new
      release();                                 // dec (+ dealloc) old (if any)
      if (p._raw) {
	_raw = p._raw;
	_off = p._off;
	_len = p._len;
      } else {
	_off = _len = 0;
      }
      return *this;
    }
    ~ptr() {
      release();
    }
    
    bool have_raw() const { return _raw ? true:false; }

    raw *clone() {
      return _raw->clone();
    }
    
    void clone_in_place() {
      raw *newraw = _raw->clone();
      release();
      newraw->nref.inc();
      _raw = newraw;
    }
    bool do_cow() {
      if (_raw->nref.read() > 1) {
	//std::cout << "doing cow on " << _raw << " len " << _len << std::endl;
	clone_in_place();
	return true;
      } else
	return false;
    }

    void swap(ptr& other) {
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

    void release() {
      if (_raw) {
        if (_raw->nref.dec() == 0) {
          //cout << "hosing raw " << (void*)_raw << " len " << _raw->len << std::endl;
          delete _raw;  // dealloc old (if any)
	}
	_raw = 0;
      }
    }

    // misc
    bool at_buffer_head() const { return _off == 0; }
    bool at_buffer_tail() const { return _off + _len == _raw->len; }

    bool is_page_aligned() const { return ((long)c_str() & ~PAGE_MASK) == 0; }
    bool is_n_page_sized() const { return (length() & ~PAGE_MASK) == 0; }

    // accessors
    raw *get_raw() const { return _raw; }
    const char *c_str() const { assert(_raw); return _raw->data + _off; }
    char *c_str() { assert(_raw); return _raw->data + _off; }
    unsigned length() const { return _len; }
    unsigned offset() const { return _off; }
    unsigned start() const { return _off; }
    unsigned end() const { return _off + _len; }
    unsigned unused_tail_length() const { 
      if (_raw)
	return _raw->len - (_off+_len); 
      else
	return 0;
    }
    const char& operator[](unsigned n) const { 
      assert(_raw); 
      assert(n < _len);
      return _raw->data[_off + n];
    }
    char& operator[](unsigned n) { 
      assert(_raw); 
      assert(n < _len);
      return _raw->data[_off + n];
    }

    const char *raw_c_str() const { assert(_raw); return _raw->data; }
    unsigned raw_length() const { assert(_raw); return _raw->len; }
    int raw_nref() const { assert(_raw); return _raw->nref.read(); }

    void copy_out(unsigned o, unsigned l, char *dest) const {
      assert(_raw);
      if (!((o >= 0 && o <= _len) &&
	    (l >= 0 && o+l <= _len)))
	throw new end_of_buffer;
      memcpy(dest, c_str()+o, l);
    }

    unsigned wasted() {
      assert(_raw);
      return _raw->len - _len;
    }

    int cmp(const ptr& o) {
      int l = _len < o._len ? _len : o._len;
      if (l) {
	int r = memcmp(c_str(), o.c_str(), l);
	if (!r)
	  return r;
      }
      if (_len < o._len)
	return -1;
      if (_len > o._len)
	return 1;
      return 0;
    }

    // modifiers
    void set_offset(unsigned o) { _off = o; }
    void set_length(unsigned l) { _len = l; }

    void append(char c) {
      assert(_raw);
      assert(1 <= unused_tail_length());
      (c_str())[_len] = c;
      _len++;
    }
    void append(const char *p, unsigned l) {
      assert(_raw);
      assert(l <= unused_tail_length());
      memcpy(c_str() + _len, p, l);
      _len += l;
    }
    
    void copy_in(unsigned o, unsigned l, const char *src) {
      assert(_raw);
      assert(o >= 0 && o <= _len);
      assert(l >= 0 && o+l <= _len);
      memcpy(c_str()+o, src, l);
    }

    void zero() {
      memset(c_str(), 0, _len);
    }
    void zero(unsigned o, unsigned l) {
      assert(o+l <= _len);
      memset(c_str()+o, 0, l);
    }

    void clean() {
      //raw *newraw = _raw->makesib(_len);
    }
  };

  friend std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp);

  /*
   * list - the useful bit!
   */

  class list {
    // my private bits
    std::list<ptr> _buffers;
    unsigned _len;

    ptr append_buffer;  // where i put small appends.

  public:
    class iterator {
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

      iterator(const iterator& other) : bl(other.bl),
					ls(other.ls),
					off(other.off),
					p(other.p),
					p_off(other.p_off) {}
      iterator operator=(const iterator& other) {
	if (this != &other) {
	  bl = other.bl;
	  ls = other.ls;
	  off = other.off;
	  p = other.p;
	  p_off = other.p_off;
	}
	return *this;
      }

      unsigned get_off() { return off; }

      bool end() {
	return p == ls->end();
	//return off == bl->length();
      }

      void advance(unsigned o) {
	//cout << this << " advance " << o << " from " << off << " (p_off " << p_off << " in " << p->length() << ")" << std::endl;
	p_off += o;
	while (p_off > 0) {
	  if (p == ls->end())
	    throw new end_of_buffer;
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
      }

      void seek(unsigned o) {
	//cout << this << " seek " << o << std::endl;
	p = ls->begin();
	off = p_off = 0;
	advance(o);
      }

      char operator*() {
	if (p == ls->end())
	  throw new end_of_buffer;
	return (*p)[p_off];
      }
      iterator& operator++() {
	if (p == ls->end())
	  throw new end_of_buffer;
	advance(1);
	return *this;
      }

      // copy data out.
      // note that these all _append_ to dest!

      void copy(unsigned len, char *dest) {
	if (p == ls->end()) seek(off);
	while (len > 0) {
	  if (p == ls->end())
	    throw new end_of_buffer;

	  unsigned howmuch = p->length() - p_off;
	  if (len < howmuch) howmuch = len;
	  p->copy_out(p_off, howmuch, dest);
	  dest += howmuch;

	  len -= howmuch;
	  advance(howmuch);
	}
      }

      void copy(unsigned len, ptr &dest) {
	dest = create(len);
	copy(len, dest.c_str());
      }

      void copy(unsigned len, list &dest) {
	if (p == ls->end()) seek(off);
	while (len > 0) {
	  if (p == ls->end())
	    throw new end_of_buffer;
	  
	  unsigned howmuch = p->length() - p_off;
	  if (len < howmuch) howmuch = len;
	  dest.append(*p, p_off, howmuch);
	  
	  len -= howmuch;
	  advance(howmuch);
	}
      }

      void copy(unsigned len, std::string &dest) {
	if (p == ls->end()) seek(off);
	while (len > 0) {
	  if (p == ls->end())
	    throw new end_of_buffer;

	  unsigned howmuch = p->length() - p_off;
	  if (len < howmuch) howmuch = len;
	  dest.append(p->c_str() + p_off, howmuch);

	  len -= howmuch;
	  advance(howmuch);
	}
      }

      // copy data in

      void copy_in(unsigned len, const char *src) {
	// copy
	if (p == ls->end()) seek(off);
	while (len > 0) {
	  if (p == ls->end())
	    throw new end_of_buffer;

	  unsigned howmuch = p->length() - p_off;
	  if (len < howmuch) howmuch = len;
	  p->copy_in(p_off, howmuch, src);
	
	  src += howmuch;
	  len -= howmuch;
	  advance(howmuch);
	}
      }

      void copy_in(unsigned len, const list& otherl) {
	if (p == ls->end()) seek(off);
	unsigned left = len;
	for (std::list<ptr>::const_iterator i = otherl._buffers.begin();
	     i != otherl._buffers.end();
	     i++) {
	  unsigned l = (*i).length();
	  if (left < l) l = left;
	  copy_in(l, i->c_str());
	  left -= l;
	  if (left == 0) break;
	}
      }

    };

  private:
    mutable iterator last_p;

  public:
    // cons/des
    list() : _len(0), last_p(this) {}
    list(unsigned prealloc) : _len(0), last_p(this) {
      append_buffer = buffer::create(prealloc);
      append_buffer.set_length(0);   // unused, so far.
    }
    ~list() {}
    
    list(const list& other) : _buffers(other._buffers), _len(other._len), last_p(this) { }
    list& operator= (const list& other) {
      _buffers = other._buffers;
      _len = other._len;
      return *this;
    }

    const std::list<ptr>& buffers() const { return _buffers; }
    
    void swap(list& other) {
      unsigned t = _len;
      _len = other._len;
      other._len = t;
      _buffers.swap(other._buffers);
      append_buffer.swap(other.append_buffer);
      //last_p.swap(other.last_p);
      last_p = begin();
      other.last_p = other.begin();
    }

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

    bool is_page_aligned() const {
      for (std::list<ptr>::const_iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++) 
	if (!it->is_page_aligned()) return false;
      return true;
    }
    bool is_n_page_sized() const {
      for (std::list<ptr>::const_iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++) 
	if (!it->is_n_page_sized()) return false;
      return true;
    }

    // modifiers
    void clear() {
      _buffers.clear();
      _len = 0;
      last_p = begin();
    }
    void push_front(ptr& bp) {
      _buffers.push_front(bp);
      _len += bp.length();
    }
    void push_front(raw *r) {
      ptr bp(r);
      _buffers.push_front(bp);
      _len += bp.length();
    }
    void push_back(const ptr& bp) {
      _buffers.push_back(bp);
      _len += bp.length();
    }
    void push_back(raw *r) {
      ptr bp(r);
      _buffers.push_back(bp);
      _len += bp.length();
    }
    void zero() {
      for (std::list<ptr>::iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++)
        it->zero();
    }
    void do_cow() {
      for (std::list<ptr>::iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++)
	it->do_cow();
    }
    void clone_in_place() {
      for (std::list<ptr>::iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++)
	it->clone_in_place();
    }
    void zero(unsigned o, unsigned l) {
      assert(o+l <= _len);
      unsigned p = 0;
      for (std::list<ptr>::iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++) {
	if (p + it->length() > o) {
	  if (p >= o && p+it->length() >= o+l)
	    it->zero();                         // all
	  else if (p >= o) 
	    it->zero(0, o+l-p);                 // head
	  else
	    it->zero(o-p, it->length()-(o-p));  // tail
	}
	p += it->length();
	if (o+l >= p) break;  // done
      }
    }

    bool is_contiguous() {
      return &(*_buffers.begin()) == &(*_buffers.rbegin());
    }
    void rebuild() {
      ptr nb;
      if ((_len & ~PAGE_MASK) == 0)
	nb = buffer::create_page_aligned(_len);
      else
	nb = buffer::create(_len);
      unsigned pos = 0;
      for (std::list<ptr>::iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++) {
	nb.copy_in(pos, it->length(), it->c_str());
	pos += it->length();
      }
      _buffers.clear();
      _buffers.push_back(nb);
    }


    // sort-of-like-assignment-op
    void claim(list& bl) {
      // free my buffers
      clear();
      claim_append(bl);
    }
    void claim_append(list& bl) {
      // steal the other guy's buffers
      _len += bl._len;
      _buffers.splice( _buffers.end(), bl._buffers );
      bl._len = 0;
      bl.last_p = bl.begin();
    }


    

    iterator begin() {
      return iterator(this, 0);
    }
    iterator end() {
      return iterator(this, _len, _buffers.end(), 0);
    }


    // crope lookalikes.
    // **** WARNING: this are horribly inefficient for large bufferlists. ****

    // data OUT

    void copy(unsigned off, unsigned len, char *dest) const {
      assert(off >= 0);
      if (off + len > length())
	throw new end_of_buffer;
      if (last_p.get_off() != off) 
	last_p.seek(off);
      last_p.copy(len, dest);
    }

    void copy(unsigned off, unsigned len, list &dest) const {
      assert(off >= 0);
      if (off + len > length())
	throw new end_of_buffer;
      if (last_p.get_off() != off) 
	last_p.seek(off);
      last_p.copy(len, dest);
    }

    void copy(unsigned off, unsigned len, std::string& dest) const {
      if (last_p.get_off() != off) 
	last_p.seek(off);
      return last_p.copy(len, dest);
    }
    
    void copy_in(unsigned off, unsigned len, const char *src) {
      assert(off >= 0);
      if (off + len > length())
	throw new end_of_buffer;
      
      if (last_p.get_off() != off) 
	last_p.seek(off);
      last_p.copy_in(len, src);
    }

    void copy_in(unsigned off, unsigned len, const list& src) {
      if (last_p.get_off() != off) 
	last_p.seek(off);
      last_p.copy_in(len, src);
    }


    void append(char c) {
      // put what we can into the existing append_buffer.
      unsigned gap = append_buffer.unused_tail_length();
      if (!gap) {
	// make a new append_buffer!
	unsigned alen = PAGE_SIZE;
	append_buffer = create_page_aligned(alen);
	append_buffer.set_length(0);   // unused, so far.
      }
      append_buffer.append(c);
      append(append_buffer, append_buffer.end() - 1, 1);	// add segment to the list
    }
    void append(const char *data, unsigned len) {
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
	if (len == 0) break;  // done!
	
	// make a new append_buffer!
	unsigned alen = PAGE_SIZE * (((len-1) / PAGE_SIZE) + 1);
	append_buffer = create_page_aligned(alen);
	append_buffer.set_length(0);   // unused, so far.
      }
    }
    void append(const string& s) {
      append(s.data(), s.length());
    }
    void append(const ptr& bp) {
      if (bp.length())
	push_back(bp);
    }
    void append(const ptr& bp, unsigned off, unsigned len) {
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
    void append(const list& bl) {
      _len += bl._len;
      for (std::list<ptr>::const_iterator p = bl._buffers.begin();
	   p != bl._buffers.end();
	   ++p) 
	_buffers.push_back(*p);
    }
    void append(istream& in) {
      while (!in.eof()) {
	string s;
	getline(in, s);
	append(s.c_str(), s.length());
	append("\n", 1);
      }
    }
    
    void append_zero(unsigned len) {
      ptr bp(len);
      bp.zero();
      append(bp);
    }

    
    /*
     * get a char
     */
    const char& operator[](unsigned n) const {
      if (n >= _len)
	throw new end_of_buffer;

      for (std::list<ptr>::const_iterator p = _buffers.begin();
	   p != _buffers.end();
	   p++) {
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
    char *c_str() {
      if (_buffers.size() == 0) 
	return 0;                         // no buffers
      if (_buffers.size() > 1) 
	rebuild();
      assert(_buffers.size() == 1);
      return _buffers.front().c_str();  // good, we're already contiguous.
    }

    void substr_of(const list& other, unsigned off, unsigned len) {
      if (off + len > other.length())
	throw new end_of_buffer;

      clear();
      
      // skip off
      std::list<ptr>::const_iterator curbuf = other._buffers.begin();
      while (off > 0 &&
	     off >= curbuf->length()) {
	// skip this buffer
	//cout << "skipping over " << *curbuf << std::endl;
	off -= (*curbuf).length();
	curbuf++;
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
	curbuf++;
      }
    }

    

    // funky modifer
    void splice(unsigned off, unsigned len, list *claim_by=0 /*, bufferlist& replace_with */) {    // fixme?
      if (off >= length())
	throw new end_of_buffer;

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
	  curbuf++;
	} else {
	  // somewhere in this buffer!
	  //cout << "off = " << off << " somewhere in " << *curbuf << std::endl;
	  break;
	}
      }
      assert(off >= 0);
      
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

    void write(int off, int len, std::ostream& out) const {
      list s;
      s.substr_of(*this, off, len);
      for (std::list<ptr>::const_iterator it = s._buffers.begin(); 
	   it != s._buffers.end(); 
	   it++)
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

    void encode_base64(list& o);
    void decode_base64(list& o);

    void hexdump(std::ostream &out) const;
    int read_file(const char *fn, bool silent=false);
    int write_file(const char *fn, int mode=0644);
    int write_fd(int fd);
    __u32 crc32c(__u32 crc) {
      for (std::list<ptr>::const_iterator it = _buffers.begin(); 
	   it != _buffers.end(); 
	   it++)
	if (it->length())
	  crc = ceph_crc32c_le(crc, (unsigned char*)it->c_str(), it->length());
      return crc;
    }

  };
};

typedef buffer::ptr bufferptr;
typedef buffer::list bufferlist;


inline bool operator>(bufferlist& l, bufferlist& r) {
  for (unsigned p = 0; ; p++) {
    if (l.length() > p && r.length() == p) return true;
    if (l.length() == p) return false;
    if (l[p] > r[p]) return true;
    if (l[p] < r[p]) return false;
    p++;
  }
}
inline bool operator>=(bufferlist& l, bufferlist& r) {
  for (unsigned p = 0; ; p++) {
    if (l.length() > p && r.length() == p) return true;
    if (r.length() == p && l.length() == p) return true;
    if (l[p] > r[p]) return true;
    if (l[p] < r[p]) return false;
    p++;
  }
}
inline bool operator<(bufferlist& l, bufferlist& r) {
  return r > l;
}
inline bool operator<=(bufferlist& l, bufferlist& r) {
  return r >= l;
}


inline std::ostream& operator<<(std::ostream& out, const buffer::raw &r) {
  return out << "buffer::raw(" << (void*)r.data << " len " << r.len << " nref " << r.nref.read() << ")";
}

inline std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp) {
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

inline std::ostream& operator<<(std::ostream& out, const buffer::list& bl) {
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

inline std::ostream& operator<<(std::ostream& out, buffer::error& e)
{
  return out << e.what();
}

}

#endif
