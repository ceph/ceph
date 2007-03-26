// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "common/Mutex.h"

#include <iostream>
#include <list>

using std::cout;
using std::endl;

#ifndef __CYGWIN__
# include <sys/mman.h>
#endif

#define BUFFER_PAGE_SIZE 4096  // fixme.

// <hack>
//  these are in config.o
extern Mutex bufferlock;
extern long buffer_total_alloc;
// </hack>

class buffer {
private:
  
  /* hack for memory utilization debugging. */
  static void inc_total_alloc(unsigned len) {
    bufferlock.Lock();
    buffer_total_alloc += len;
    bufferlock.Unlock();
  }
  static void dec_total_alloc(unsigned len) {
    bufferlock.Lock();
    buffer_total_alloc -= len;
    bufferlock.Unlock();
  }

  /*
   * an abstract raw buffer.  with a reference count.
   */
  class raw {
  public:
    char *data;
    unsigned len;
    int nref;
    Mutex lock;  // we'll make it non-recursive.

    raw(unsigned l) : len(l), nref(0), lock(false) {}
    raw(char *c, unsigned l) : data(c), len(l), nref(0), lock(false) {}
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
  };

  friend std::ostream& operator<<(std::ostream& out, const raw &r);

  /*
   * primitive buffer types
   */
  class raw_char : public raw {
  public:
    raw_char(unsigned l) : raw(l) {
      data = new char[len];
      inc_total_alloc(len);
    }
    ~raw_char() {
      delete[] data;
      dec_total_alloc(len);      
    }
    raw* clone_empty() {
      return new raw_char(len);
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
      inc_total_alloc(len);
    }
    ~raw_mmap_pages() {
      ::munmap(data, len);
      dec_total_alloc(len);
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
      ::posix_memalign((void**)&data, BUFFER_PAGE_SIZE, len);
#endif /* DARWIN */
      inc_total_alloc(len);
    }
    ~raw_posix_aligned() {
      ::free((void*)data);
      dec_total_alloc(len);
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
      realdata = new char[len+4095];
      unsigned off = ((unsigned)realdata) % 4096;
      if (off) 
	data = realdata + 4096 - off;
      else
	data = realdata;
      inc_total_alloc(len+4095);
      //cout << "hack aligned " << (unsigned)data 
      //<< " in raw " << (unsigned)realdata
      //<< " off " << off << endl;
      assert(((unsigned)data & 4095) == 0);
    }
    ~raw_hack_aligned() {
      delete[] realdata;
      dec_total_alloc(len+4095);
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

  static raw* create_page_aligned(unsigned len) {
#ifndef __CYGWIN__
    return new raw_mmap_pages(len);
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
      ++r->nref;
    }
    ptr(unsigned l) : _off(0), _len(l) {
      _raw = create(l);
      ++_raw->nref;
    }
    ptr(char *d, unsigned l) : _off(0), _len(l) {    // ditto.
      _raw = copy(d, l);
      ++_raw->nref;
    }
    ptr(const ptr& p) : _raw(p._raw), _off(p._off), _len(p._len) {
      if (_raw) {
	_raw->lock.Lock();
	++_raw->nref;
	_raw->lock.Unlock();
      }
    }
    ptr(const ptr& p, unsigned o, unsigned l) : _raw(p._raw), _off(p._off + o), _len(l) {
      assert(o+l <= p._len);
      assert(_raw);
      _raw->lock.Lock();
      ++_raw->nref;
      _raw->lock.Unlock();
    }
    ptr& operator= (const ptr& p) {
      // be careful -- we need to properly handle self-assignment.
      if (p._raw) {
	p._raw->lock.Lock();
	++p._raw->nref;                              // inc new
	p._raw->lock.Unlock();
      }
      release();                                 // dec (+ dealloc) old (if any)
      _raw = p._raw;                               // change my ref
      _off = p._off;
      _len = p._len;
      return *this;
    }
    ~ptr() {
      release();
    }

    void release() {
      if (_raw) {
	_raw->lock.Lock();
	if (--_raw->nref == 0) {
	  //cout << "hosing raw " << (void*)_raw << " len " << _raw->len << std::endl;
	  _raw->lock.Unlock();	  
	  delete _raw;  // dealloc old (if any)
	} else
	  _raw->lock.Unlock();	  
	_raw = 0;
      }
    }

    // misc
    bool at_buffer_head() const { return _off == 0; }
    bool at_buffer_tail() const { return _off + _len == _raw->len; }

    // accessors
    const char *c_str() const { assert(_raw); return _raw->data + _off; }
    char *c_str() { assert(_raw); return _raw->data + _off; }
    unsigned length() const { return _len; }
    unsigned offset() const { return _off; }
    unsigned unused_tail_length() const { return _raw->len - (_off+_len); }
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
    int raw_nref() const { assert(_raw); return _raw->nref; }

    void copy_out(unsigned o, unsigned l, char *dest) const {
      assert(_raw);
      assert(o >= 0 && o <= _len);
      assert(l >= 0 && o+l <= _len);
      memcpy(dest, c_str()+o, l);
    }

    unsigned wasted() {
      assert(_raw);
      return _raw->len - _len;
    }

    // modifiers
    void set_offset(unsigned o) { _off = o; }
    void set_length(unsigned l) { _len = l; }

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

  public:
    // cons/des
    list() : _len(0) {}
    list(const list& other) : _buffers(other._buffers), _len(other._len) { }
    list(unsigned l) : _len(0) {
      ptr bp(l);
      push_back(bp);
    }
    ~list() {}
    
    list& operator= (const list& other) {
      _buffers = other._buffers;
      _len = other._len;
      return *this;
    }

    const std::list<ptr>& buffers() const { return _buffers; }
    
    unsigned length() const {
#if 0
      // DEBUG: verify _len
      unsigned len = 0;
      for (std::list<ptr>::iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++) {
	len += (*it).length();
      }
      assert(len == _len);
#endif
      return _len;
    }


    // modifiers
    void clear() {
      _buffers.clear();
      _len = 0;
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
    void push_back(ptr& bp) {
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
    }
    
    // crope lookalikes
    void copy(unsigned off, unsigned len, char *dest) {
      assert(off >= 0);
      assert(off + len <= length());
      /*assert(off < length());
	if (off + len > length()) 
	len = length() - off;
      */
      // advance to off
      std::list<ptr>::iterator curbuf = _buffers.begin();
      
      // skip off
      while (off > 0) {
	assert(curbuf != _buffers.end());
	if (off >= (*curbuf).length()) {
	  // skip this buffer
	  off -= (*curbuf).length();
	  curbuf++;
	} else {
	  // somewhere in this buffer!
	  break;
	}
      }
      
      // copy
      while (len > 0) {
	// is the rest ALL in this buffer?
	if (off + len <= (*curbuf).length()) {
	  (*curbuf).copy_out(off, len, dest);        // yup, last bit!
	  break;
	}
	
	// get as much as we can from this buffer.
	unsigned howmuch = (*curbuf).length() - off;
	(*curbuf).copy_out(off, howmuch, dest);
	
	dest += howmuch;
	len -= howmuch;
	off = 0;
	curbuf++;
	assert(curbuf != _buffers.end());
      }
    }
    
    void copy_in(unsigned off, unsigned len, const char *src) {
      assert(off >= 0);
      assert(off + len <= length());
      
      // advance to off
      std::list<ptr>::iterator curbuf = _buffers.begin();
      
      // skip off
      while (off > 0) {
	assert(curbuf != _buffers.end());
	if (off >= (*curbuf).length()) {
	  // skip this buffer
	  off -= (*curbuf).length();
	  curbuf++;
	} else {
	  // somewhere in this buffer!
	  break;
	}
      }
      
      // copy
      while (len > 0) {
	// is the rest ALL in this buffer?
	if (off + len <= (*curbuf).length()) {
	  (*curbuf).copy_in(off, len, src);        // yup, last bit!
	  break;
	}
	
	// get as much as we can from this buffer.
	unsigned howmuch = (*curbuf).length() - off;
	(*curbuf).copy_in(off, howmuch, src);
	
	src += howmuch;
	len -= howmuch;
	off = 0;
	curbuf++;
	assert(curbuf != _buffers.end());
      }
    }
    void copy_in(unsigned off, unsigned len, const list& bl) {
      unsigned left = len;
      for (std::list<ptr>::const_iterator i = bl._buffers.begin();
	   i != bl._buffers.end();
	   i++) {
	unsigned l = (*i).length();
	if (left < l) l = left;
	copy_in(off, l, (*i).c_str());
	left -= l;
	if (left == 0) break;
	off += l;
      }
    }


    void append(const char *data, unsigned len) {
      if (len == 0) return;
      
      unsigned alen = 0;
      
      // copy into the tail buffer?
      if (!_buffers.empty()) {
	unsigned avail = _buffers.back().unused_tail_length();
	if (avail > 0) {
	  //std::cout << "copying up to " << len << " into tail " << avail << " bytes of tail buf " << _buffers.back() << std::endl;
	  if (avail > len) 
	    avail = len;
	  _buffers.back().append(data, avail);
	  _len += avail;
	  data += avail;
	  len -= avail;
	}
	alen = _buffers.back().length();
      }
      if (len == 0) return;
      
      // just add another buffer.
      // alloc a bit extra, in case we do a bunch of appends.   FIXME be smarter!
      if (alen < 4096) alen = 4096;
      ptr bp = create(alen);
      bp.set_length(len);
      bp.copy_in(0, len, data);
      push_back(bp);
    }
    void append(ptr& bp) {
      push_back(bp);
    }
    void append(ptr& bp, unsigned off, unsigned len) {
      assert(len+off <= bp.length());
      ptr tempbp(bp, off, len);
      push_back(tempbp);
    }
    void append(const list& bl) {
      list temp(bl);         // copy list
      claim_append(temp);    // and append
    }
    
    
    /*
     * get a char
     */
    const char& operator[](unsigned n) {
      assert(n < _len);
      for (std::list<ptr>::iterator p = _buffers.begin();
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
      if (_buffers.size() == 1) {
	return _buffers.front().c_str();  // good, we're already contiguous.
      }
      else if (_buffers.size() == 0) {
	return 0;                         // no buffers
      } 
      else {
	ptr newbuf = create(length());	     // make one new contiguous buffer.
	copy(0, length(), newbuf.c_str());   // copy myself into it.
	clear();
	push_back(newbuf);
	return newbuf.c_str();	// now it'll work.
      }
    }

    void substr_of(list& other, unsigned off, unsigned len) {
      assert(off + len <= other.length());
      clear();
      
      // skip off
      std::list<ptr>::iterator curbuf = other._buffers.begin();
      while (off > 0) {
	assert(curbuf != _buffers.end());
	if (off >= (*curbuf).length()) {
	  // skip this buffer
	  //cout << "skipping over " << *curbuf << endl;
	  off -= (*curbuf).length();
	  curbuf++;
	} else {
	  // somewhere in this buffer!
	  //cout << "somewhere in " << *curbuf << endl;
	  break;
	}
      }
      
      while (len > 0) {
	// partial?
	if (off + len < (*curbuf).length()) {
	  //cout << "copying partial of " << *curbuf << endl;
	  _buffers.push_back( ptr( *curbuf, off, len ) );
	  _len += len;
	  break;
	}
	
	// through end
	//cout << "copying end (all?) of " << *curbuf << endl;
	unsigned howmuch = (*curbuf).length() - off;
	_buffers.push_back( ptr( *curbuf, off, howmuch ) );
	_len += howmuch;
	len -= howmuch;
	off = 0;
	curbuf++;
      }
    }


    // funky modifer
    void splice(unsigned off, unsigned len, list *claim_by=0 /*, bufferlist& replace_with */) {    // fixme?
      assert(off < length()); 
      assert(len > 0);
      //cout << "splice off " << off << " len " << len << " ... mylen = " << length() << endl;
      
      // skip off
      std::list<ptr>::iterator curbuf = _buffers.begin();
      while (off > 0) {
	assert(curbuf != _buffers.end());
	if (off >= (*curbuf).length()) {
	  // skip this buffer
	  //cout << "off = " << off << " skipping over " << *curbuf << endl;
	  off -= (*curbuf).length();
	  curbuf++;
	} else {
	  // somewhere in this buffer!
	  //cout << "off = " << off << " somewhere in " << *curbuf << endl;
	  break;
	}
      }
      assert(off >= 0);
      
      if (off) {
	// add a reference to the front bit
	//  insert it before curbuf (which we'll hose)
	//cout << "keeping front " << off << " of " << *curbuf << endl;
	_buffers.insert( curbuf, ptr( *curbuf, 0, off ) );
	_len += off;
      }
      
      while (len > 0) {
	// partial?
	if (off + len < (*curbuf).length()) {
	  //cout << "keeping end of " << *curbuf << ", losing first " << off+len << endl;
	  if (claim_by) 
	    claim_by->append( *curbuf, off, len );
	  (*curbuf).set_offset( off+len + (*curbuf).offset() );    // ignore beginning big
	  (*curbuf).set_length( (*curbuf).length() - (len+off) );
	  _len -= off+len;
	  //cout << " now " << *curbuf << endl;
	  break;
	}
	
	// hose though the end
	unsigned howmuch = (*curbuf).length() - off;
	//cout << "discarding " << howmuch << " of " << *curbuf << endl;
	if (claim_by) 
	  claim_by->append( *curbuf, off, howmuch );
	_len -= (*curbuf).length();
	_buffers.erase( curbuf++ );
	len -= howmuch;
	off = 0;
      }
      
      // splice in *replace (implement me later?)
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
  return out << "buffer::raw(" << (void*)r.data << " len " << r.len << " nref " << r.nref << ")";
}

inline std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp) {
  out << "buffer::ptr(" << bp.offset() << "~" << bp.length()
      << " " << (void*)bp.c_str() 
      << " in raw " << (void*)bp.raw_c_str()
      << " len " << bp.raw_length()
      << " nref " << bp.raw_nref() << ")";
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




// encoder/decode helpers

// -- basic types --
// string
inline void _encode(const std::string& s, bufferlist& bl) 
{
  bl.append(s.c_str(), s.length()+1);
}
inline void _decode(std::string& s, bufferlist& bl, int& off)
{
  s = bl.c_str() + off;
  off += s.length() + 1;
}

// bufferptr (encapsulated)
inline void _encode(bufferptr& bp, bufferlist& bl) 
{
  size_t len = bp.length();
  bl.append((char*)&len, sizeof(len));
  bl.append(bp);
}
inline void _decode(bufferptr& bp, bufferlist& bl, int& off)
{
  size_t len;
  bl.copy(off, sizeof(len), (char*)&len);
  off += sizeof(len);
  bufferlist s;
  s.substr_of(bl, off, len);
  off += len;

  if (s.buffers().size() == 1)
    bp = s.buffers().front();
  else
    bp = buffer::copy(s.c_str(), s.length());
}

// bufferlist (encapsulated)
inline void _encode(const bufferlist& s, bufferlist& bl) 
{
  size_t len = s.length();
  bl.append((char*)&len, sizeof(len));
  bl.append(s);
}
inline void _decode(bufferlist& s, bufferlist& bl, int& off)
{
  size_t len;
  bl.copy(off, sizeof(len), (char*)&len);
  off += sizeof(len);
  s.substr_of(bl, off, len);
  off += len;
}


#include <set>
#include <deque>
#include <map>
#include <vector>
#include <string>

// set<string>
inline void _encode(const std::set<std::string>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (std::set<std::string>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    ::_encode(*it, bl);
    n--;
  }
  assert(n==0);
}
inline void _decode(std::set<std::string>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    std::string v;
    ::_decode(v, bl, off);
    s.insert(v);
  }
  assert(s.size() == (unsigned)n);
}

// list<bufferlist>
inline void _encode(const std::list<bufferlist>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (std::list<bufferlist>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    ::_encode(*it, bl);
    n--;
  }
  assert(n==0);
}
inline void _decode(std::list<bufferlist>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    bufferlist v;
    s.push_back(v);
    ::_decode(s.back(), bl, off);
  }
  //assert(s.size() == (unsigned)n);
}


// set<T>
template<class T>
inline void _encode(const std::set<T>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::set<T>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T v = *it;
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class T>
inline void _decode(std::set<T>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T v;
    bl.copy(off, sizeof(v), (char*)&v);
    off += sizeof(v);
    s.insert(v);
  }
  assert(s.size() == (unsigned)n);
}

// deque<T>
template<class T>
inline void _encode(const std::deque<T>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::deque<T>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T v = *it;
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class T>
inline void _decode(std::deque<T>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T v;
    bl.copy(off, sizeof(v), (char*)&v);
    off += sizeof(v);
    s.push_back(v);
  }
  assert(s.size() == (unsigned)n);
}

// vector<T>
template<class T>
inline void _encode(std::vector<T>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::vector<T>::iterator it = s.begin();
       it != s.end();
       it++) {
    T v = *it;
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class T>
inline void _decode(std::vector<T>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  s = std::vector<T>(n);
  for (int i=0; i<n; i++) {
    T v;
    bl.copy(off, sizeof(v), (char*)&v);
    off += sizeof(v);
    s[i] = v;
  }
  assert(s.size() == (unsigned)n);
}

// list<string>
inline void _encode(const std::list<std::string>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (std::list<std::string>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    ::_encode(*it, bl);
    n--;
  }
  assert(n==0);
}
inline void _decode(std::list<std::string>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    std::string st;
    ::_decode(st, bl, off);
    s.push_back(st);
  }
  assert(s.size() == (unsigned)n);
}

// list<T>
template<class T>
inline void _encode(const std::list<T>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::list<T>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T v = *it;
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class T>
inline void _decode(std::list<T>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T v;
    bl.copy(off, sizeof(v), (char*)&v);
    off += sizeof(v);
    s.push_back(v);
  }
  assert(s.size() == (unsigned)n);
}


// map<string,bufferptr>
inline void _encode(std::map<std::string, bufferptr>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (std::map<std::string, bufferptr>::iterator it = s.begin();
       it != s.end();
       it++) {
    _encode(it->first, bl);
    _encode(it->second, bl);
    n--;
  }
  assert(n==0);
}
inline void _decode(std::map<std::string,bufferptr>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    std::string k;
    _decode(k, bl, off);
    _decode(s[k], bl, off);
  }
  assert(s.size() == (unsigned)n);
}


// map<T,bufferlist>
template<class T>
inline void _encode(const std::map<T, bufferlist>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  //std::cout << "n = " << n << std::endl;
  for (typename std::map<T, bufferlist>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T k = it->first;
    bl.append((char*)&k, sizeof(k));
    _encode(it->second, bl);
    n--;
    //std::cout << "--n = " << n << " after k " << k << std::endl;
  }
  assert(n==0);
}
template<class T>
inline void _decode(std::map<T,bufferlist>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T k;
    bl.copy(off, sizeof(k), (char*)&k);
    off += sizeof(k);
    _decode(s[k], bl, off);
  }
  assert(s.size() == (unsigned)n);
}

// map<T,string>
template<class T>
inline void _encode(const std::map<T, std::string>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::map<T, std::string>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T k = it->first;
    bl.append((char*)&k, sizeof(k));
    _encode(it->second, bl);
    n--;
  }
  assert(n==0);
}
template<class T>
inline void _decode(std::map<T,std::string>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T k;
    bl.copy(off, sizeof(k), (char*)&k);
    off += sizeof(k);
    _decode(s[k], bl, off);
  }
  assert(s.size() == (unsigned)n);
}

// map<string,U>
template<class U>
inline void _encode(const std::map<std::string, U>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::map<std::string, U>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    ::_encode(it->first, bl);
    U v = it->second;
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class U>
inline void _decode(std::map<std::string,U>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    std::string k;
    U v;
    ::_decode(k, bl, off);
    bl.copy(off, sizeof(v), (char*)&v);
    off += sizeof(v);
    s[k] = v;
  }
  assert(s.size() == (unsigned)n);
}

// map<T,set<U>>
template<class T, class U>
inline void _encode(const std::map<T, std::set<U> >& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::map<T, std::set<U> >::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T k = it->first;
    bl.append((char*)&k, sizeof(k));
    ::_encode(it->second, bl);
    n--;
  }
  assert(n==0);
}
template<class T, class U>
inline void _decode(std::map<T, std::set<U> >& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T k;
    bl.copy(off, sizeof(k), (char*)&k);
    off += sizeof(k);
    ::_decode(s[k], bl, off);
  }
  assert(s.size() == (unsigned)n);
}

// map<T,deque<U>>
template<class T, class U>
inline void _encode(const std::map<T, std::deque<U> >& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::map<T, std::deque<U> >::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T k = it->first;
    bl.append((char*)&k, sizeof(k));
    ::_encode(it->second, bl);
    n--;
  }
  assert(n==0);
}
template<class T, class U>
inline void _decode(std::map<T, std::deque<U> >& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T k;
    bl.copy(off, sizeof(k), (char*)&k);
    off += sizeof(k);
    ::_decode(s[k], bl, off);
  }
  assert(s.size() == (unsigned)n);
}


// map<T,U>
template<class T, class U>
inline void _encode(const std::map<T, U>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename std::map<T, U>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T k = it->first;
    U v = it->second;
    bl.append((char*)&k, sizeof(k));
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class T, class U>
inline void _decode(std::map<T,U>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T k;
    U v;
    bl.copy(off, sizeof(k), (char*)&k);
    off += sizeof(k);
    bl.copy(off, sizeof(v), (char*)&v);
    off += sizeof(v);
    s[k] = v;
  }
  assert(s.size() == (unsigned)n);
}




#endif
