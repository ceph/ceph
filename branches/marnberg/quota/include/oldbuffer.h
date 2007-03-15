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

#include <cassert>
#include <string.h>

#include <iostream>
using namespace std;

// bit masks
#define BUFFER_MODE_NOCOPY 0
#define BUFFER_MODE_COPY   1    // copy on create, my buffer

#define BUFFER_MODE_NOFREE 0
#define BUFFER_MODE_FREE   2

#define BUFFER_MODE_CUSTOMFREE 4

#define BUFFER_MODE_DEFAULT 3//(BUFFER_MODE_COPY|BUFFER_MODE_FREE)


// debug crap
#include "config.h"
#define bdbout(x) if (x <= g_conf.debug_buffer) cout

#include "common/Mutex.h"

// HACK: in config.cc
/*
 * WARNING: bufferlock placements are tricky for efficiency.  note that only bufferptr and
 * buffer ever use buffer._ref, and only bufferptr should call ~buffer().
 *
 * So, I only need to protect:
 *  - buffer()'s modification of buffer_total_alloc
 *  - ~bufferptr() check of buffer._ref, and ~buffer's mod of buffer_total_alloc
 * 
 * I don't protect
 *  - buffer._get() .. increment is atomic on any sane architecture
 *  - buffer._put() .. only called by ~bufferptr.
 *  - ~buffer       .. only called by ~bufferptr   *** I HOPE!!  
 */
extern Mutex bufferlock;
extern long buffer_total_alloc;


typedef void (buffer_free_func_t)(void*,char*,unsigned);


/*
 * buffer  - the underlying buffer container.  with a reference count.
 * 
 * the buffer never shrinks.
 *
 * some invariants:
 *  _len never shrinks
 *  _len <= _alloc_len
 */
class buffer {
 protected:
  //wtf
  //static Mutex bufferlock;
  //static long buffer_total_alloc;// = 0;

 private:
  // raw buffer alloc
  char *_dataptr;
  bool _myptr;
  unsigned _len;
  unsigned _alloc_len;

  // ref counts
  unsigned _ref;
  int _get() { 
    bdbout(1) << "buffer.get " << *this << " get " << _ref+1 << endl;
    return ++_ref;
  }
  int _put() { 
    bdbout(1) << "buffer.put " << *this << " put " << _ref-1 << endl;
    assert(_ref > 0);
    return --_ref;
  }

  // custom (de!)allocator
  buffer_free_func_t *free_func;
  void *free_func_arg;
  
  friend class bufferptr;

 public:
  // constructors
  buffer() : _dataptr(0), _myptr(true), _len(0), _alloc_len(0), _ref(0), free_func(0), free_func_arg(0) { 
    bdbout(1) << "buffer.cons " << *this << endl;
  }
  buffer(unsigned a) : _dataptr(0), _myptr(true), _len(a), _alloc_len(a), _ref(0), free_func(0), free_func_arg(0) {
    bdbout(1) << "buffer.cons " << *this << endl;
    _dataptr = new char[a];
    bufferlock.Lock();
    buffer_total_alloc += _alloc_len;
    bufferlock.Unlock();
    bdbout(1) << "buffer.malloc " << (void*)_dataptr << endl;
  }
  ~buffer() {
    bdbout(1) << "buffer.des " << *this << " " << (void*)free_func << endl;
    if (free_func) {
      bdbout(1) << "buffer.custom_free_func " << free_func_arg << " " << (void*)_dataptr << endl;
      free_func( free_func_arg, _dataptr, _alloc_len );
    }
    else if (_dataptr && _myptr) {
      bdbout(1) << "buffer.free " << (void*)_dataptr << endl;
      delete[] _dataptr;
      buffer_total_alloc -= _alloc_len;
    }
  }
  
  buffer(const char *p, int l, int mode=BUFFER_MODE_DEFAULT, int alloc_len=0,
         buffer_free_func_t free_func=0, void* free_func_arg=0) : 
    _dataptr(0), 
    _myptr(false),
    _len(l), 
    _ref(0), 
    free_func(0), free_func_arg(0) {
    
    if (alloc_len) 
      _alloc_len = alloc_len;
    else
      _alloc_len = l;

    _myptr = mode & BUFFER_MODE_FREE ? true:false;
    bdbout(1) << "buffer.cons " << *this << " mode = " << mode << ", myptr=" << _myptr << endl;
    if (mode & BUFFER_MODE_COPY) {
      _dataptr = new char[_alloc_len];
      bdbout(1) << "buffer.malloc " << (void*)_dataptr << endl;
      bufferlock.Lock();
      buffer_total_alloc += _alloc_len;
      bufferlock.Unlock();
      memcpy(_dataptr, p, l);
      bdbout(1) << "buffer.copy " << *this << endl;
    } else {
      _dataptr = (char*)p;                              // ugly
      bdbout(1) << "buffer.claim " << *this << " myptr=" << _myptr << endl;
    }

    if (mode & BUFFER_MODE_CUSTOMFREE && free_func) {
      this->free_func = free_func;
      this->free_func_arg = free_func_arg;
    }
  }

  // operators
  buffer& operator=(buffer& other) {
    assert(0);  // not implemented, no reasonable assignment semantics.
    return *this;
  }

  char *c_str() {
    return _dataptr;
  }

  bool has_free_func() { return free_func != 0; }
  
  // accessor
  unsigned alloc_length() {
    return _alloc_len;
  }
  void set_length(unsigned l) {
    assert(l <= _alloc_len);
    _len = l;
  }
  unsigned length() { return _len; }
  unsigned unused_tail_length() { return _alloc_len - _len; }

  friend ostream& operator<<(ostream& out, buffer& b);
};

inline ostream& operator<<(ostream& out, buffer& b) {
  return out << "buffer(this=" << &b << " len=" << b._len << ", alloc=" << b._alloc_len << ", data=" << (void*)b._dataptr << " ref=" << b._ref << ")";
}


/*
 * smart pointer class for buffer
 *
 * we reference count the actual buffer.
 * we also let you refer to a subset of a buffer.
 * we implement the high-level buffer accessor methods.
 *
 * some invariants:
 *  _off        <  _buffer->_len
 *  _off + _len <= _buffer->_len
 */
class bufferptr {
 private:
  buffer *_buffer;
  unsigned _len, _off;

 public:
  // empty cons
  bufferptr() :
    _buffer(0),
    _len(0),
    _off(0) { }
  // main cons - the entire buffer
  bufferptr(buffer *b) :
    _buffer(b),
    _len(b->_len),
    _off(0) {
    assert(_buffer->_ref == 0);
    _buffer->_get();   // this is always the first one.
  }
  // subset cons - a subset of another bufferptr (subset)
  bufferptr(const bufferptr& bp, unsigned len, unsigned off) {
    bufferlock.Lock();
    _buffer = bp._buffer;
    _len = len;
    _off = bp._off + off;
    _buffer->_get();
    assert(_off < _buffer->_len);          // sanity checks
    assert(_off + _len <= _buffer->_len);
    bufferlock.Unlock();
  }

  // copy cons
  bufferptr(const bufferptr &other) {
    bufferlock.Lock();
    _buffer = other._buffer;
    _len = other._len;
    _off = other._off;
    if (_buffer) _buffer->_get();    
    bufferlock.Unlock();
  }

  // assignment operator
  bufferptr& operator=(const bufferptr& other) {
    //assert(0);
    // discard old
    discard_buffer();

    // point to other
    bufferlock.Lock();
    _buffer = other._buffer;
    _len = other._len;
    _off = other._off;
    if (_buffer) _buffer->_get();
    bufferlock.Unlock();
    return *this;
  }

  ~bufferptr() {
    discard_buffer();
  }

  void discard_buffer() {
    if (_buffer) {
      bufferlock.Lock();
      if (_buffer->_put() == 0) 
        delete _buffer;
      _buffer = 0;
      bufferlock.Unlock();
    }
  }


  // dereference to get the actual buffer
  buffer& operator*() { 
    return *_buffer;
  }


  bool at_buffer_head() const {
    return _off == 0;
  }
  bool at_buffer_tail() const {
    return _off + _len == _buffer->_len;
  }

  // accessors for my subset
  char *c_str() {
    return _buffer->c_str() + _off;
  }
  unsigned length() const {
    return _len;
  }
  unsigned offset() const {
    return _off;
  }
  unsigned unused_tail_length() {
    if (!at_buffer_tail()) return 0;
    return _buffer->unused_tail_length();
  }



  // modifiers
  void set_offset(unsigned off) {
    assert(off <= _buffer->_alloc_len);
    _off = off;
  }
  void set_length(unsigned len) {
    assert(len >= 0 && _off + len <= _buffer->_alloc_len);
    if (_buffer->_len < _off + len) 
      _buffer->_len = _off + len;    // set new buffer len (_IF_ i'm expanding it)
    _len = len;                      // my len too
  }
  void zero() {
      //bzero((void*)c_str(), _len);
    memset((void*)c_str(), 0, _len);
  }


  // crope lookalikes
  void append(const char *p, unsigned len) {
    assert(len + _len + _off <= _buffer->_alloc_len);  // FIXME later for auto-expansion?

    // copy
    memcpy(c_str() + _len, p, len);
    _buffer->_len += len;
    _len += len;
  }
  void copy_out(unsigned off, unsigned len, char *dest) {
    assert(off >= 0 && off <= _len);
    assert(len >= 0 && off + len <= _len);
    memcpy(dest, c_str() + off, len);
  }
  void copy_in(unsigned off, unsigned len, const char *src) {
    assert(off >= 0 && off <= _len);
    assert(len >= 0 && off + len <= _len);
    memcpy(c_str() + off, src, len);
  }

  friend ostream& operator<<(ostream& out, bufferptr& bp);
};


inline ostream& operator<<(ostream& out, bufferptr& bp) {
  return out << "bufferptr(len=" << bp._len << " off=" << bp._off 
             << " cstr=" << (void*)bp.c_str()
             << " buf=" << *bp._buffer 
             << ")";
}



#endif
