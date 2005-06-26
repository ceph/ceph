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

#define BUFFER_MODE_DEFAULT 3//(BUFFER_MODE_COPY|BUFFER_MODE_FREE)

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
 private:
  char *_dataptr;
  bool _myptr;
  int _len;
  int _alloc_len;

  
  int _ref;
  int _get() { 
	//cout << "buffer.get " << *this << " get " << _ref+1 << endl;
	return ++_ref; 
  }
  int _put() { 
	//cout << "buffer.put " << *this << " put " << _ref-1 << endl;
	return --_ref; 
  }
  
  friend class bufferptr;

 public:
  // constructors
  buffer() : _dataptr(0), _len(0), _alloc_len(0), _ref(0), _myptr(true) { 
	//cout << "buffer.cons " << *this << endl;
  }
  buffer(int a) : _dataptr(0), _len(0), _alloc_len(a), _ref(0), _myptr(true) {
	//cout << "buffer.cons " << *this << endl;
	_dataptr = new char[a];
	//cout << "buffer.malloc " << (void*)_dataptr << endl;
  }
  ~buffer() {
	//cout << "buffer.des " << *this << endl;
	if (_dataptr && _myptr) {
	  //cout << "buffer.free " << (void*)_dataptr << endl;
	  delete[] _dataptr;
	}
  }
  
  buffer(const char *p, int l, int mode=BUFFER_MODE_DEFAULT) : 
	_dataptr(0), 
	 _len(l), 
	 _alloc_len(l), 
	 _ref(0),
	 _myptr(0) {
	_myptr = mode & BUFFER_MODE_FREE ? true:false;
	//cout << "buffer.cons " << *this << " mode = " << mode << ", myptr=" << _myptr << endl;
	if (mode & BUFFER_MODE_COPY) {
	  _dataptr = new char[l];
	  //cout << "buffer.malloc " << (void*)_dataptr << endl;
	  memcpy(_dataptr, p, l);
	  //cout << "buffer(copy) " << *this << endl;
	} else {
	  _dataptr = (char*)p;                              // ugly
	  //cout << "buffer(claim), myptr=" << _myptr << " " << *this << endl;
	}
  }

  // operators
  buffer& operator=(buffer& other) {
	assert(0);  // not implemented, no reasonable assignment semantics.
  }

  char *c_str() {
	return _dataptr;
  }

  // accessor
  int alloc_length() {
	return _alloc_len;
  }
  int set_length(int l) {
	assert(l <= _alloc_len);
	_len = l;
  }
  int length() { return _len; }

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
  int _len, _off;

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
	_buffer->_get();
  }
  // subset cons - a subset of another bufferptr (subset)
  bufferptr(const bufferptr& bp, int len, int off) : 
	_buffer(bp._buffer), 
	_len(len) {
	_off = bp._off + off;
	_buffer->_get();
	assert(_off < _buffer->_len);          // sanity checks
	assert(_off + _len <= _buffer->_len);
  }

  // copy cons
  bufferptr(const bufferptr &other) : 
	_buffer(other._buffer),
	_len(other._len),
	_off(other._off) {
	_buffer->_get();	
  }

  // assignment operator
  bufferptr& operator=(const bufferptr& other) {
	// discard old
	if (_buffer && _buffer->_put() == 0) 
	  delete _buffer; 

	// new
	_buffer = other._buffer;
	_len = other._len;
	_off = other._off;
	_buffer->_get();
  }

  ~bufferptr() {
	if (_buffer && _buffer->_put() == 0) 
	  delete _buffer;
  }


  // dereference to get the actual buffer
  buffer& operator*() { 
	return *_buffer;
  }


  // accessors for my subset
  char *c_str() {
	return _buffer->_dataptr + _off;
  }
  int length() {
	return _len;
  }
  int offset() {
	return _off;
  }


  // modifiers
  void set_offset(int off) {
	assert(off <= _buffer->_alloc_len);
	_off = off;
  }
  void set_length(int len) {
	assert(len >= 0 && _off + len <= _buffer->_alloc_len);
	if (_buffer->_len < _off + len) 
	  _buffer->_len = _off + len;    // set new buffer len (_IF_ i'm expanding it)
	_len = len;                      // my len too
  }


  // crope lookalikes
  void append(const char *p, int len) {
	assert(len + _len + _off <= _buffer->_alloc_len);  // FIXME later for auto-expansion?

	// copy
	memcpy(c_str() + _len, p, len);
	_buffer->_len += len;
	_len += len;
  }
  void copy_out(int off, int len, char *dest) {
	assert(off >= 0 && off <= _len);
	assert(len >= 0 && off + len <= _len);
	memcpy(dest, c_str() + off, len);
  }
  void copy_in(int off, int len, char *src) {
	assert(off >= 0 && off <= _len);
	assert(len >= 0 && off + len <= _len);
	memcpy(c_str() + off, src, len);
  }

  friend ostream& operator<<(ostream& out, bufferptr& bp);
};


inline ostream& operator<<(ostream& out, bufferptr& bp) {
  return out << "bufferptr(len=" << bp._len << ", off=" << bp._off 
			 << ", int=" << *(int*)(bp.c_str())
			 << ", " << *bp._buffer 
			 << ")";
}



#endif
