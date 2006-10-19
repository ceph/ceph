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


#ifndef __BUFFERLIST_H
#define __BUFFERLIST_H

#include "buffer.h"

#include <list>
#include <map>
#include <set>
#include <vector>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;


// debug crap
#include "config.h"
#define bdbout(x) if (x <= g_conf.debug_buffer) cout



class bufferlist {
 private:
  /* local state limited to _buffers, and _len.
   * we maintain _len ourselves, so we must be careful when fiddling with buffers!
   */
  list<bufferptr> _buffers;
  unsigned _len;

 public:
  // cons/des
  bufferlist() : _len(0) {
    bdbout(1) << "bufferlist.cons " << this << endl;
  }
  bufferlist(const bufferlist& bl) : _len(0) {
    //assert(0); // o(n) and stupid
    bdbout(1) << "bufferlist.cons " << this << endl; 
    _buffers = bl._buffers;
    _len = bl._len;
  }
  ~bufferlist() {
    bdbout(1) << "bufferlist.des " << this << endl;
  }
  
  bufferlist& operator=(bufferlist& bl) {
    //assert(0);  // actually, this should be fine, just slow (O(n)) and stupid.
    bdbout(1) << "bufferlist.= " << this << endl; 
    _buffers = bl._buffers;
    _len = bl._len;
    return *this;
  }
  

  // accessors
  list<bufferptr>& buffers() { 
    return _buffers; 
  }
  //list<buffer*>::iterator begin() { return _buffers.begin(); }
  //list<buffer*>::iterator end() { return _buffers.end(); }

  unsigned length() const {
#if 0
    { // DEBUG: verify _len
      int len = 0;
      for (list<bufferptr>::iterator it = _buffers.begin();
           it != _buffers.end();
           it++) {
        len += (*it).length();
      }
      assert(len == _len);
    }
#endif
    return _len;
  }

  void _rope(crope& r) {
    for (list<bufferptr>::iterator it = _buffers.begin();
         it != _buffers.end();
         it++)
      r.append((*it).c_str(), (*it).length());
  }
  
  // modifiers
  void clear() {
    _buffers.clear();
    _len = 0;
  }
  void push_front(bufferptr& bp) {
    _buffers.push_front(bp);
    _len += bp.length();
  }
  void push_front(buffer *b) {
    bufferptr bp(b);
    _buffers.push_front(bp);
    _len += bp.length();
  }
  void push_back(bufferptr& bp) {
    _buffers.push_back(bp);
    _len += bp.length();
  }
  void push_back(buffer *b) {
    bufferptr bp(b);

    _buffers.push_back(bp);
    _len += bp.length();

  }
  void zero() {
      for (list<bufferptr>::iterator it = _buffers.begin();
         it != _buffers.end();
         it++)
        it->zero();
    }

  // sort-of-like-assignment-op
  void claim(bufferlist& bl) {
    // free my buffers
    clear();
    claim_append(bl);
  }
  void claim_append(bufferlist& bl) {
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
    list<bufferptr>::iterator curbuf = _buffers.begin();

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
    list<bufferptr>::iterator curbuf = _buffers.begin();

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
  void copy_in(unsigned off, unsigned len, bufferlist& bl) {
    unsigned left = len;
    for (list<bufferptr>::iterator i = bl._buffers.begin();
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
        //cout << "copying up to " << len << " into tail " << avail << " bytes of tail buf" << endl;
        if (avail > len) 
          avail = len;
        unsigned blen = _buffers.back().length();
        memcpy(_buffers.back().c_str() + blen, data, avail);
        blen += avail;
        _buffers.back().set_length(blen);
        _len += avail;
        data += avail;
        len -= avail;
      }
      alen = _buffers.back().length();
    }
    if (len == 0) return;

    // just add another buffer.
    // alloc a bit extra, in case we do a bunch of appends.   FIXME be smarter!
    if (alen < 1024) alen = 1024;
    push_back(new buffer(data, len, BUFFER_MODE_DEFAULT, len+alen));  
  }
  void append(bufferptr& bp) {
    push_back(bp);
  }
  void append(bufferptr& bp, unsigned len, unsigned off) {
    bufferptr tempbp(bp, len, off);
    push_back(tempbp);
  }
  void append(const bufferlist& bl) {
    bufferlist temp = bl;  // copy list
    claim_append(temp);    // and append
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
      // make one new contiguous buffer.
      bufferptr newbuf = new buffer(length());
      unsigned off = 0;

      for (list<bufferptr>::iterator it = _buffers.begin();
           it != _buffers.end();
           it++) {
        //assert((*(*it)).has_free_func() == false);     // not allowed if there's a funky free_func.. -sage   ...for debugging at least!
        memcpy(newbuf.c_str() + off,
               (*it).c_str(), (*it).length());
        off += (*it).length();
      }
      assert(off == newbuf.length());
      
      _buffers.clear();
      _buffers.push_back( newbuf );

      // now it'll work.
      return c_str();
    }
  }


  void substr_of(bufferlist& other, unsigned off, unsigned len) {
    assert(off + len <= other.length());
    clear();

    // skip off
    list<bufferptr>::iterator curbuf = other._buffers.begin();
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
        _buffers.push_back( bufferptr( *curbuf, len, off ) );
        _len += len;
        break;
      }

      // through end
      //cout << "copying end (all?) of " << *curbuf << endl;
      unsigned howmuch = (*curbuf).length() - off;
      _buffers.push_back( bufferptr( *curbuf, howmuch, off ) );
      _len += howmuch;
      len -= howmuch;
      off = 0;
      curbuf++;
    }
  }

  // funky modifer
  void splice(unsigned off, unsigned len, bufferlist *claim_by=0 /*, bufferlist& replace_with */) {    // fixme?
    assert(off < length()); 
    assert(len > 0);
    //cout << "splice off " << off << " len " << len << " ... mylen = " << length() << endl;

    // skip off
    list<bufferptr>::iterator curbuf = _buffers.begin();
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
      _buffers.insert( curbuf, bufferptr( *curbuf, off, 0 ) );
      _len += off;
    }

    while (len > 0) {
      // partial?
      if (off + len < (*curbuf).length()) {
        //cout << "keeping end of " << *curbuf << ", losing first " << off+len << endl;
        if (claim_by) 
          claim_by->append( *curbuf, len, off );
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
        claim_by->append( *curbuf, howmuch, off );
      _len -= (*curbuf).length();
      _buffers.erase( curbuf++ );
      len -= howmuch;
      off = 0;
    }

    // splice in *replace (implement me later?)
  }

  friend ostream& operator<<(ostream& out, bufferlist& bl);

};

inline ostream& operator<<(ostream& out, bufferlist& bl) {
  out << "bufferlist(len=" << bl.length() << endl;
  for (list<bufferptr>::iterator it = bl._buffers.begin();
       it != bl._buffers.end();
       it++) 
    out << "\t" << *it << endl;
  out << ")" << endl;
  return out;
}



// encoder/decode helpers

// string
inline void _encode(const string& s, bufferlist& bl) 
{
  bl.append(s.c_str(), s.length()+1);
}
inline void _decode(string& s, bufferlist& bl, int& off)
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
    bp = new buffer(s.c_str(), s.length());
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


// set<T>
template<class T>
inline void _encode(set<T>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename set<T>::iterator it = s.begin();
       it != s.end();
       it++) {
    T v = *it;
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class T>
inline void _decode(set<T>& s, bufferlist& bl, int& off) 
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

// vector<T>
template<class T>
inline void _encode(vector<T>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename vector<T>::iterator it = s.begin();
       it != s.end();
       it++) {
    T v = *it;
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class T>
inline void _decode(vector<T>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  s = vector<T>(n);
  for (int i=0; i<n; i++) {
    T v;
    bl.copy(off, sizeof(v), (char*)&v);
    off += sizeof(v);
    s[i] = v;
  }
  assert(s.size() == (unsigned)n);
}

// list<T>
template<class T>
inline void _encode(const list<T>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename list<T>::const_iterator it = s.begin();
       it != s.end();
       it++) {
    T v = *it;
    bl.append((char*)&v, sizeof(v));
    n--;
  }
  assert(n==0);
}
template<class T>
inline void _decode(list<T>& s, bufferlist& bl, int& off) 
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
inline void _encode(map<string, bufferptr>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (map<string, bufferptr>::iterator it = s.begin();
       it != s.end();
       it++) {
    _encode(it->first, bl);
    _encode(it->second, bl);
    n--;
  }
  assert(n==0);
}
inline void _decode(map<string,bufferptr>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    string k;
    _decode(k, bl, off);
    _decode(s[k], bl, off);
  }
  assert(s.size() == (unsigned)n);
}


// map<T,bufferlist>
template<class T>
inline void _encode(const map<T, bufferlist>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename map<T, bufferlist>::const_iterator it = s.begin();
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
inline void _decode(map<T,bufferlist>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
    T k;
    bl.copy(off, sizeof(k), (char*)&k);
    off += sizeof(k);
    bufferlist b;
    _decode(b, bl, off);
    s[k] = b;
  }
  assert(s.size() == (unsigned)n);
}

// map<T,U>
template<class T, class U>
inline void _encode(const map<T, U>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (typename map<T, U>::const_iterator it = s.begin();
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
inline void _decode(map<T,U>& s, bufferlist& bl, int& off) 
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
