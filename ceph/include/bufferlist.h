#ifndef __BUFFERLIST_H
#define __BUFFERLIST_H

#include "buffer.h"

#include <list>
#include <set>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;

class bufferlist {
 private:
  list<bufferptr> _buffers;

 public:
  // cons/des
  ~bufferlist() {
	clear();
  }

  // sort-of-like-assignment-op
  void claim(bufferlist& bl) {
	// free my buffers
	clear();                    
	claim_append(bl);
  }
  void claim_append(bufferlist& bl) {
	// steal the other guy's buffers
	_buffers.splice( _buffers.end(), bl._buffers );
  }

  // accessors
  list<bufferptr>& buffers() { 
	return _buffers; 
  }
  //list<buffer*>::iterator begin() { return _buffers.begin(); }
  //list<buffer*>::iterator end() { return _buffers.end(); }

  int length() {
	int len = 0;
	for (list<bufferptr>::iterator it = _buffers.begin();
		 it != _buffers.end();
		 it++) {
	  len += (*it).length();
	}
	return len;
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
  }
  void push_front(bufferptr& bp) {
	_buffers.push_front(bp);
  }
  void push_front(buffer *b) {
	bufferptr bp(b);
	_buffers.push_front(bp);
  }
  void push_back(bufferptr& bp) {
	_buffers.push_back(bp);
  }
  void push_back(buffer *b) {
	bufferptr bp(b);
	_buffers.push_back(bp);
  }

  
  // crope lookalikes
  void copy(int off, int len, char *dest) {
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
		(*curbuf).copy(off, len, dest);        // yup, last bit!
		break;
	  }

	  // get as much as we can from this buffer.
	  int howmuch = (*curbuf).length() - off;
	  (*curbuf).copy(off, howmuch, dest);

	  dest += howmuch;
	  len -= howmuch;
	  off = 0;
	  curbuf++;
	  assert(curbuf != _buffers.end());
	}
  }
  void append(const char *data, int len) {
	if (len == 0) return;
	
	// just add another buffer
	push_back(new buffer(data, len));  
  }
  void append(bufferptr& bp) {
	push_back(bp);
  }
  void append(bufferptr& bp, int len, int off) {
	bufferptr tempbp(bp, len, off);
	push_back(tempbp);
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

	  for (list<bufferptr>::iterator it = _buffers.begin();
		   it != _buffers.end();
		   it++) {
		memcpy(newbuf.c_str() + newbuf.length(),
			   (*it).c_str(), (*it).length());
		newbuf.set_length( newbuf.length() + (*it).length() );
	  }
	  
	  _buffers.clear();
	  _buffers.push_back( newbuf );

	  // now it'll work.
	  return c_str();
	}
  }


  void substr_of(bufferlist& other, int off, int len) {
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
		break;
	  }

	  // hose the whole thing
	  //cout << "copying end (all?) of " << *curbuf << endl;
	  int howmuch = (*curbuf).length() - off;
	  _buffers.push_back( bufferptr( *curbuf, howmuch, off ) );
	  len -= howmuch;
	  off = 0;
	  curbuf++;
	}
  }

  // funky modifer
  void splice(int off, int len, bufferlist *claim_by=0 /*, bufferlist& replace_with */) {    // fixme?
	// skip off
	list<bufferptr>::iterator curbuf = _buffers.begin();
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

	if (off) {
	  // add a reference to the front bit
	  //  insert it before curbuf (which we'll hose)
	  //cout << "keeping front " << off << " of " << *curbuf << endl;
	  _buffers.insert( curbuf, bufferptr( *curbuf, off, 0 ) );
	}

	while (len > 0) {
	  // partial?
	  if (off + len < (*curbuf).length()) {
		//cout << "keeping end of " << *curbuf << ", losing first " << off+len << endl;
		if (claim_by) 
		  claim_by->append( *curbuf, len, off );
		(*curbuf).set_offset( off + len );    // ignore beginning big
		(*curbuf).set_length( (*curbuf).length() - len - off );
		//cout << " now " << *curbuf << endl;
		break;
	  }

	  // hose the whole thing
	  int howmuch = (*curbuf).length() - off;
	  //cout << "discarding " << howmuch << " of " << *curbuf << endl;
	  if (claim_by) 
		claim_by->append( *curbuf, howmuch, off );
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

// set<int>
inline void _encode(set<int>& s, bufferlist& bl)
{
  int n = s.size();
  bl.append((char*)&n, sizeof(n));
  for (set<int>::iterator it = s.begin();
	   it != s.end();
	   it++) {
	int v = *it;
	bl.append((char*)&v, sizeof(v));
	n--;
  }
  assert(n==0);
}
inline void _decode(set<int>& s, bufferlist& bl, int& off) 
{
  s.clear();
  int n;
  bl.copy(off, sizeof(n), (char*)&n);
  off += sizeof(n);
  for (int i=0; i<n; i++) {
	int v;
	bl.copy(off, sizeof(v), (char*)&v);
	off += sizeof(v);
	s.insert(v);
  }
  assert(s.size() == n);
}




#endif
