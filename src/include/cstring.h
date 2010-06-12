#ifndef CEPH_CSTRING_H
#define CEPH_CSTRING_H

/*
 * cstring - a simple string class
 *
 * the key difference between this and std::string is that the string
 * is stored with a null terminator, providing an efficient c_str()
 * method.
 */

#include "buffer.h"
#include "encoding.h"

class cstring {
 private:
  int _len;
  char *_data;
  
 public:
  cstring() : _len(0), _data(0) {}
  cstring(int l, const char *d=0) : _len(l) {
    _data = new char[_len + 1];
    if (d)
      memcpy(_data, d, l);
    _data[l] = 0;
  }
  cstring(const char *s) { 
    if (s) {
      _len = strlen(s);
      _data = new char[_len + 1];
      memcpy(_data, s, _len);
      _data[_len] = 0;
    } else {
      _len = 0;
      _data = 0;
    }
  }
  cstring(const string &s) {
    _len = s.length();
    _data = new char[_len + 1];
    memcpy(_data, s.data(), _len);
    _data[_len] = 0;
  }
  cstring(const cstring &s) {
    _len = s.length();
    _data = new char[_len + 1];
    memcpy(_data, s.data(), _len);
    _data[_len] = 0;
  }
  ~cstring() {
    if (_data) delete[] _data;
  }

  // accessors
  int length() const { return _len; }
  bool empty() const { return _len == 0; }
  char *c_str() const { return _data; }
  char *data() const { return _data; }

  //const char *operator() const { return _data; }

  // modifiers
  const cstring& operator=(const char *s) {
    if (_data) delete[] _data;
    _len = strlen(s);
    _data = new char[_len + 1];
    memcpy(_data, s, _len);
    _data[_len] = 0;
    return *this;
  }
  const cstring& operator=(const string &s) {
    if (_data) delete[] _data;
    _len = s.length();
    _data = new char[_len + 1];
    memcpy(_data, s.data(), _len);
    _data[_len] = 0;
    return *this;
  }
  const cstring& operator=(const cstring &ns) {
    if (_data) delete[] _data;
    _len = ns.length();
    _data = new char[_len + 1];
    memcpy(_data, ns.data(), _len);
    _data[_len] = 0;
    return *this;
  }
  char &operator[](int n) {
    assert(n <= _len);
    return _data[n];
  }
  void swap(cstring &other) {
    int tlen = _len;
    char *tdata = _data;
    _len = other._len;
    _data = other._data;
    other._len = tlen;
    other._data = tdata;
  }
  void clear() {
    _len = 0;
    delete _data;
    _data = 0;
  }

  // encoding
  void encode(bufferlist &bl) const {
    __u32 l = _len;
    ::encode(l, bl);
    bl.append(_data, _len);
  }
  void decode(bufferlist::iterator &bl) {
    __u32 l;
    ::decode(l, bl);
    decode_nohead(l, bl);
  }
  void decode_nohead(int l, bufferlist::iterator& bl) {
    if (_data) delete[] _data;
    _len = l;
    _data = new char[_len + 1];
    bl.copy(_len, _data);
    _data[_len] = 0;
  }
};
WRITE_CLASS_ENCODER(cstring)

inline void encode_nohead(const cstring& s, bufferlist& bl)
{
  bl.append(s.data(), s.length());
}
inline void decode_nohead(int len, cstring& s, bufferlist::iterator& p)
{
  s.decode_nohead(len, p);
}


#endif
