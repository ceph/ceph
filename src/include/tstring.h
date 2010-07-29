#ifndef CEPH_TSTRING_6183F100D51A4EDABFB908CB07D30601
#define CEPH_TSTRING_6183F100D51A4EDABFB908CB07D30601

#include "common/Mutex.h"

class stringtable {
private:
  Mutex lock;
  struct stringrec {
    char *data;
    int len;
    int ref;
    stringrec() : data(0), len(0), ref(0) {}
    ~stringrec() {
      delete[] data;
    }
  };
  hash_map<const char *, stringrec, hash<const char*>, eqstr> stab;

public:
  stringtable() : lock("stringtable::lock") {}

  // @s must be null terminated
  const char *get_cstr(const char *s, int len) {
    Mutex::Locker l(lock);
    stringrec &r = stab[s];
    if (r.ref == 0) {
      r.len = len;
      r.data = new char[len+1];
      memcpy(r.data, s, len);
      r.data[len] = 0;
    }
    r.ref++;
    //cout << "get_cstr " << &r << " '" << r.data << "' ref " << r.ref << std::endl;
    return r.data;
  }

  const char *get_heap_cstr(char *s, int len) {
    Mutex::Locker l(lock);
    stringrec &r = stab[s];
    if (r.ref == 0) {
      r.len = len;
      r.data = s;
    } else
      delete[] s;
    r.ref++;
    //cout << "get_heap_cstr " << &r << " '" << r.data << "' ref " << r.ref << std::endl;
    return r.data;
  }

  // @data need not be null-terminated.
  const char *get_data(const char *data, int len) {
    Mutex::Locker l(lock);
    char *t = new char[len+1];
    memcpy(t, data, len);
    t[len] = 0;
    stringrec &r = stab[t];
    if (r.ref == 0) {
      r.len = len;
      r.data = t;
    } else {
      delete[] t;
    }
    r.ref++;
    //cout << "get_data " << &r << " '" << r.data << "' ref " << r.ref << std::endl;
    return r.data;
  }

  // @s must be null terminated (as with any existing tstring.data)
  const char *get_existing(const char *s) {
    Mutex::Locker l(lock);
    stringrec &r = stab[s];
    assert(r.ref > 0);
    r.ref++;
    //cout << "get_existing " << &r << " '" << r.data << "' ref " << r.ref << std::endl;
    return r.data;
  }

  void put(const char *s) {
    Mutex::Locker l(lock);
    stringrec &r = stab[s];
    r.ref--;
    //cout << "put " << &r << " '" << r.data << "' ref " << r.ref << std::endl;
    if (r.ref == 0) {
      if (s == r.data) {
	// make a copy of the string; otherwise hash_map.erase is unhappy
	char t[r.len+1];
	memcpy(t, s, r.len+1);
	stab.erase(t);
      } else
	stab.erase(s);
    }
  }
  
};

extern stringtable g_stab;


class tstring {
private:
  const char *_data;
  int _len;
  
public:
  tstring() : _data(0), _len(0) {}
  tstring(const char *s) {
    _len = strlen(s);
    _data = g_stab.get_cstr(s, _len);
  }
  tstring(const string &s) {
    _len = s.length();
    _data = g_stab.get_data(s.data(), _len);
  }
  tstring(const tstring &s) {
    _len = s.length();
    _data = g_stab.get_existing(s.c_str());
  }
  ~tstring() {
    if (_data)
      g_stab.put(_data);
  }

  // accessors
  int length() const { return _len; }
  bool empty() const { return _len == 0; }
  const char *c_str() const { return _data; }
  const char *data() const { return _data; }

  // modifiers
  const tstring& operator=(const char *s) {
    if (_data)
      g_stab.put(_data);
    _len = strlen(s);
    _data = g_stab.get_cstr(s, _len);
    return *this;
  }
  const tstring& operator=(const string &s) {
    if (_data)
      g_stab.put(_data);
    _len = s.length();
    _data = g_stab.get_data(s.data(), _len);
    return *this;
  }
  const tstring& operator=(const tstring &s) {
    if (_data)
      g_stab.put(_data);
    _len = s.length();
    _data = g_stab.get_existing(s.data());
    return *this;
  }
 
  void swap(tstring &other) {
    int tlen = _len;
    const char *tdata = _data;
    _len = other._len;
    _data = other._data;
    other._len = tlen;
    other._data = tdata;
  }
  void clear() {
    _len = 0;
    if (_data) {
      g_stab.put(_data);
      _data = 0;
    }
  }

  // encoding
  void encode(bufferlist &bl) const {
    __u32 l = _len;
    ::encode(l, bl);
    bl.append(_data, _len);
  }
  void decode(bufferlist::iterator &bl) {
    clear();
    __u32 l;
    ::decode(l, bl);
    _len = l;
    char *t = new char[_len+1];
    bl.copy(_len, t);
    t[_len] = 0;
    _data = g_stab.get_heap_cstr(t, _len);
  }
};
WRITE_CLASS_ENCODER(tstring)

#endif
