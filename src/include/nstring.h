#ifndef _CEPH_NSTRING
#define _CEPH_NSTRING

/*
 * nstring - a simple string class
 *
 * the key difference between this and std::string is that the string
 * is stored with a null terminator, providing an efficient c_str()
 * method.
 */
class nstring {
 private:
  int _len;
  char *_data;
  
 public:
  nstring() : _len(0), _data(0) {}
  nstring(int l, const char *d=0) : _len(l) {
    _data = new char[_len + 1];
    if (d) {
      memcpy(_data, d, l);
      _data[l] = 0;
    }
  }
  nstring(const char *s) { 
    _len = strlen(s);
    _data = new char[_len + 1];
    memcpy(_data, s, _len);
    _data[_len] = 0;
  }
  nstring(const string &s) {
    _len = s.length();
    _data = new char[_len + 1];
    memcpy(_data, s.data(), _len);
    _data[_len] = 0;
  }
  nstring(const nstring &s) {
    _len = s.length();
    _data = new char[_len + 1];
    memcpy(_data, s.data(), _len);
    _data[_len] = 0;
  }
  ~nstring() {
    if (_data) delete[] _data;
  }

  // accessors
  int length() const { return _len; }
  bool empty() const { return _len == 0; }
  const char *c_str() const { return _data; }
  const char *data() const { return _data; }

  //const char *operator() const { return _data; }

  // modifiers
  const nstring& operator=(const char *s) {
    if (_data) delete[] _data;
    _len = strlen(s);
    _data = new char[_len + 1];
    memcpy(_data, s, _len);
    _data[_len] = 0;
    return *this;
  }
  const nstring& operator=(const string &s) {
    if (_data) delete[] _data;
    _len = s.length();
    _data = new char[_len + 1];
    memcpy(_data, s.data(), _len);
    _data[_len] = 0;
    return *this;
  }
  const nstring& operator=(const nstring &ns) {
    if (_data) delete[] _data;
    _len = ns.length();
    _data = new char[_len + 1];
    memcpy(_data, ns.data(), _len);
    _data[_len] = 0;
    return *this;
  }
  void swap(nstring &other) {
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
    if (_data) delete[] _data;
    __u32 l;
    ::decode(l, bl);
    _len = l;
    _data = new char[_len + 1];
    bl.copy(_len, _data);
    _data[_len] = 0;
  }
};
WRITE_CLASS_ENCODER(nstring)

static inline bool operator==(const nstring &l, const nstring &r) {
  return l.length() == r.length() && memcmp(l.data(), r.data(), l.length()) == 0;
}
static inline bool operator!=(const nstring &l, const nstring &r) {
  return l.length() != r.length() || memcmp(l.data(), r.data(), l.length()) != 0;
}
static inline bool operator<(const nstring &l, const nstring &r) {
  return strcmp(l.c_str(), r.c_str()) < 0;
}
static inline bool operator<=(const nstring &l, const nstring &r) {
  return strcmp(l.c_str(), r.c_str()) <= 0;
}
static inline bool operator>(const nstring &l, const nstring &r) {
  return strcmp(l.c_str(), r.c_str()) > 0;
}
static inline bool operator>=(const nstring &l, const nstring &r) {
  return strcmp(l.c_str(), r.c_str()) >= 0;
}

static inline ostream& operator<<(ostream &out, const nstring &s) {
  return out << s.c_str();
}

namespace __gnu_cxx {
  template<> struct hash< nstring >
  {
    size_t operator()( const nstring& x ) const
    {
      static hash<const char*> H;
      return H(x.c_str());
    }
  };
}

#endif
