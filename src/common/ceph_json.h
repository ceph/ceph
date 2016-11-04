#ifndef CEPH_JSON_H
#define CEPH_JSON_H

#include <iosfwd>
#include <include/types.h>
#include <list>

#ifdef _ASSERT_H
#define NEED_ASSERT_H
#pragma push_macro("_ASSERT_H")
#endif

#include "json_spirit/json_spirit.h"
#undef _ASSERT_H

#ifdef NEED_ASSERT_H
#pragma pop_macro("_ASSERT_H")
#endif

#include "Formatter.h"


using namespace json_spirit;


class JSONObj;

class JSONObjIter {
  typedef map<string, JSONObj *>::iterator map_iter_t;
  map_iter_t cur;
  map_iter_t last;

public:
  JSONObjIter();
  ~JSONObjIter();
  void set(const JSONObjIter::map_iter_t &_cur, const JSONObjIter::map_iter_t &_end);

  void operator++();
  JSONObj *operator*();

  bool end() {
    return (cur == last);
  }
};

class JSONObj
{
  JSONObj *parent;
protected:
  string name; // corresponds to obj_type in XMLObj
  Value data;
  string data_string;
  multimap<string, JSONObj *> children;
  map<string, string> attr_map;
  void handle_value(Value v);

public:

  JSONObj() : parent(NULL){}

  virtual ~JSONObj();

  void init(JSONObj *p, Value v, string n);

  string& get_name() { return name; }
  string& get_data() { return data_string; }
  bool get_data(const string& key, string *dest);
  JSONObj *get_parent();
  void add_child(string el, JSONObj *child);
  bool get_attr(string name, string& attr);
  JSONObjIter find(const string& name);
  JSONObjIter find_first();
  JSONObjIter find_first(const string& name);
  JSONObj *find_obj(const string& name);

  friend ostream& operator<<(ostream &out,
			     const JSONObj &obj); // does not work, FIXME

  bool is_array();
  bool is_object();
  vector<string> get_array_elements();
};

class JSONParser : public JSONObj
{
  int buf_len;
  string json_buffer;
  bool success;
public:
  JSONParser();
  virtual ~JSONParser();
  void handle_data(const char *s, int len);

  bool parse(const char *buf_, int len);
  bool parse(int len);
  bool parse();
  bool parse(const char *file_name);

  const char *get_json() { return json_buffer.c_str(); }
  void set_failure() { success = false; }
};


class JSONDecoder {
public:
  struct err {
    string message;

    err(const string& m) : message(m) {}
  };

  JSONParser parser;

  JSONDecoder(bufferlist& bl) {
    if (!parser.parse(bl.c_str(), bl.length())) {
      cout << "JSONDecoder::err()" << std::endl;
      throw JSONDecoder::err("failed to parse JSON input");
    }
  }

  template<class T>
  static bool decode_json(const char *name, T& val, JSONObj *obj, bool mandatory = false);

  template<class C>
  static bool decode_json(const char *name, C& container, void (*cb)(C&, JSONObj *obj), JSONObj *obj, bool mandatory = false);

  template<class T>
  static void decode_json(const char *name, T& val, T& default_val, JSONObj *obj);
};

template<class T>
void decode_json_obj(T& val, JSONObj *obj)
{
  val.decode_json(obj);
}

static inline void decode_json_obj(string& val, JSONObj *obj)
{
  val = obj->get_data();
}

void decode_json_obj(unsigned long long& val, JSONObj *obj);
void decode_json_obj(long long& val, JSONObj *obj);
void decode_json_obj(unsigned long& val, JSONObj *obj);
void decode_json_obj(long& val, JSONObj *obj);
void decode_json_obj(unsigned& val, JSONObj *obj);
void decode_json_obj(int& val, JSONObj *obj);
void decode_json_obj(bool& val, JSONObj *obj);
void decode_json_obj(bufferlist& val, JSONObj *obj);
class utime_t;
void decode_json_obj(utime_t& val, JSONObj *obj);

template<class T>
void decode_json_obj(list<T>& l, JSONObj *obj)
{
  l.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    T val;
    JSONObj *o = *iter;
    decode_json_obj(val, o);
    l.push_back(val);
  }
}

template<class T>
void decode_json_obj(deque<T>& l, JSONObj *obj)
{
  l.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    T val;
    JSONObj *o = *iter;
    decode_json_obj(val, o);
    l.push_back(val);
  }
}

template<class T>
void decode_json_obj(set<T>& l, JSONObj *obj)
{
  l.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    T val;
    JSONObj *o = *iter;
    decode_json_obj(val, o);
    l.insert(val);
  }
}

template<class T>
void decode_json_obj(vector<T>& l, JSONObj *obj)
{
  l.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    T val;
    JSONObj *o = *iter;
    decode_json_obj(val, o);
    l.push_back(val);
  }
}

template<class K, class V>
void decode_json_obj(map<K, V>& m, JSONObj *obj)
{
  m.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    K key;
    V val;
    JSONObj *o = *iter;
    JSONDecoder::decode_json("key", key, o);
    JSONDecoder::decode_json("val", val, o);
    m[key] = val;
  }
}

template<class K, class V>
void decode_json_obj(multimap<K, V>& m, JSONObj *obj)
{
  m.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    K key;
    V val;
    JSONObj *o = *iter;
    JSONDecoder::decode_json("key", key, o);
    JSONDecoder::decode_json("val", val, o);
    m.insert(make_pair(key, val));
  }
}

template<class C>
void decode_json_obj(C& container, void (*cb)(C&, JSONObj *obj), JSONObj *obj)
{
  container.clear();

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    JSONObj *o = *iter;
    cb(container, o);
  }
}

template<class T>
bool JSONDecoder::decode_json(const char *name, T& val, JSONObj *obj, bool mandatory)
{
  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    if (mandatory) {
      string s = "missing mandatory field " + string(name);
      throw err(s);
    }
    val = T();
    return false;
  }

  try {
    decode_json_obj(val, *iter);
  } catch (err& e) {
    string s = string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }

  return true;
}

template<class C>
bool JSONDecoder::decode_json(const char *name, C& container, void (*cb)(C&, JSONObj *), JSONObj *obj, bool mandatory)
{
  container.clear();

  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    if (mandatory) {
      string s = "missing mandatory field " + string(name);
      throw err(s);
    }
    return false;
  }

  try {
    decode_json_obj(container, cb, *iter);
  } catch (err& e) {
    string s = string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }

  return true;
}

template<class T>
void JSONDecoder::decode_json(const char *name, T& val, T& default_val, JSONObj *obj)
{
  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    val = default_val;
    return;
  }

  try {
    decode_json_obj(val, *iter);
  } catch (err& e) {
    val = default_val;
    string s = string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }
}

template<class T>
static void encode_json(const char *name, const T& val, ceph::Formatter *f)
{
  f->open_object_section(name);
  val.dump(f);
  f->close_section();
}

class utime_t;

void encode_json(const char *name, const string& val, ceph::Formatter *f);
void encode_json(const char *name, const char *val, ceph::Formatter *f);
void encode_json(const char *name, bool val, ceph::Formatter *f);
void encode_json(const char *name, int val, ceph::Formatter *f);
void encode_json(const char *name, unsigned val, ceph::Formatter *f);
void encode_json(const char *name, long val, ceph::Formatter *f);
void encode_json(const char *name, unsigned long val, ceph::Formatter *f);
void encode_json(const char *name, long long val, ceph::Formatter *f);
void encode_json(const char *name, const utime_t& val, ceph::Formatter *f);
void encode_json(const char *name, const bufferlist& bl, ceph::Formatter *f);
void encode_json(const char *name, long long val, ceph::Formatter *f);
void encode_json(const char *name, long long unsigned val, ceph::Formatter *f);

template<class T>
static void encode_json(const char *name, const std::list<T>& l, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (typename std::list<T>::const_iterator iter = l.begin(); iter != l.end(); ++iter) {
    encode_json("obj", *iter, f);
  }
  f->close_section();
}
template<class T>
static void encode_json(const char *name, const std::deque<T>& l, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (typename std::deque<T>::const_iterator iter = l.begin(); iter != l.end(); ++iter) {
    encode_json("obj", *iter, f);
  }
  f->close_section();
}template<class T>
static void encode_json(const char *name, const std::set<T>& l, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (typename std::set<T>::const_iterator iter = l.begin(); iter != l.end(); ++iter) {
    encode_json("obj", *iter, f);
  }
  f->close_section();
}

template<class T>
static void encode_json(const char *name, const std::vector<T>& l, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (typename std::vector<T>::const_iterator iter = l.begin(); iter != l.end(); ++iter) {
    encode_json("obj", *iter, f);
  }
  f->close_section();
}

template<class K, class V>
static void encode_json(const char *name, const std::map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (typename std::map<K, V>::const_iterator i = m.begin(); i != m.end(); ++i) {
    f->open_object_section("entry");
    encode_json("key", i->first, f);
    encode_json("val", i->second, f);
    f->close_section();
  }
  f->close_section();
}

template<class K, class V>
static void encode_json(const char *name, const std::multimap<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (typename std::multimap<K, V>::const_iterator i = m.begin(); i != m.end(); ++i) {
    f->open_object_section("entry");
    encode_json("key", i->first, f);
    encode_json("val", i->second, f);
    f->close_section();
  }
  f->close_section();
}
template<class K, class V>
void encode_json_map(const char *name, const map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  typename map<K,V>::const_iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    encode_json("obj", iter->second, f);
  }
  f->close_section(); 
}


template<class K, class V>
void encode_json_map(const char *name, const char *index_name,
                     const char *object_name, const char *value_name,
                     void (*cb)(const char *, const V&, ceph::Formatter *, void *), void *parent,
                     const map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  typename map<K,V>::const_iterator iter;
  for (iter = m.begin(); iter != m.end(); ++iter) {
    if (index_name) {
      f->open_object_section("key_value");
      f->dump_string(index_name, iter->first);
    }

    if (object_name) {
      f->open_object_section(object_name);
    }

    if (cb) {
      cb(value_name, iter->second, f, parent);
    } else {
      encode_json(value_name, iter->second, f);
    }

    if (object_name) {
      f->close_section();
    }
    if (index_name) {
      f->close_section();
    }
  }
  f->close_section(); 
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name,
                     const char *object_name, const char *value_name,
                     const map<K, V>& m, ceph::Formatter *f)
{
  encode_json_map<K, V>(name, index_name, object_name, value_name, NULL, NULL, m, f);
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name, const char *value_name,
                     const map<K, V>& m, ceph::Formatter *f)
{
  encode_json_map<K, V>(name, index_name, NULL, value_name, NULL, NULL, m, f);
}

#endif
