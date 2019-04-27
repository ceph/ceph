#ifndef CEPH_JSON_H
#define CEPH_JSON_H

#include <include/types.h>


#include "json_spirit/json_spirit.h"

#include "Formatter.h"

using namespace json_spirit;


class JSONObj;

class JSONObjIter {
  typedef std::map<std::string, JSONObj *>::iterator map_iter_t;
  map_iter_t cur;
  map_iter_t last;

public:
  JSONObjIter();
  ~JSONObjIter();
  void set(const JSONObjIter::map_iter_t &_cur, const JSONObjIter::map_iter_t &_end);

  void operator++();
  JSONObj *operator*();

  bool end() const {
    return (cur == last);
  }
};

class JSONObj
{
  JSONObj *parent;
public:
  struct data_val {
    std::string str;
    bool quoted{false};

    void set(std::string_view s, bool q) {
      str = s;
      quoted = q;
    }
  };
protected:
  std::string name; // corresponds to obj_type in XMLObj
  json_spirit::Value data;
  struct data_val val;
  bool data_quoted{false};
  std::multimap<std::string, JSONObj *> children;
  std::map<std::string, data_val> attr_map;
  void handle_value(json_spirit::Value v);

public:

  JSONObj() : parent(NULL){}

  virtual ~JSONObj();

  void init(JSONObj *p, json_spirit::Value v, std::string n);

  std::string& get_name() { return name; }
  data_val& get_data_val() { return val; }
  const std::string& get_data() { return val.str; }
  bool get_data(const std::string& key, data_val *dest);
  JSONObj *get_parent();
  void add_child(std::string el, JSONObj *child);
  bool get_attr(std::string name, data_val& attr);
  JSONObjIter find(const std::string& name);
  JSONObjIter find_first();
  JSONObjIter find_first(const std::string& name);
  JSONObj *find_obj(const std::string& name);

  friend std::ostream& operator<<(std::ostream &out,
                                  const JSONObj &obj); // does not work, FIXME

  bool is_array();
  bool is_object();
  std::vector<std::string> get_array_elements();
};

inline std::ostream& operator<<(std::ostream &out, const JSONObj::data_val& dv) {
  const char *q = (dv.quoted ? "\"" : "");
   out << q << dv.str << q;
   return out;
}

class JSONParser : public JSONObj
{
  int buf_len;
  std::string json_buffer;
  bool success;
public:
  JSONParser();
  ~JSONParser() override;
  void handle_data(const char *s, int len);

  bool parse(const char *buf_, int len);
  bool parse(int len);
  bool parse();
  bool parse(const char *file_name);

  const char *get_json() { return json_buffer.c_str(); }
  void set_failure() { success = false; }
};

void encode_json(const char *name, const JSONObj::data_val& v, ceph::Formatter *f);

class JSONDecoder {
public:
  struct err {
    std::string message;

    err(const std::string& m) : message(m) {}
  };

  JSONParser parser;

  JSONDecoder(ceph::buffer::list& bl) {
    if (!parser.parse(bl.c_str(), bl.length())) {
      std::cout << "JSONDecoder::err()" << std::endl;
      throw JSONDecoder::err("failed to parse JSON input");
    }
  }

  template<class T>
  static bool decode_json(const char *name, T& val, JSONObj *obj, bool mandatory = false);

  template<class C>
  static bool decode_json(const char *name, C& container, void (*cb)(C&, JSONObj *obj), JSONObj *obj, bool mandatory = false);

  template<class T>
  static void decode_json(const char *name, T& val, const T& default_val, JSONObj *obj);

  template<class T>
  static bool decode_json(const char *name, boost::optional<T>& val, JSONObj *obj, bool mandatory = false);

};

template<class T>
void decode_json_obj(T& val, JSONObj *obj)
{
  val.decode_json(obj);
}

inline void decode_json_obj(std::string& val, JSONObj *obj)
{
  val = obj->get_data();
}

static inline void decode_json_obj(JSONObj::data_val& val, JSONObj *obj)
{
  val = obj->get_data_val();
}

void decode_json_obj(unsigned long long& val, JSONObj *obj);
void decode_json_obj(long long& val, JSONObj *obj);
void decode_json_obj(unsigned long& val, JSONObj *obj);
void decode_json_obj(long& val, JSONObj *obj);
void decode_json_obj(unsigned& val, JSONObj *obj);
void decode_json_obj(int& val, JSONObj *obj);
void decode_json_obj(bool& val, JSONObj *obj);
void decode_json_obj(ceph::buffer::list& val, JSONObj *obj);
class utime_t;
void decode_json_obj(utime_t& val, JSONObj *obj);

template<class T>
void decode_json_obj(std::list<T>& l, JSONObj *obj)
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
void decode_json_obj(std::deque<T>& l, JSONObj *obj)
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
void decode_json_obj(std::set<T>& l, JSONObj *obj)
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
void decode_json_obj(std::vector<T>& l, JSONObj *obj)
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

template<class K, class V, class C = std::less<K> >
void decode_json_obj(std::map<K, V, C>& m, JSONObj *obj)
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
void decode_json_obj(std::multimap<K, V>& m, JSONObj *obj)
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
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    val = T();
    return false;
  }

  try {
    decode_json_obj(val, *iter);
  } catch (err& e) {
    std::string s = std::string(name) + ": ";
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
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    return false;
  }

  try {
    decode_json_obj(container, cb, *iter);
  } catch (err& e) {
    std::string s = std::string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }

  return true;
}

template<class T>
void JSONDecoder::decode_json(const char *name, T& val, const T& default_val, JSONObj *obj)
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
    std::string s = std::string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }
}

template<class T>
bool JSONDecoder::decode_json(const char *name, boost::optional<T>& val, JSONObj *obj, bool mandatory)
{
  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    if (mandatory) {
      std::string s = "missing mandatory field " + std::string(name);
      throw err(s);
    }
    val = boost::none;
    return false;
  }

  try {
    val.reset(T());
    decode_json_obj(val.get(), *iter);
  } catch (err& e) {
    val.reset();
    std::string s = std::string(name) + ": ";
    s.append(e.message);
    throw err(s);
  }

  return true;
}

template<class T>
static void encode_json(const char *name, const T& val, ceph::Formatter *f)
{
  f->open_object_section(name);
  val.dump(f);
  f->close_section();
}

class utime_t;

void encode_json(const char *name, const std::string& val, ceph::Formatter *f);
void encode_json(const char *name, const char *val, ceph::Formatter *f);
void encode_json(const char *name, bool val, ceph::Formatter *f);
void encode_json(const char *name, int val, ceph::Formatter *f);
void encode_json(const char *name, unsigned val, ceph::Formatter *f);
void encode_json(const char *name, long val, ceph::Formatter *f);
void encode_json(const char *name, unsigned long val, ceph::Formatter *f);
void encode_json(const char *name, long long val, ceph::Formatter *f);
void encode_json(const char *name, const utime_t& val, ceph::Formatter *f);
void encode_json(const char *name, const ceph::buffer::list& bl, ceph::Formatter *f);
void encode_json(const char *name, long long unsigned val, ceph::Formatter *f);

template<class T>
static void encode_json(const char *name, const std::list<T>& l, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = l.cbegin(); iter != l.cend(); ++iter) {
    encode_json("obj", *iter, f);
  }
  f->close_section();
}
template<class T>
static void encode_json(const char *name, const std::deque<T>& l, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = l.cbegin(); iter != l.cend(); ++iter) {
    encode_json("obj", *iter, f);
  }
  f->close_section();
}
template<class T, class Compare = std::less<T> >
static void encode_json(const char *name, const std::set<T, Compare>& l, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = l.cbegin(); iter != l.cend(); ++iter) {
    encode_json("obj", *iter, f);
  }
  f->close_section();
}

template<class T>
static void encode_json(const char *name, const std::vector<T>& l, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = l.cbegin(); iter != l.cend(); ++iter) {
    encode_json("obj", *iter, f);
  }
  f->close_section();
}

template<class K, class V, class C = std::less<K> >
static void encode_json(const char *name, const std::map<K, V, C>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto i = m.cbegin(); i != m.cend(); ++i) {
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
  for (auto i = m.begin(); i != m.end(); ++i) {
    f->open_object_section("entry");
    encode_json("key", i->first, f);
    encode_json("val", i->second, f);
    f->close_section();
  }
  f->close_section();
}
template<class K, class V>
void encode_json_map(const char *name, const std::map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = m.cbegin(); iter != m.cend(); ++iter) {
    encode_json("obj", iter->second, f);
  }
  f->close_section();
}


template<class K, class V>
void encode_json_map(const char *name, const char *index_name,
                     const char *object_name, const char *value_name,
                     void (*cb)(const char *, const V&, ceph::Formatter *, void *), void *parent,
                     const std::map<K, V>& m, ceph::Formatter *f)
{
  f->open_array_section(name);
  for (auto iter = m.cbegin(); iter != m.cend(); ++iter) {
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
                     const std::map<K, V>& m, ceph::Formatter *f)
{
  encode_json_map<K, V>(name, index_name, object_name, value_name, NULL, NULL, m, f);
}

template<class K, class V>
void encode_json_map(const char *name, const char *index_name, const char *value_name,
                     const std::map<K, V>& m, ceph::Formatter *f)
{
  encode_json_map<K, V>(name, index_name, NULL, value_name, NULL, NULL, m, f);
}

class JSONFormattable : public ceph::JSONFormatter {
  JSONObj::data_val value;
  std::vector<JSONFormattable> arr;
  std::map<std::string, JSONFormattable> obj;

  std::vector<JSONFormattable *> enc_stack;
  JSONFormattable *cur_enc;

protected:
  bool handle_value(const char *name, std::string_view s, bool quoted) override;
  bool handle_open_section(const char *name, const char *ns, bool section_is_array) override;
  bool handle_close_section() override;

public:
  JSONFormattable(bool p = false) : JSONFormatter(p) {
    cur_enc = this;
    enc_stack.push_back(cur_enc);
  }

  enum Type {
    FMT_NONE,
    FMT_VALUE,
    FMT_ARRAY,
    FMT_OBJ,
  } type{FMT_NONE};

  void set_type(Type t) {
    type = t;
  }

  void decode_json(JSONObj *jo) {
    if (jo->is_array()) {
      set_type(JSONFormattable::FMT_ARRAY);
      decode_json_obj(arr, jo);
    } else if (jo->is_object()) {
      set_type(JSONFormattable::FMT_OBJ);
      auto iter = jo->find_first();
      for (;!iter.end(); ++iter) {
        JSONObj *field = *iter;
        decode_json_obj(obj[field->get_name()], field);
      }
    } else {
      set_type(JSONFormattable::FMT_VALUE);
      decode_json_obj(value, jo);
    }
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode((uint8_t)type, bl);
    encode(value.str, bl);
    encode(arr, bl);
    encode(obj, bl);
    encode(value.quoted, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    uint8_t t;
    decode(t, bl);
    type = (Type)t;
    decode(value.str, bl);
    decode(arr, bl);
    decode(obj, bl);
    if (struct_v >= 2) {
      decode(value.quoted, bl);
    } else {
      value.quoted = true;
    }
    DECODE_FINISH(bl);
  }

  const std::string& val() const {
    return value.str;
  }

  int val_int() const;
  long val_long() const;
  long long val_long_long() const;
  bool val_bool() const;

  const std::map<std::string, JSONFormattable> object() const {
    return obj;
  }

  const std::vector<JSONFormattable>& array() const {
    return arr;
  }

  const JSONFormattable& operator[](const std::string& name) const;
  const JSONFormattable& operator[](size_t index) const;

  JSONFormattable& operator[](const std::string& name);
  JSONFormattable& operator[](size_t index);

  operator std::string() const {
    return value.str;
  }

  explicit operator int() const {
    return val_int();
  }

  explicit operator long() const {
    return val_long();
  }

  explicit operator long long() const {
    return val_long_long();
  }

  explicit operator bool() const {
    return val_bool();
  }

  template<class T>
  T operator[](const std::string& name) const {
    return this->operator[](name)(T());
  }

  template<class T>
  T operator[](const std::string& name) {
    return this->operator[](name)(T());
  }

  std::string operator ()(const char *def_val) const {
    return def(std::string(def_val));
  }

  int operator()(int def_val) const {
    return def(def_val);
  }

  bool operator()(bool def_val) const {
    return def(def_val);
  }

  bool exists(const std::string& name) const;
  bool exists(size_t index) const;

  std::string def(const std::string& def_val) const;
  int def(int def_val) const;
  bool def(bool def_val) const;

  bool find(const std::string& name, std::string *val) const;

  std::string get(const std::string& name, const std::string& def_val) const;

  int get_int(const std::string& name, int def_val) const;
  bool get_bool(const std::string& name, bool def_val) const;

  int set(const std::string& name, const std::string& val);
  int erase(const std::string& name);

  void derive_from(const JSONFormattable& jf);

  void encode_json(const char *name, ceph::Formatter *f) const;

  bool is_array() const {
    return (type == FMT_ARRAY);
  }
};
WRITE_CLASS_ENCODER(JSONFormattable)

void encode_json(const char *name, const JSONFormattable& v, ceph::Formatter *f);

#endif
