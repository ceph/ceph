#ifndef CEPH_JSON_H
#define CEPH_JSON_H

#include <iostream>
#include <include/types.h>

#include "json_spirit/json_spirit.h"


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

  JSONObj() : parent(NULL){};

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

  friend ostream& operator<<(ostream& out, JSONObj& obj); // does not work, FIXME

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
  static bool decode_json(const string& name, T& val, JSONObj *obj, bool mandatory = false);

  template<class T>
  static void decode_json(const string& name, T& val, T& default_val, JSONObj *obj);
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

void decode_json_obj(unsigned long& val, JSONObj *obj);
void decode_json_obj(long& val, JSONObj *obj);
void decode_json_obj(unsigned& val, JSONObj *obj);
void decode_json_obj(int& val, JSONObj *obj);
void decode_json_obj(bool& val, JSONObj *obj);

template<class T>
void decode_json_obj(list<T>& l, JSONObj *obj)
{
  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) {
    T val;
    JSONObj *o = *iter;
    decode_json_obj(val, o);
    l.push_back(val);
  }
}

template<class T>
bool JSONDecoder::decode_json(const string& name, T& val, JSONObj *obj, bool mandatory)
{
  JSONObjIter iter = obj->find_first(name);
  if (iter.end()) {
    if (mandatory) {
      string s = "missing mandatory field " + name;
      throw err(s);
    }
    return false;
  }

  try {
    decode_json_obj(val, *iter);
  } catch (err& e) {
    string s = name + ": ";
    s.append(e.message);
    throw err(s);
  }

  return true;
}

template<class T>
void JSONDecoder::decode_json(const string& name, T& val, T& default_val, JSONObj *obj)
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
    string s = name + ": ";
    s.append(e.message);
    throw err(s);
  }
}

#endif
