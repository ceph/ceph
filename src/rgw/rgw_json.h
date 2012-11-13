#ifndef RGW_JSON_H
#define RGW_JSON_H

#include <iostream>
#include <include/types.h>

// for testing DELETE ME
#include <fstream>

#include "json_spirit/json_spirit.h"


using namespace std;
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

class RGWJSONParser : public JSONObj
{
  int buf_len;
  string json_buffer;
  bool success;
public:
  RGWJSONParser();
  virtual ~RGWJSONParser();
  void handle_data(const char *s, int len);

  bool parse(const char *buf_, int len);
  bool parse(int len);
  bool parse();
  bool parse(const char *file_name);

  const char *get_json() { return json_buffer.c_str(); }
  void set_failure() { success = false; }
};


#endif
