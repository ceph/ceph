// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "common/Formatter.h"
#include "common/ceph_json.h"

#include "rgw_common.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

void dump_array(JSONObj *obj)
{

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) { 
    JSONObj *o = *iter;
    cout << "data=" << o->get_data() << std::endl;
  }

}
                                  
struct Key {
  string user;
  string access_key;
  string secret_key;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("user", user, obj);
    JSONDecoder::decode_json("access_key", access_key, obj);
    JSONDecoder::decode_json("secret_key", secret_key, obj);
  }
};

struct UserInfo {
  string uid;
  string display_name;
  int max_buckets;
  list<Key> keys;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("user_id", uid, obj);
    JSONDecoder::decode_json("display_name", display_name, obj);
    JSONDecoder::decode_json("max_buckets", max_buckets, obj);
    JSONDecoder::decode_json("keys", keys, obj);
  }
};


int main(int argc, char **argv) {
  JSONParser parser;

  char buf[1024];
  bufferlist bl;

  for (;;) {
    int done;
    int len;

    len = fread(buf, 1, sizeof(buf), stdin);
    if (ferror(stdin)) {
      cerr << "read error" << std::endl;
      exit(-1);
    }
    done = feof(stdin);

    bool ret = parser.parse(buf, len);
    if (!ret)
      cerr << "parse error" << std::endl;

    if (done) {
      bl.append(buf, len);
      break;
    }
  }

  JSONObjIter iter = parser.find_first();

  for (; !iter.end(); ++iter) { 
    JSONObj *obj = *iter;
    cout << "is_object=" << obj->is_object() << std::endl;
    cout << "is_array=" << obj->is_array() << std::endl;
    cout << "name=" << obj->get_name() << std::endl;
    cout << "data=" << obj->get_data() << std::endl;
  }

  iter = parser.find_first("conditions");
  if (!iter.end()) {
    JSONObj *obj = *iter;

    JSONObjIter iter2 = obj->find_first();
    for (; !iter2.end(); ++iter2) {
      JSONObj *child = *iter2;
      cout << "is_object=" << child->is_object() << std::endl;
      cout << "is_array=" << child->is_array() << std::endl;
      if (child->is_array()) {
        dump_array(child);
      }
      cout << "name=" << child->get_name() <<std::endl;
      cout << "data=" << child->get_data() <<std::endl;
    }
  }

  RGWUserInfo ui;

  try {
    ui.decode_json(&parser);
  } catch (JSONDecoder::err& e) {
    cout << "failed to decode JSON input: " << e.message << std::endl;
    exit(1);
  }

  JSONFormatter formatter(true);

  formatter.open_object_section("user_info");
  ui.dump(&formatter);
  formatter.close_section();

  formatter.flush(std::cout);

  std::cout << std::endl;
}

