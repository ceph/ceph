#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_json.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

void dump_array(JSONObj *obj)
{

  JSONObjIter iter = obj->find_first();

  for (; !iter.end(); ++iter) { 
    JSONObj *o = *iter;
    cout << "data=" << o->get_data() << endl;
  }

}
                                  
int main(int argc, char **argv) {
  RGWJSONParser parser;

  char buf[1024];

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

    if (done)
      break;
  }

  JSONObjIter iter = parser.find_first();

  for (; !iter.end(); ++iter) { 
    JSONObj *obj = *iter;
    cout << "is_object=" << obj->is_object() << endl;
    cout << "is_array=" << obj->is_array() << endl;
    cout << "name=" << obj->get_name() << endl;
    cout << "data=" << obj->get_data() << endl;
  }

  iter = parser.find_first("conditions");
  if (!iter.end()) {
    JSONObj *obj = *iter;

    JSONObjIter iter2 = obj->find_first();
    for (; !iter2.end(); ++iter2) {
      JSONObj *child = *iter2;
      cout << "is_object=" << child->is_object() << endl;
      cout << "is_array=" << child->is_array() << endl;
      if (child->is_array()) {
        dump_array(child);
      }
      cout << "name=" << child->get_name() << endl;
      cout << "data=" << child->get_data() << endl;
    }
  }


  exit(0);
}

