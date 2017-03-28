#include <list>
#include <string>
#include <iostream>

#include "common/ceph_json.h"
#include "rgw_es_query.h"

using namespace std;

int main(int argc, char *argv[])
{
  list<string> infix;

  string expr;

  if (argc > 1) {
    expr = argv[1];
  } else {
    expr = "age >= 30";
  }

  ESQueryCompiler es_query(expr, nullptr, "x-amz-meta-");

  map<string, ESEntityTypeMap::EntityType> custom_map = { {"str", ESEntityTypeMap::ES_ENTITY_STR},
                                                          {"int", ESEntityTypeMap::ES_ENTITY_INT},
                                                          {"date", ESEntityTypeMap::ES_ENTITY_DATE} };
  ESEntityTypeMap em(custom_map);
  es_query.set_custom_type_map(&em);
  
  bool valid = es_query.compile();
  if (!valid) {
    cout << "invalid query, failed generating request json" << std::endl;
    return EINVAL;
  }

  JSONFormatter f;
  encode_json("root", es_query, &f);

  f.flush(cout);

  return 0;
}

