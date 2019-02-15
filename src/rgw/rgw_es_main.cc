// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <list>
#include <string>
#include <iostream>

#include "global/global_init.h"
#include "global/global_context.h"

#include "common/ceph_argparse.h"
#include "common/ceph_json.h"
#include "rgw_es_query.h"


int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);

  common_init_finish(g_ceph_context);

  string expr;

  if (argc > 1) {
    expr = argv[1];
  } else {
    expr = "age >= 30";
  }

  ESQueryCompiler es_query(expr, nullptr, "x-amz-meta-");

  map<string, string, ltstr_nocase> aliases = { { "key", "name" },
                                  { "etag", "meta.etag" },
                                  { "size", "meta.size" },
                                  { "mtime", "meta.mtime" },
                                  { "lastmodified", "meta.mtime" },
                                  { "contenttype", "meta.contenttype" },
  };
  es_query.set_field_aliases(&aliases);

  map<string, ESEntityTypeMap::EntityType> generic_map = { {"bucket", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"name", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"instance", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.etag", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.contenttype", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.mtime", ESEntityTypeMap::ES_ENTITY_DATE},
                                                           {"meta.size", ESEntityTypeMap::ES_ENTITY_INT} };
  ESEntityTypeMap gm(generic_map);
  es_query.set_generic_type_map(&gm);

  map<string, ESEntityTypeMap::EntityType> custom_map = { {"str", ESEntityTypeMap::ES_ENTITY_STR},
                                                          {"int", ESEntityTypeMap::ES_ENTITY_INT},
                                                          {"date", ESEntityTypeMap::ES_ENTITY_DATE} };
  ESEntityTypeMap em(custom_map);
  es_query.set_custom_type_map(&em);

  string err;
  
  bool valid = es_query.compile(&err);
  if (!valid) {
    cout << "failed to compile query: " << err << std::endl;
    return EINVAL;
  }

  JSONFormatter f;
  encode_json("root", es_query, &f);

  f.flush(cout);

  return 0;
}

