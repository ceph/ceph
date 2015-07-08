// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 SanDisk
 *
 * Author: Varada Kari<varada.kari@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it. 
 *
 */

/*
 *  Contains the interfaces what a backend DB plugin should implement to use DB
 *  as keyvalue store backend adhearing to KeyValueDB semantics.
 */
#ifndef PLUGGABLEDB_INTERFACES_H
#define PLUGGABLEDB_INTERFACES_H
#include <string>
#include <vector>
#include "PluggableDBIterator.h"
#include <stdint.h>

using namespace std;

enum DBOpType {
  DB_OP_WRITE,
  DB_OP_DELETE,
};

typedef struct value {
  char *data;
  uint64_t datalen;
}value_t;

typedef struct DBOp {
  DBOpType type;
  std::string data;
}DBOp;
      	  
extern "C" {
int dbinit(const char* osd_data, int osdid, std::map<string, string> *options);
void dbclose();
int submit_transaction(map<string, DBOp>& ops);
value_t* getobject(const string& key);
PluggableDBIterator* get_iterator();
uint64_t getdbsize();
int getstatfs(struct statfs *buf);
}

#endif
