// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#include "include/interval_set.h"
#include "include/buffer.h"
#include <list>
#include <map>
#include <set>
#include "include/rados/librados.h"
#include "RadosModel.h"
#include "TestOpStat.h"


void TestOp::begin()
{
  //if (stat) stat->begin(this);
  _begin();
}

void TestOp::finish(TestOp::CallbackInfo *info)
{
  _finish(info);
  //if (stat && finished()) stat->end(this);
}

void read_callback(librados::completion_t comp, void *arg) {
  TestOp* op = static_cast<TestOp*>(arg);
  op->finish(NULL);
}

void write_callback(librados::completion_t comp, void *arg) {
  std::pair<TestOp*, TestOp::CallbackInfo*> *args =
    static_cast<std::pair<TestOp*, TestOp::CallbackInfo*> *>(arg);
  TestOp* op = args->first;
  TestOp::CallbackInfo *info = args->second;
  op->finish(info);
  delete args;
  delete info;
}
