// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

void TestOp::finish()
{
  _finish();
  //if (stat && finished()) stat->end(this);
}

void callback(librados::completion_t cb, void *arg) {
  TestOp *op = static_cast<TestOp*>(arg);
  op->finish();
}
