// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "common/Mutex.h"
#include "common/Cond.h"

#include <iostream>
#include <sstream>
#include <map>
#include <set>
#include <list>
#include <string>
#include <stdlib.h>
#include <unistd.h>

#include "test/osd/RadosModel.h"

using namespace std;

TestOp::~TestOp()
{
}

TestOpGenerator::~TestOpGenerator()
{
}

struct ReadWriteGenerator : public TestOpGenerator
{
  TestOp *nextop;
  int op;
  int ops;
  int objects;
  int read_percent;
  ReadWriteGenerator(int ops, int objects, int read_percent) :
    nextop(0), op(0), ops(ops), objects(objects), read_percent(read_percent)
  {}


  TestOp *next(RadosTestContext &context)
  {
    op++;
    if (op <= objects) {
      stringstream oid;
      oid << op;
      cout << "Writing initial " << oid.str() << std::endl;
      return new WriteOp(&context, oid.str());
    } else if (op >= ops) {
      return 0;
    }

    if (nextop) {
      TestOp *retval = nextop;
      nextop = 0;
      return retval;
    }
    int switchval = rand() % 110;
    if (switchval < read_percent) {
      string oid = *(rand_choose(context.oid_not_in_use));
      cout << "Reading " << oid << std::endl;
      return new ReadOp(&context, oid, 0);
    } else if (switchval < 100) {
      string oid = *(rand_choose(context.oid_not_in_use));
      cout << "Writing " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new WriteOp(&context, oid, 0);
    } else {
      string oid = *(rand_choose(context.oid_not_in_use));
      cout << "Deleting " << oid << " current snap is "
	   << context.current_snap << std::endl;
      return new DeleteOp(&context, oid, 0);
    }
  }
};

int main(int argc, char **argv)
{
  int ops = 10000;
  int objects = 500;
  int read_percent = 50;
  int max_in_flight = 16;
  int size = 4000000; // 4 MB
  if (argc > 1) {
    ops = atoi(argv[1]);
  }

  if (argc > 2) {
    objects = atoi(argv[2]);
  }

  if (argc > 3) {
    read_percent = atoi(argv[3]);
  }

  if (argc > 4) {
    max_in_flight = atoi(argv[4]);
  }

  if (argc > 5) {
    size = atoi(argv[5]);
  }

  if (max_in_flight > objects) {
    cerr << "Error: max_in_flight must be less than the number of objects"
	 << std::endl;
    return 0;
  }

  char *id = getenv("CEPH_CLIENT_ID");
  if (id) cerr << "Client id is: " << id << std::endl;
  string pool_name = "data";
  VarLenGenerator cont_gen(size);
  RadosTestContext context(pool_name, max_in_flight, cont_gen, id);

  ReadWriteGenerator gen = ReadWriteGenerator(ops, objects, read_percent);
  context.loop(&gen);

  context.shutdown();
  cerr << context.errors << " errors." << std::endl;
  return 0;
}
