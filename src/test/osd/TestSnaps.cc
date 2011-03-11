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

struct SnapTestGenerator : public TestOpGenerator
{
  TestOp *nextop;
  int op;
  int ops;
  int objects;
  SnapTestGenerator(int ops, int objects) : 
    nextop(0), op(0), ops(ops), objects(objects) 
  {}


  TestOp *next(RadosTestContext &context)
  {
    op++;
    if (op <= objects) {
      stringstream oid;
      oid << op;
      cout << "Writing initial " << oid.str() << std::endl;
      return new WriteOp(context, oid.str());
    } else if (op >= ops) {
      return 0;
    }

    if (nextop) {
      TestOp *retval = nextop;
      nextop = 0;
      return retval;
    }
		
    int switchval = rand() % 50;
    if (switchval < 20) {
      string oid = *(rand_choose(context.oid_not_in_use));
      cout << "Reading " << oid << std::endl;
      return new ReadOp(context, oid);
    } else if (switchval < 40) {
      string oid = *(rand_choose(context.oid_not_in_use));
      cout << "Writing " << oid << " current snap is " 
	   << context.current_snap << std::endl;
      return new WriteOp(context, oid);
    } else if ((switchval < 45) && !context.snaps.empty()) {
      int snap = *(rand_choose(context.snaps));
      string oid = *(rand_choose(context.oid_not_in_use));
      cout << "RollingBack " << oid << " to " << snap << std::endl;
      nextop = new ReadOp(context, oid);
      return new RollbackOp(context, oid, snap);
    } else if ((switchval < 47) && !context.snaps.empty()) {
      int snap = *(rand_choose(context.snaps));
      cout << "RemovingSnap " << snap << std::endl;
      return new SnapRemoveOp(context, snap);
    } else {
      cout << "Snapping" << std::endl;
      return new SnapCreateOp(context);
    }
  }
};
		
int main(int argc, char **argv)
{
  int ops = 1000;
  int objects = 50;
  int max_in_flight = 16;
  if (argc > 1) {
    ops = atoi(argv[1]);
  }

  if (argc > 2) {
    objects = atoi(argv[2]);
  }

  if (argc > 3) {
    max_in_flight = atoi(argv[3]);
  }

  if (max_in_flight > objects) {
    cerr << "Error: max_in_flight must be greater than the number of objects" 
	 << std::endl;
    return 0;
  }
		
  string pool_name = "casdata";
  RadosTestContext context(pool_name, max_in_flight);
  SnapTestGenerator gen = SnapTestGenerator(ops, objects);
  context.loop(gen);
  context.shutdown();
  cerr << context.errors << " errors." << std::endl;
  return 0;
}
