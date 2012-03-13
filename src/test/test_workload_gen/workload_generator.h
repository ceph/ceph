/*
 * workload_generator.h
 *
 *  Created on: Mar 12, 2012
 *      Author: jecluis
 */

#ifndef WORKLOAD_GENERATOR_H_
#define WORKLOAD_GENERATOR_H_

#include "os/FileStore.h"
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <pthread.h>

typedef boost::mt11213b gen_type;

class WorkloadGenerator
{
private:
	  static const int NUM_THREADS 		= 2;
	  static const int MAX_IN_FLIGHT	= 50;

	  static const int DEF_NUM_OBJ_PER_COLL = 6000;
	  static const int DEF_NUM_COLLS	= 30;

	  static const coll_t META_COLL;
	  static const coll_t TEMP_COLL;

	  static const size_t MIN_WRITE_BYTES = 1;
	  static const size_t MAX_WRITE_MB	  = 5;
	  static const size_t MAX_WRITE_BYTES = (MAX_WRITE_MB * 1024 * 1024);

	  static const size_t MIN_XATTR_OBJ_BYTES 	= 2;
	  static const size_t MAX_XATTR_OBJ_BYTES 	= 300;
	  static const size_t MIN_XATTR_COLL_BYTES 	= 4;
	  static const size_t MAX_XATTR_COLL_BYTES 	= 600;
//	  static const size_t XATTR_NAME_BYTES		= 30;

	  static const size_t LOG_APPEND_BYTES		= 1024;

	  int NUM_COLLS;
	  int NUM_OBJ_PER_COLL;

	  boost::scoped_ptr<ObjectStore> store;

	  gen_type rng;
	  int in_flight;
	  ObjectStore::Sequencer *osr;

	  Mutex lock;
	  Cond cond;

	  bool stop_running;

	  void wait_for_ready() {
		  while (in_flight >= MAX_IN_FLIGHT)
			  cond.Wait(lock);
	  }

	  void wait_for_done() {
		  Mutex::Locker locker(lock);
		  while (in_flight)
			  cond.Wait(lock);
	  }

	  void init_args(vector<const char*> args);
	  void init();

	  int get_random_collection_nr();
	  int get_random_object_nr(int coll_nr);

	  coll_t get_collection_by_nr(int nr);
	  hobject_t get_object_by_nr(int nr);
	  hobject_t get_coll_meta_object(coll_t coll);

	  size_t get_random_byte_amount(size_t min, size_t max);
	  void get_filled_byte_array(bufferlist& bl, size_t size);

	  int do_write_object(ObjectStore::Transaction *t,
			  coll_t coll, hobject_t obj);
	  int do_setattr_object(ObjectStore::Transaction *t,
	  			  coll_t coll, hobject_t obj);
	  int do_setattr_collection(ObjectStore::Transaction *t, coll_t coll);
	  int do_append_log(ObjectStore::Transaction *t, coll_t coll);

public:
	WorkloadGenerator(vector<const char*> args);
	~WorkloadGenerator() {
		store->umount();
	}

	class C_WorkloadGeneratorOnReadable : public Context {
		WorkloadGenerator *state;
		ObjectStore::Transaction *t;

	public:
		C_WorkloadGeneratorOnReadable(WorkloadGenerator *state,
				ObjectStore::Transaction *t) : state(state), t(t) {}

		void finish(int r) {
			dout(0) << "Got one back!" << dendl;
			Mutex::Locker locker(state->lock);
			state->in_flight--;
			state->cond.Signal();
		}
	};


	void run(void);
	void print_results(void);
	void stop() {
		stop_running = true;
	}
};

#endif /* WORKLOAD_GENERATOR_H_ */
