/*
 * workload_generator.cc
 *
 *  Created on: Mar 12, 2012
 *      Author: jecluis
 */
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <signal.h>
#include "os/FileStore.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/debug.h"
#include <boost/scoped_ptr.hpp>
#include "workload_generator.h"

boost::scoped_ptr<WorkloadGenerator> wrkldgen;
const coll_t WorkloadGenerator::META_COLL("meta");
const coll_t WorkloadGenerator::TEMP_COLL("temp");

void sig_handler(int sig) {
	if (sig == SIGINT) {
		wrkldgen->stop();
	}
}

WorkloadGenerator::WorkloadGenerator(vector<const char*> args) :
		NUM_COLLS(DEF_NUM_COLLS), 
		NUM_OBJ_PER_COLL(DEF_NUM_OBJ_PER_COLL),
		store(0), rng(time(NULL)), in_flight(0),
		lock("State Lock"), stop_running(false)
{
	init_args(args);

	::mkdir("workload_gen_dir", 0777);
	ObjectStore *store_ptr = new FileStore(string("workload_gen_dir"),
			string("workload_gen_journal"));
	store.reset(store_ptr);
	store->mkfs();
	store->mount();

	osr = new ObjectStore::Sequencer[NUM_COLLS];

	init();
}

void WorkloadGenerator::init_args(vector<const char*> args) {
	for (std::vector<const char*>::iterator i = args.begin();
			i != args.end(); ) {
		string val;

		if (ceph_argparse_double_dash(args, i)) {
			break;
		} else if (ceph_argparse_witharg(args, i, &val,
				"-C", "--num-collections", (char*)NULL)) {
			NUM_COLLS = strtoll(val.c_str(), NULL, 10);
		} else if (ceph_argparse_witharg(args, i, &val,
				"-O", "--num-objects", (char*)NULL)) {
			NUM_OBJ_PER_COLL = strtoll(val.c_str(), NULL, 10);
		}
//		else if (ceph_argparse_witharg(args, i, &val,
//				"-o", "--outfn", (char*)NULL)) {
//			outfn = val;
//		}
	}
}

void WorkloadGenerator::init() {

	dout(0) << "Initializing..." << dendl;

	ObjectStore::Transaction *t = new ObjectStore::Transaction;

	t->create_collection(META_COLL);
	t->create_collection(TEMP_COLL);
//	store->queue_transaction(&osr, t);
	store->apply_transaction(*t);

	wait_for_ready();

	char buf[100];
	for (int i = 0; i < NUM_COLLS; i ++) {
		memset(buf, 0, 100);
		snprintf(buf, 100, "0.%d_head", i);
		coll_t coll(buf);

		dout(0) << "Creating collection " << coll.to_str() << dendl;

		t = new ObjectStore::Transaction;

		t->create_collection(coll);

		memset(buf, 0, 100);
		snprintf(buf, 100, "pglog_0.%d_head", i);
		hobject_t coll_meta_obj(sobject_t(object_t(buf), CEPH_NOSNAP));
		t->touch(META_COLL, coll_meta_obj);

		store->queue_transaction(&osr[i], t,
				new C_WorkloadGeneratorOnReadable(this, t));
		in_flight++;
	}

	wait_for_done();
	dout(0) << "Done initializing!" << dendl;
}

int WorkloadGenerator::get_random_collection_nr() {
	return (rand() % NUM_COLLS);
}

int WorkloadGenerator::get_random_object_nr(int coll_nr) {
	return ((rand() % NUM_OBJ_PER_COLL) + (coll_nr*NUM_OBJ_PER_COLL));
}

coll_t WorkloadGenerator::get_collection_by_nr(int nr) {

	char buf[100];
	memset(buf, 0, 100);

	snprintf(buf, 100, "0.%d_head", nr);
	return coll_t(buf);
}

hobject_t WorkloadGenerator::get_object_by_nr(int nr) {

	char buf[100];
	memset(buf, 0, 100);
	snprintf(buf, 100, "%d", nr);

	return hobject_t(sobject_t(object_t(buf), CEPH_NOSNAP));
}

hobject_t WorkloadGenerator::get_coll_meta_object(coll_t coll) {
	char buf[100];
	memset(buf, 0, 100);
	snprintf(buf, 100, "pglog_%s", coll.c_str());

	return hobject_t(sobject_t(object_t(buf), CEPH_NOSNAP));
}

/**
 * We'll generate a random amount of bytes, ranging from a single byte up to
 * a couple of MB.
 */
size_t WorkloadGenerator::get_random_byte_amount(size_t min, size_t max) {
	size_t diff = max - min;

	return (size_t) (min + (rand() % diff));
}

void WorkloadGenerator::get_filled_byte_array(bufferlist& bl, size_t size) {

	static const char alphanum[] =
			"0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";
	bufferptr bp(size);
	for (unsigned int i = 0; i < size-1; i ++) {
		bp[i] = alphanum[rand() % sizeof(alphanum)];
	}
	bp[size-1] = '\0';
	bl.append(bp);
}

int WorkloadGenerator::do_write_object(ObjectStore::Transaction *t,
		coll_t coll, hobject_t obj) {

	size_t bytes = get_random_byte_amount(MIN_WRITE_BYTES, MAX_WRITE_BYTES);
	bufferlist bl;
	get_filled_byte_array(bl, bytes);
	t->write(coll, obj, 0, bl.length(), bl);

	return 0;
}

int WorkloadGenerator::do_setattr_object(ObjectStore::Transaction *t,
		coll_t coll, hobject_t obj) {
	size_t size;
	size = get_random_byte_amount(MIN_XATTR_OBJ_BYTES, MAX_XATTR_OBJ_BYTES);

	bufferlist bl;
	get_filled_byte_array(bl, size);
	t->setattr(coll, obj, "objxattr", bl);

	return 0;
}

int WorkloadGenerator::do_setattr_collection(ObjectStore::Transaction *t,
		coll_t coll) {

	size_t size;
	size = get_random_byte_amount(MIN_XATTR_COLL_BYTES, MAX_XATTR_COLL_BYTES);

	bufferlist bl;
	get_filled_byte_array(bl, size);
	t->collection_setattr(coll, "collxattr", bl);

	return 0;
}

int WorkloadGenerator::do_append_log(ObjectStore::Transaction *t,
		coll_t coll) {

	bufferlist bl;
	get_filled_byte_array(bl, LOG_APPEND_BYTES);
	hobject_t log_obj = get_coll_meta_object(coll);

	struct stat st;
	int err = store->stat(META_COLL, log_obj, &st);
	assert(err >= 0);
	t->write(META_COLL, log_obj, st.st_size, bl.length(), bl);

	return 0;
}

void WorkloadGenerator::run() {

	do {
		lock.Lock();
		wait_for_ready();

		int coll_nr = get_random_collection_nr();
		int obj_nr = get_random_object_nr(coll_nr);
		coll_t coll = get_collection_by_nr(coll_nr);
		hobject_t obj = get_object_by_nr(obj_nr);

		ObjectStore::Transaction *t = new ObjectStore::Transaction;
		int err;

		err = do_write_object(t, coll, obj);
		assert(err == 0);

		err = do_setattr_object(t, coll, obj);
		assert(err == 0);

		err = do_setattr_collection(t, coll);
		assert(err == 0);

		err = do_append_log(t, coll);
		assert(err == 0);

		store->queue_transaction(&osr[coll_nr], t,
				new C_WorkloadGeneratorOnReadable(this, t));

		in_flight++;

		lock.Unlock();
	} while (!stop_running);
}

void WorkloadGenerator::print_results() {

}


int main(int argc, char *argv[]) {
	vector<const char*> args;
	argv_to_vec(argc, (const char **)argv, args);

	global_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
	common_init_finish(g_ceph_context);
	g_ceph_context->_conf->set_val("osd_journal_size", "400");
	g_ceph_context->_conf->apply_changes(NULL);


	WorkloadGenerator *wrkldgen_ptr = new WorkloadGenerator(args);
	wrkldgen.reset(wrkldgen_ptr);
	wrkldgen->run();
	wrkldgen->print_results();

	return 0;
}
