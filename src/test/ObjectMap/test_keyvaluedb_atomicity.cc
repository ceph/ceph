// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <pthread.h>
#include "include/buffer.h"
#include "kv/KeyValueDB.h"
#include <sys/types.h>
#include <dirent.h>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <iostream>
#include <sstream>
#include "stdlib.h"
#include "global/global_context.h"
#include "global/global_init.h"

using namespace std;

const string CONTROL_PREFIX = "CONTROL";
const string PRIMARY_PREFIX = "PREFIX";
const int NUM_COPIES = 100;
const int NUM_THREADS = 30;

string prefix_gen(int i) {
  stringstream ss;
  ss << PRIMARY_PREFIX << "_" << i << std::endl;
  return ss.str();
}

int verify(KeyValueDB *db) {
  // Verify
  {
    map<int, KeyValueDB::Iterator> iterators;
    for (int i = 0; i < NUM_COPIES; ++i) {
      iterators[i] = db->get_iterator(prefix_gen(i));
      iterators[i]->seek_to_first();
    }
    while (iterators.rbegin()->second->valid()) {
      for (map<int, KeyValueDB::Iterator>::iterator i = iterators.begin();
	   i != iterators.end();
	   ++i) {
	ceph_assert(i->second->valid());
	ceph_assert(i->second->key() == iterators.rbegin()->second->key());
	bufferlist r = i->second->value();
	bufferlist l = iterators.rbegin()->second->value();
	i->second->next();
      }
    }
    for (map<int, KeyValueDB::Iterator>::iterator i = iterators.begin();
	 i != iterators.end();
	 ++i) {
      ceph_assert(!i->second->valid());
    }
  }
  return 0;
}

void *write(void *_db) {
  KeyValueDB *db = static_cast<KeyValueDB*>(_db);
  std::cout << "Writing..." << std::endl;
  for (int i = 0; i < 12000; ++i) {
    if (!(i % 10)) {
      std::cout << "Iteration: " << i << std::endl;
    }
    int key_num = rand();
    stringstream key;
    key << key_num << std::endl;
    map<string, bufferlist> to_set;
    stringstream val;
    val << i << std::endl;
    bufferptr bp(val.str().c_str(), val.str().size() + 1);
    to_set[key.str()].push_back(bp);
    
    KeyValueDB::Transaction t = db->get_transaction();
    for (int j = 0; j < NUM_COPIES; ++j) {
      t->set(prefix_gen(j), to_set);
    }
    ceph_assert(!db->submit_transaction(t));
  }
  return 0;
}

int main() {
  char *path = getenv("OBJECT_MAP_PATH");
  boost::scoped_ptr< KeyValueDB > db;
  if (!path) {
    std::cerr << "No path found, OBJECT_MAP_PATH undefined" << std::endl;
    return 0;
  }
  string strpath(path);
  std::cerr << "Using path: " << strpath << std::endl;

  std::vector<const char*> args;
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
             CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  KeyValueDB *store = KeyValueDB::create(g_ceph_context, "rocksdb", strpath);
  ceph_assert(!store->create_and_open(std::cerr));
  db.reset(store);

  verify(db.get());

  vector<pthread_t> threads(NUM_THREADS);
  for (vector<pthread_t>::iterator i = threads.begin();
       i != threads.end();
       ++i) {
    pthread_create(&*i, 0, &write, static_cast<void *>(db.get()));
  }
  for (vector<pthread_t>::iterator i = threads.begin();
       i != threads.end();
       ++i) {
    void *tmp;
    pthread_join(*i, &tmp);
  }
  verify(db.get());
}
