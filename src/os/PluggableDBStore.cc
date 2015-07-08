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
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/ceph_crypto.h"
#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <errno.h>
#include "common/perf_counters.h"
#include "PluggableDBStore.h"
#include <dlfcn.h>
#include <sys/stat.h>

#define dout_subsys ceph_subsys_keyvaluestore
using std::string;

#define numelems(a)      (sizeof(a)/sizeof(*(a)))

bool PluggableDBStore::create_if_missing = false;

/*
 * Function pointers.
 */
static int
(*ptr_init)(const char* osd_data, int osdid, map<string, string> *options);

static void
(*ptr_close)();

static int
(*ptr_submit_transaction)(map<string, DBOp>& ops);

static PluggableDBIterator*
(*ptr_get_iterator)();

static value_t*
(*ptr_getobject)(const string& key);

static uint64_t
(*ptr_getdbsize)();

static int
(*ptr_getstatfs)(struct statfs *buf);

/*
 * Linkage table.
 */
static struct {
    const char *name;
    void       *func;
} table[] ={
    { "dbinit",                &ptr_init},
    { "dbclose",               &ptr_close},
    { "submit_transaction",    &ptr_submit_transaction},
    { "get_iterator",          &ptr_get_iterator},
    { "getobject",             &ptr_getobject},
    { "getdbsize",             &ptr_getdbsize},
    { "getstatfs",             &ptr_getstatfs},
};

static void
undefined(const char *sym)
{
	derr << "Undefined symbol" << sym << dendl;
}

/*
 * init
 */
int
PluggableDBStore::DBInit(const char* osd_data, int osdid, map<string, string> *options)
{
    if (unlikely(!ptr_init))
        undefined("init");

    return (*ptr_init)(osd_data, osdid, options);
}

/*
 * close
 */
void
PluggableDBStore::DBClose()
{
    if (unlikely(!ptr_close))
        undefined("close");

    (*ptr_close)();
}

/*
 * submit TX
 */
int
PluggableDBStore::DBSubmitTransaction(map<string, DBOp>& ops)
{
    if (unlikely(!ptr_submit_transaction))
        undefined("submit_transaction");

    return (*ptr_submit_transaction)(ops);
}
/*
 * Get Iterator
 */
PluggableDBIterator*
PluggableDBStore::DBGetIterator()
{
    if (unlikely(!ptr_get_iterator))
        undefined("get_iterator");

    return (*ptr_get_iterator)();
}

/*
 * GetObject
 */
value_t*
PluggableDBStore::DBGetObject(const string& key)
{
    if (unlikely(!ptr_getobject))
        undefined("getobject");

    return (*ptr_getobject)(key);
}

/*
 * GetDBSize
 */
uint64_t
PluggableDBStore::DBGetSize()
{
    if (unlikely(!ptr_getdbsize))
        undefined("getobject");

    return (*ptr_getdbsize)();
}

/*
 * GetStatFs
 */
int
PluggableDBStore::DBGetStatfs(struct statfs *buf)
{
    if (unlikely(!ptr_getstatfs))
        undefined("getstatfs");

    return (*ptr_getstatfs)(buf);
}


uint64_t PluggableDBStore::get_estimated_size(map<string,uint64_t> &extra) {
	uint64_t size = DBGetSize();
	extra["total"] = size;
    return size;
}

int PluggableDBStore::get_statfs(struct statfs *buf) {
    int r = DBGetStatfs(buf);

    return r;
}

/*
 * Given a pathname, assume it is a Store library and try loading it.
 */
int
PluggableDBStore::load_symbols(const char *path)
{
    int i;
    void  *dl = dlopen(path, RTLD_NOW);
    char *err = dlerror();

    if (!dl) {
       derr <<" Error opening the library" << err << dendl;
       return -1;
    }
	library = dl;
    
    int n = numelems(table);
    for (i = 0; i < n; i++) {
        const char *name = table[i].name;
        void *func = dlsym(dl, name);
        if (func) {
            *(void **)table[i].func = func;
	    } else {
	       derr << "Error loading symbol: " << name << dendl;	
    	   err = dlerror();
           derr <<" Error loading symbol: " << err << dendl;
	       return -1;
	    }
    }
    return 0;
}

int PluggableDBStore::_test_init()
{
  dout(10) << __func__ << dendl;
  PluggableDBStore::create_if_missing = true;
  return 0;
}

int PluggableDBStore::getosdid()
{
  dout(30) << __func__ << dendl;
  char *end;
  const char *id = g_conf->name.get_id().c_str();
  int whoami = strtol(id, &end, 10);
  return whoami;
}

int PluggableDBStore::init(string opt_str)
{
  dout(10) << __func__ << opt_str << dendl;
  int ret = load_symbols(g_conf->keyvaluestore_backend_library.c_str());
  assert(ret ==0);
  const char* path = g_conf->osd_data.data();
  map<string, string> options;
  if (PluggableDBStore::create_if_missing) {
    options["create_if_missing"] = "true";
  } else {
    options["create_if_missing"] = "false";
  }
  int status = DBInit(path, getosdid(), &options);
  assert(status);
  init_done = true;
  return 0;
}

int PluggableDBStore::do_open(ostream &out, bool create_if_missing)
{
  dout(10) << __func__ <<dendl;
  if (!init_done) {
    init(string());
  }
  PerfCountersBuilder plb(g_ceph_context, "db", l_PluggableDB_first, l_PluggableDB_last);
  plb.add_u64_counter(l_PluggableDB_gets, "db_get");
  plb.add_u64_counter(l_PluggableDB_txns, "db_transaction");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  return 0;
}

PluggableDBStore::PluggableDBStore(CephContext *c) :
  cct(c),
  logger(NULL),init_done(false)
{
}

PluggableDBStore::~PluggableDBStore()
{
  close();
  dlclose(library);
  delete logger;
}

void PluggableDBStore::close()
{
  //shutdown the DB backend
  DBClose();
  if (logger)
    cct->get_perfcounters_collection()->remove(logger);
}

int PluggableDBStore::submit_transaction(KeyValueDB::Transaction t)
{
  dout(20) << __func__ <<dendl;
  PluggableDBTransactionImpl * _t =
    static_cast<PluggableDBTransactionImpl *>(t.get());

  int ret = DBSubmitTransaction(_t->ops);
  logger->inc(l_PluggableDB_txns);
  return ret;
}

int PluggableDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  return submit_transaction(t);
}

void PluggableDBStore::PluggableDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  string key = combine_strings(prefix, k);
  DBOp op;
  op.type = DB_OP_WRITE;
  string data;
  to_set_bl.copy(0,to_set_bl.length(), data);
  op.data = data;
  ops[key] = op;
}

void PluggableDBStore::PluggableDBTransactionImpl::rmkey(const string &prefix,
												         const string &k)
{
  string key = combine_strings(prefix, k);
  DBOp op;
  op.type = DB_OP_DELETE;
  ops[key] = op;
}

void PluggableDBStore::PluggableDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    pair<string,string> key1 = it->raw_key();
    string key =  key1.first + "\001" + key1.second;
    DBOp op;
    op.type = DB_OP_DELETE;
    ops[key] = op;
  }
}

int PluggableDBStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out
    )
{
  dout(30) << __func__ << dendl;
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    string key = combine_strings(prefix, *i);
	value_t *value = DBGetObject(key);
	if (value->datalen == 0) {
		free(value);
		break;
	}
	bufferlist bl;
	bufferptr bp = buffer::claim_char(value->datalen, value->data);
	bl.push_back(bp);
    out->insert(make_pair(*i, bl));
	free(value);
  }
  logger->inc(l_PluggableDB_gets);
  return 0;
}


string PluggableDBStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(1);
  out.append(value);
  return out;
}

ceph::bufferlist PluggableDBStore::to_bufferlist(const string& obj)
{
  bufferlist bl;
  bufferptr bp = buffer::create(obj.length());
  memcpy(bp.c_str(), obj.data(), obj.length());
  bl.push_back(bp);
  return bl;
}

int PluggableDBStore::split_key(string in_prefix, string *prefix, string *key)
{
  dout(30) << __func__ << "Key : " << in_prefix << dendl;	
  size_t prefix_len = in_prefix.find('\1');
  if (prefix_len >= in_prefix.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(in_prefix, 0, prefix_len);
  if (key)
    *key= string(in_prefix, prefix_len + 1);
  return 0;
}

PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::PluggableDBWholeSpaceIteratorImpl(PluggableDBStore *parent)
	:store(parent)
{
		iter = store->DBGetIterator();
}

int PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::seek_to_first(const string &prefix)
{
  dout(30) << __func__ << " prefix: " << prefix << dendl;
  iter->seek_to_first(prefix);
  return 0;
}

int PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::seek_to_last()
{
  dout(30) << __func__ << dendl;
  iter->seek_to_last();
  return valid();
}

int PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::seek_to_last(const string &prefix)
{
  dout(30) << __func__ <<" prefix: " << prefix << dendl;
  iter->seek_to_first(prefix);
  while (valid()) {
    pair<string,string> key = raw_key();
    if (key.first == prefix)
      next();
    else
      break;
  }
  return 0;
}

int PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after) {
  dout(30) << __func__ <<" prefix: " << prefix << " after: " <<  dendl;
  string key = combine_strings(prefix, after);
  iter->upper_bound(key);
  return 0;
}

int PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to) {
  dout(30) << __func__ <<" Prefix: " << prefix <<" to: " << to << dendl;
  string key = combine_strings(prefix, to);
  iter->lower_bound(key);
  return 0;
}

bool PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::valid() {
  dout(30) << __func__ << ":" << iter->valid() << dendl;
  return iter->valid();
}

int PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::next() {
  string key_ = iter->key();
  dout(30) << __func__ <<" key: "  << key_ << dendl;
  string prefix, key;
  split_key(key_, &prefix, &key);
  return upper_bound(prefix, key);
}

int PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::prev() {
  string key_ = iter->key();
  dout(30) << __func__ << " key: " << key_ << dendl;
  iter->prev();
  return 0;
}

string PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::key() {
  dout(30) << __func__ << dendl;
  string key_ = iter->key();
  string prefix, key;
  split_key(key_, &prefix, &key);
  return key;
}

pair<string,string> PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::raw_key() {
  dout(30) << __func__ << dendl;
  string key_ = iter->key();
  string prefix, key;
  split_key(key_, &prefix, &key);
  return make_pair(prefix, key);
}

bufferlist PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::value() {
  dout(30) << __func__ << dendl;
  string value = iter->value();
  return store->to_bufferlist(value);
}

int PluggableDBStore::PluggableDBWholeSpaceIteratorImpl::status() {
  dout(30) << __func__ << dendl;
  return 0;
}
