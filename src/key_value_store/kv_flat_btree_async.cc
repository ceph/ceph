/*
 * Key-value store using librados
 *
 * September 2, 2012
 * Eleanor Cawthon
 * eleanor.cawthon@inktank.com
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "include/compat.h"
#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree_async.h"
#include "key_value_store/kvs_arg_types.h"
#include "include/rados/librados.hpp"
#include "common/ceph_context.h"
#include "common/Clock.h"
#include "include/types.h"

#include <errno.h>
#include <string>
#include <iostream>
#include <cassert>
#include <climits>
#include <cmath>
#include <sstream>
#include <stdlib.h>
#include <iterator>

using ceph::bufferlist;

bool index_data::is_timed_out(utime_t now, utime_t timeout) const {
  return prefix != "" && now - ts > timeout;
}

void IndexCache::clear() {
  k2itmap.clear();
  t2kmap.clear();
}

void IndexCache::push(const string &key, const index_data &idata) {
  if (cache_size == 0) {
    return;
  }
  index_data old_idata;
  map<key_data, pair<index_data, utime_t> >::iterator old_it =
      k2itmap.lower_bound(key_data(key));
  if (old_it != k2itmap.end()) {
    t2kmap.erase(old_it->second.second);
    k2itmap.erase(old_it);
  }
  map<key_data, pair<index_data, utime_t> >::iterator new_it =
      k2itmap.find(idata.kdata);
  if (new_it != k2itmap.end()) {
    utime_t old_time = new_it->second.second;
    t2kmap.erase(old_time);
  }
  utime_t time = ceph_clock_now();
  k2itmap[idata.kdata] = make_pair(idata, time);
  t2kmap[time] = idata.kdata;
  if ((int)k2itmap.size() > cache_size) {
    pop();
  }

}

void IndexCache::push(const index_data &idata) {
  if (cache_size == 0) {
    return;
  }
  if (k2itmap.count(idata.kdata) > 0) {
    utime_t old_time = k2itmap[idata.kdata].second;
    t2kmap.erase(old_time);
    k2itmap.erase(idata.kdata);
  }
  utime_t time = ceph_clock_now();
  k2itmap[idata.kdata] = make_pair(idata, time);
  t2kmap[time] = idata.kdata;
  if ((int)k2itmap.size() > cache_size) {
    pop();
  }
}

void IndexCache::pop() {
  if (cache_size == 0) {
    return;
  }
  map<utime_t, key_data>::iterator it = t2kmap.begin();
  utime_t time = it->first;
  key_data kdata = it->second;
  k2itmap.erase(kdata);
  t2kmap.erase(time);
}

void IndexCache::erase(key_data kdata) {
  if (cache_size == 0) {
    return;
  }
  if (k2itmap.count(kdata) > 0) {
    utime_t c = k2itmap[kdata].second;
    k2itmap.erase(kdata);
    t2kmap.erase(c);
  }
}

int IndexCache::get(const string &key, index_data *idata) const {
  if (cache_size == 0) {
    return -ENODATA;
  }
  if ((int)k2itmap.size() == 0) {
    return -ENODATA;
  }
  map<key_data, pair<index_data, utime_t> >::const_iterator it =
      k2itmap.lower_bound(key_data(key));
  if (it == k2itmap.end() || !(it->second.first.min_kdata < key_data(key))) {
    return -ENODATA;
  } else {
    *idata = it->second.first;
  }
  return 0;
}

int IndexCache::get(const string &key, index_data *idata,
    index_data *next_idata) const {
  if (cache_size == 0) {
    return -ENODATA;
  }
  map<key_data, pair<index_data, utime_t> >::const_iterator it =
      k2itmap.lower_bound(key_data(key));
  if (it == k2itmap.end() || ++it == k2itmap.end()) {
    return -ENODATA;
  } else {
    --it;
    if (!(it->second.first.min_kdata < key_data(key))){
      //stale, should be reread.
      return -ENODATA;
    } else {
      *idata = it->second.first;
      ++it;
      if (it != k2itmap.end()) {
	*next_idata = it->second.first;
      }
    }
  }
  return 0;
}

int KvFlatBtreeAsync::nothing() {
  return 0;
}

int KvFlatBtreeAsync::wait() {
  if (rand() % 10 == 0) {
    usleep(wait_ms);
  }
  return 0;
}

int KvFlatBtreeAsync::suicide() {
  if (rand() % 10 == 0) {
    if (verbose) cout << client_name << " is suiciding" << std::endl;
    return 1;
  }
  return 0;
}

int KvFlatBtreeAsync::next(const index_data &idata, index_data * out_data)
{
  if (verbose) cout << "\t\t" << client_name << "-next: finding next of "
      << idata.str()
      << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<std::string, bufferlist> kvs;
  oro.omap_get_vals2(idata.kdata.encoded(),1,&kvs, nullptr, &err);
  err = io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    if (verbose) cout << "\t\t\t" << client_name
	<< "-next: getting index failed with error "
	<< err << std::endl;
    return err;
  }
  if (!kvs.empty()) {
    out_data->kdata.parse(kvs.begin()->first);
    auto b = kvs.begin()->second.cbegin();
    out_data->decode(b);
    if (idata.is_timed_out(ceph_clock_now(), timeout)) {
      if (verbose) cout << client_name << " THINKS THE OTHER CLIENT DIED."
	  << std::endl;
      //the client died after deleting the object. clean up.
      cleanup(idata, err);
    }
  } else {
    err = -EOVERFLOW;
  }
  return err;
}

int KvFlatBtreeAsync::prev(const index_data &idata, index_data * out_data)
{
  if (verbose) cout << "\t\t" << client_name << "-prev: finding prev of "
      << idata.str() << std::endl;
  int err = 0;
  bufferlist inbl;
  idata_from_idata_args in_args;
  in_args.idata = idata;
  in_args.encode(inbl);
  bufferlist outbl;
  err = io_ctx.exec(index_name,"kvs", "get_prev_idata", inbl, outbl);
  if (err < 0){
    if (verbose) cout << "\t\t\t" << client_name
	<< "-prev: getting index failed with error "
	<< err << std::endl;
    if (idata.is_timed_out(ceph_clock_now(), timeout)) {
      if (verbose) cout << client_name << " THINKS THE OTHER CLIENT DIED."
	  << std::endl;
      //the client died after deleting the object. clean up.
      err = cleanup(idata, err);
      if (err == -ESUICIDE) {
	return err;
      } else {
	err = 0;
      }
    }
    return err;
  }
  auto it = outbl.cbegin();
  in_args.decode(it);
  *out_data = in_args.next_idata;
  if (verbose) cout << "\t\t" << client_name << "-prev: prev is "
      << out_data->str()
      << std::endl;
  return err;
}

int KvFlatBtreeAsync::read_index(const string &key, index_data * idata,
    index_data * next_idata, bool force_update) {
  int err = 0;
  if (!force_update) {
    if (verbose) cout << "\t" << client_name
	<< "-read_index: getting index_data for " << key
	<< " from cache" << std::endl;
    icache_lock.Lock();
    if (next_idata != NULL) {
      err = icache.get(key, idata, next_idata);
    } else {
      err = icache.get(key, idata);
    }
    icache_lock.Unlock();

    if (err == 0) {
      //if (verbose) cout << "CACHE SUCCESS" << std::endl;
      return err;
    } else {
      if (verbose) cout << "NOT IN CACHE" << std::endl;
    }
  }

  if (verbose) cout << "\t" << client_name
      << "-read_index: getting index_data for " << key
      << " from object" << std::endl;
  librados::ObjectReadOperation oro;
  bufferlist raw_val;
  std::set<std::string> key_set;
  key_set.insert(key_data(key).encoded());
  std::map<std::string, bufferlist> kvmap;
  std::map<std::string, bufferlist> dupmap;
  oro.omap_get_vals_by_keys(key_set, &dupmap, &err);
  oro.omap_get_vals2(key_data(key).encoded(),
      (cache_size / cache_refresh >= 2? cache_size / cache_refresh: 2),
      &kvmap, nullptr, &err);
  err = io_ctx.operate(index_name, &oro, NULL);
  utime_t mytime = ceph_clock_now();
  if (err < 0){
    cerr << "\t" << client_name
	<< "-read_index: getting keys failed with "
	<< err << std::endl;
    ceph_abort_msg(client_name + "-read_index: reading index failed");
    return err;
  }
  kvmap.insert(dupmap.begin(), dupmap.end());
  for (map<string, bufferlist>::iterator it = ++kvmap.begin();
      it != kvmap.end();
      ++it) {
    bufferlist bl = it->second;
    auto blit = bl.cbegin();
    index_data this_idata;
    this_idata.decode(blit);
    if (this_idata.is_timed_out(mytime, timeout)) {
      if (verbose) cout << client_name
	  << " THINKS THE OTHER CLIENT DIED. (mytime is "
  	<< mytime.sec() << "." << mytime.usec() << ", idata.ts is "
  	<< this_idata.ts.sec() << "." << this_idata.ts.usec()
  	<< ", it has been " << (mytime - this_idata.ts).sec()
	<< '.' << (mytime - this_idata.ts).usec()
	<< ", timeout is " << timeout << ")" << std::endl;
      //the client died after deleting the object. clean up.
      if (cleanup(this_idata, -EPREFIX) == -ESUICIDE) {
        return -ESUICIDE;
      }
      return read_index(key, idata, next_idata, force_update);
    }
    icache_lock.Lock();
    icache.push(this_idata);
    icache_lock.Unlock();
  }
  auto b = kvmap.begin()->second.cbegin();
  idata->decode(b);
  idata->kdata.parse(kvmap.begin()->first);
  if (verbose) cout << "\t" << client_name << "-read_index: kvmap_size is "
      << kvmap.size()
      << ", idata is " << idata->str() << std::endl;

  ceph_assert(idata->obj != "");
  icache_lock.Lock();
  icache.push(key, *idata);
  icache_lock.Unlock();

  if (next_idata != NULL && idata->kdata.prefix != "1") {
    next_idata->kdata.parse((++kvmap.begin())->first);
    auto nb = (++kvmap.begin())->second.cbegin();
    next_idata->decode(nb);
    icache_lock.Lock();
    icache.push(*next_idata);
    icache_lock.Unlock();
  }
  return err;
}

int KvFlatBtreeAsync::split(const index_data &idata) {
  int err = 0;
  opmap['l']++;

  if (idata.prefix != "") {
    return -EPREFIX;
  }

  rebalance_args args;
  args.bound = 2 * k - 1;
  args.comparator = CEPH_OSD_CMPXATTR_OP_GT;
  err = read_object(idata.obj, &args);
  args.odata.max_kdata = idata.kdata;
  if (err < 0) {
    if (verbose) cout << "\t\t" << client_name << "-split: read object "
	<< args.odata.name
	<< " got " << err << std::endl;
    return err;
  }

  if (verbose) cout << "\t\t" << client_name << "-split: splitting "
      << idata.obj
      << ", which has size " << args.odata.size
      << " and actual size " << args.odata.omap.size() << std::endl;

  ///////preparations that happen outside the critical section
  //for prefix index
  vector<object_data> to_create;
  vector<object_data> to_delete;
  to_delete.push_back(object_data(idata.min_kdata,
      args.odata.max_kdata, args.odata.name, args.odata.version));

  //for lower half object
  map<std::string, bufferlist>::const_iterator it = args.odata.omap.begin();
  client_index_lock.Lock();
  to_create.push_back(object_data(to_string(client_name, client_index++)));
  client_index_lock.Unlock();
  for (int i = 0; i < k; i++) {
    to_create[0].omap.insert(*it);
    ++it;
  }
  to_create[0].min_kdata = idata.min_kdata;
  to_create[0].max_kdata = key_data(to_create[0].omap.rbegin()->first);

  //for upper half object
  client_index_lock.Lock();
  to_create.push_back(object_data(to_create[0].max_kdata,
        args.odata.max_kdata,
        to_string(client_name, client_index++)));
  client_index_lock.Unlock();
  to_create[1].omap.insert(
      ++args.odata.omap.find(to_create[0].omap.rbegin()->first),
      args.odata.omap.end());

  //setting up operations
  librados::ObjectWriteOperation owos[6];
  vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > ops;
  index_data out_data;
  set_up_prefix_index(to_create, to_delete, &owos[0], &out_data, &err);
  ops.push_back(make_pair(
      pair<int, string>(ADD_PREFIX, index_name),
      &owos[0]));
  for (int i = 1; i < 6; i++) {
    ops.push_back(make_pair(make_pair(0,""), &owos[i]));
  }
  set_up_ops(to_create, to_delete, &ops, out_data, &err);

  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for idata.val
  err = perform_ops("\t\t" + client_name + "-split:", out_data, &ops);
  if (err < 0) {
    return err;
  }
  if (verbose) cout << "\t\t" << client_name << "-split: done splitting."
      << std::endl;
  /////END CRITICAL SECTION/////
  icache_lock.Lock();
  for (vector<delete_data>::iterator it = out_data.to_delete.begin();
      it != out_data.to_delete.end(); ++it) {
    icache.erase(it->max);
  }
  for (vector<create_data>::iterator it = out_data.to_create.begin();
      it != out_data.to_create.end(); ++it) {
    icache.push(index_data(*it));
  }
  icache_lock.Unlock();
  return err;
}

int KvFlatBtreeAsync::rebalance(const index_data &idata1,
    const index_data &next_idata){
  opmap['m']++;
  int err = 0;

  if (idata1.prefix != "") {
    return -EPREFIX;
  }

  rebalance_args args1;
  args1.bound = k + 1;
  args1.comparator = CEPH_OSD_CMPXATTR_OP_LT;
  index_data idata2 = next_idata;

  rebalance_args args2;
  args2.bound = k + 1;
  args2.comparator = CEPH_OSD_CMPXATTR_OP_LT;

  if (idata1.kdata.prefix == "1") {
    //this is the highest key in the index, so it doesn't have a next.

    //read the index for the previous entry
    err = prev(idata1, &idata2);
    if (err == -ERANGE) {
      if (verbose) cout << "\t\t" << client_name
	  << "-rebalance: this is the only node, "
  	  << "so aborting" << std::endl;
      return -EUCLEAN;
    } else if (err < 0) {
      return err;
    }

    //read the first object
    err = read_object(idata1.obj, &args2);
    if (err < 0) {
      if (verbose) cout << "reading " << idata1.obj << " failed with " << err
	  << std::endl;
      if (err == -ENOENT) {
	return -ECANCELED;
      }
      return err;
    }
    args2.odata.min_kdata = idata1.min_kdata;
    args2.odata.max_kdata = idata1.kdata;

    //read the second object
    args1.bound = 2 * k + 1;
    err = read_object(idata2.obj, &args1);
    if (err < 0) {
      if (verbose) cout << "reading " << idata1.obj << " failed with " << err
	  << std::endl;
      return err;
    }
    args1.odata.min_kdata = idata2.min_kdata;
    args1.odata.max_kdata = idata2.kdata;

    if (verbose) cout << "\t\t" << client_name << "-rebalance: read "
	<< idata2.obj
        << ". size: " << args1.odata.size << " version: "
        << args1.odata.version
        << std::endl;
  } else {
    assert (next_idata.obj != "");
    //there is a next key, so get it.
    err = read_object(idata1.obj, &args1);
    if (err < 0) {
      if (verbose) cout << "reading " << idata1.obj << " failed with " << err
	  << std::endl;
      return err;
    }
    args1.odata.min_kdata = idata1.min_kdata;
    args1.odata.max_kdata = idata1.kdata;

    args2.bound = 2 * k + 1;
    err = read_object(idata2.obj, &args2);
    if (err < 0) {
      if (verbose) cout << "reading " << idata1.obj << " failed with " << err
	  << std::endl;
      if (err == -ENOENT) {
	return -ECANCELED;
      }
      return err;
    }
    args2.odata.min_kdata = idata2.min_kdata;
    args2.odata.max_kdata = idata2.kdata;

    if (verbose) cout << "\t\t" << client_name << "-rebalance: read "
	<< idata2.obj
        << ". size: " << args2.odata.size << " version: "
        << args2.odata.version
        << std::endl;
  }

  if (verbose) cout << "\t\t" << client_name << "-rebalance: o1 is "
      << args1.odata.max_kdata.encoded() << ","
      << args1.odata.name  << " with size " << args1.odata.size
      << " , o2 is " << args2.odata.max_kdata.encoded()
      << "," << args2.odata.name  << " with size " << args2.odata.size
      << std::endl;

  //calculations
  if ((int)args1.odata.size > k && (int)args1.odata.size <= 2*k
      && (int)args2.odata.size > k
      && (int)args2.odata.size <= 2*k) {
    //nothing to do
    if (verbose) cout << "\t\t" << client_name
	<< "-rebalance: both sizes in range, so"
	<< " aborting " << std::endl;
    return -EBALANCE;
  } else if (idata1.prefix != "" || idata2.prefix != "") {
    return -EPREFIX;
  }

  //this is the high object. it gets created regardless of rebalance or merge.
  client_index_lock.Lock();
  string o2w = to_string(client_name, client_index++);
  client_index_lock.Unlock();
  index_data idata;
  vector<object_data> to_create;
  vector<object_data> to_delete;
  librados::ObjectWriteOperation create[2];//possibly only 1 will be used
  librados::ObjectWriteOperation other_ops[6];
  vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > ops;
  ops.push_back(make_pair(
      pair<int, string>(ADD_PREFIX, index_name),
      &other_ops[0]));

  if ((int)args1.odata.size + (int)args2.odata.size <= 2*k) {
    //merge
    if (verbose) cout << "\t\t" << client_name << "-rebalance: merging "
	<< args1.odata.name
	<< " and " << args2.odata.name << " to get " << o2w
	<< std::endl;
    map<string, bufferlist> write2_map;
    write2_map.insert(args1.odata.omap.begin(), args1.odata.omap.end());
    write2_map.insert(args2.odata.omap.begin(), args2.odata.omap.end());
    to_create.push_back(object_data(args1.odata.min_kdata,
	args2.odata.max_kdata, o2w, write2_map));
    ops.push_back(make_pair(
	pair<int, string>(MAKE_OBJECT, o2w),
	&create[0]));
    ceph_assert((int)write2_map.size() <= 2*k);
  } else {
    //rebalance
    if (verbose) cout << "\t\t" << client_name << "-rebalance: rebalancing "
	<< args1.odata.name
	<< " and " << args2.odata.name << std::endl;
    map<std::string, bufferlist> write1_map;
    map<std::string, bufferlist> write2_map;
    map<std::string, bufferlist>::iterator it;
    client_index_lock.Lock();
    string o1w = to_string(client_name, client_index++);
    client_index_lock.Unlock();
    int target_size_1 = ceil(((int)args1.odata.size + (int)args2.odata.size)
	/ 2.0);
    if (args1.odata.max_kdata != idata1.kdata) {
      //this should be true if idata1 is the high object
      target_size_1 = floor(((int)args1.odata.size + (int)args2.odata.size)
	  / 2.0);
    }
    for (it = args1.odata.omap.begin();
	it != args1.odata.omap.end() && (int)write1_map.size()
	    < target_size_1;
	++it) {
      write1_map.insert(*it);
    }
    if (it != args1.odata.omap.end()){
      //write1_map is full, so put the rest in write2_map
      write2_map.insert(it, args1.odata.omap.end());
      write2_map.insert(args2.odata.omap.begin(), args2.odata.omap.end());
    } else {
      //args1.odata.omap was small, and write2_map still needs more
      map<std::string, bufferlist>::iterator it2;
      for(it2 = args2.odata.omap.begin();
	  (it2 != args2.odata.omap.end()) && ((int)write1_map.size()
	      < target_size_1);
	  ++it2) {
	write1_map.insert(*it2);
      }
      write2_map.insert(it2, args2.odata.omap.end());
    }
    if (verbose) cout << "\t\t" << client_name
	<< "-rebalance: write1_map has size "
	<< write1_map.size() << ", write2_map.size() is " << write2_map.size()
	<< std::endl;
    //at this point, write1_map and write2_map should have the correct pairs
    to_create.push_back(object_data(args1.odata.min_kdata,
	key_data(write1_map.rbegin()->first),
	o1w,write1_map));
    to_create.push_back(object_data( key_data(write1_map.rbegin()->first),
	args2.odata.max_kdata, o2w, write2_map));
    ops.push_back(make_pair(
	pair<int, string>(MAKE_OBJECT, o1w),
	&create[0]));
    ops.push_back(make_pair(
	pair<int, string>(MAKE_OBJECT, o2w),
	&create[1]));
  }

  to_delete.push_back(object_data(args1.odata.min_kdata,
      args1.odata.max_kdata, args1.odata.name, args1.odata.version));
  to_delete.push_back(object_data(args2.odata.min_kdata,
      args2.odata.max_kdata, args2.odata.name, args2.odata.version));
  for (int i = 1; i < 6; i++) {
    ops.push_back(make_pair(make_pair(0,""), &other_ops[i]));
  }

  index_data out_data;
  set_up_prefix_index(to_create, to_delete, &other_ops[0], &out_data, &err);
  set_up_ops(to_create, to_delete, &ops, out_data, &err);

  //at this point, all operations should be completely set up.
  /////BEGIN CRITICAL SECTION/////
  err = perform_ops("\t\t" + client_name + "-rebalance:", out_data, &ops);
  if (err < 0) {
    return err;
  }
  icache_lock.Lock();
  for (vector<delete_data>::iterator it = out_data.to_delete.begin();
      it != out_data.to_delete.end(); ++it) {
    icache.erase(it->max);
  }
  for (vector<create_data>::iterator it = out_data.to_create.begin();
      it != out_data.to_create.end(); ++it) {
    icache.push(index_data(*it));
  }
  icache_lock.Unlock();
  if (verbose) cout << "\t\t" << client_name << "-rebalance: done rebalancing."
      << std::endl;
  /////END CRITICAL SECTION/////
  return err;
}

int KvFlatBtreeAsync::read_object(const string &obj, object_data * odata) {
  librados::ObjectReadOperation get_obj;
  librados::AioCompletion * obj_aioc = rados.aio_create_completion();
  int err;
  bufferlist unw_bl;
  odata->name = obj;
  get_obj.omap_get_vals2("", LONG_MAX, &odata->omap, nullptr, &err);
  get_obj.getxattr("unwritable", &unw_bl, &err);
  io_ctx.aio_operate(obj, obj_aioc, &get_obj, NULL);
  obj_aioc->wait_for_safe();
  err = obj_aioc->get_return_value();
  if (err < 0){
    //possibly -ENOENT, meaning someone else deleted it.
    obj_aioc->release();
    return err;
  }
  odata->unwritable = string(unw_bl.c_str(), unw_bl.length()) == "1";
  odata->version = obj_aioc->get_version64();
  odata->size = odata->omap.size();
  obj_aioc->release();
  return 0;
}

int KvFlatBtreeAsync::read_object(const string &obj, rebalance_args * args) {
  bufferlist inbl;
  args->encode(inbl);
  bufferlist outbl;
  int err;
  librados::AioCompletion * a = rados.aio_create_completion();
  io_ctx.aio_exec(obj, a, "kvs", "maybe_read_for_balance", inbl, &outbl);
  a->wait_for_safe();
  err = a->get_return_value();
  if (err < 0) {
    if (verbose) cout << "\t\t" << client_name
	<< "-read_object: reading failed with "
	<< err << std::endl;
    a->release();
    return err;
  }
  auto it = outbl.cbegin();
  args->decode(it);
  args->odata.name = obj;
  args->odata.version = a->get_version64();
  a->release();
  return err;
}

void KvFlatBtreeAsync::set_up_prefix_index(
    const vector<object_data> &to_create,
    const vector<object_data> &to_delete,
    librados::ObjectWriteOperation * owo,
    index_data * idata,
    int * err) {
  std::map<std::string, pair<bufferlist, int> > assertions;
  map<string, bufferlist> to_insert;
  idata->prefix = "1";
  idata->ts = ceph_clock_now();
  for(vector<object_data>::const_iterator it = to_create.begin();
      it != to_create.end();
      ++it) {
    create_data c(it->min_kdata, it->max_kdata, it->name);
    idata->to_create.push_back(c);
  }
  for(vector<object_data>::const_iterator it = to_delete.begin();
      it != to_delete.end();
      ++it) {
    delete_data d(it->min_kdata, it->max_kdata, it->name, it->version);
    idata->to_delete.push_back(d);
  }
  for(vector<object_data>::const_iterator it = to_delete.begin();
      it != to_delete.end();
      ++it) {
    idata->obj = it->name;
    idata->min_kdata = it->min_kdata;
    idata->kdata = it->max_kdata;
    bufferlist insert;
    idata->encode(insert);
    to_insert[it->max_kdata.encoded()] = insert;
    index_data this_entry;
    this_entry.min_kdata = idata->min_kdata;
    this_entry.kdata = idata->kdata;
    this_entry.obj = idata->obj;
    assertions[it->max_kdata.encoded()] = pair<bufferlist, int>
    (to_bl(this_entry),	CEPH_OSD_CMPXATTR_OP_EQ);
    if (verbose) cout << "\t\t\t" << client_name
	<< "-setup_prefix: will assert "
	<< this_entry.str() << std::endl;
  }
  ceph_assert(*err == 0);
  owo->omap_cmp(assertions, err);
  if (to_create.size() <= 2) {
    owo->omap_set(to_insert);
  }
}

//some args can be null if there are no corresponding entries in p
void KvFlatBtreeAsync::set_up_ops(
    const vector<object_data> &create_vector,
    const vector<object_data> &delete_vector,
    vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops,
    const index_data &idata,
    int * err) {
  vector<pair<pair<int, string>,
    librados::ObjectWriteOperation* > >::iterator it;

  //skip the prefixing part
  for(it = ops->begin(); it->first.first == ADD_PREFIX; ++it) {}
  map<string, bufferlist> to_insert;
  std::set<string> to_remove;
  map<string, pair<bufferlist, int> > assertions;
  if (create_vector.size() > 0) {
    for (int i = 0; i < (int)idata.to_delete.size(); ++i) {
      it->first = pair<int, string>(UNWRITE_OBJECT, idata.to_delete[i].obj);
      set_up_unwrite_object(delete_vector[i].version, it->second);
      ++it;
    }
  }
  for (int i = 0; i < (int)idata.to_create.size(); ++i) {
    index_data this_entry(idata.to_create[i].max, idata.to_create[i].min,
	idata.to_create[i].obj);
    to_insert[idata.to_create[i].max.encoded()] = to_bl(this_entry);
    if (idata.to_create.size() <= 2) {
      it->first = pair<int, string>(MAKE_OBJECT, idata.to_create[i].obj);
    } else {
      it->first = pair<int, string>(AIO_MAKE_OBJECT, idata.to_create[i].obj);
    }
    set_up_make_object(create_vector[i].omap, it->second);
    ++it;
  }
  for (int i = 0; i < (int)idata.to_delete.size(); ++i) {
    index_data this_entry = idata;
    this_entry.obj = idata.to_delete[i].obj;
    this_entry.min_kdata = idata.to_delete[i].min;
    this_entry.kdata = idata.to_delete[i].max;
    if (verbose) cout << "\t\t\t" << client_name << "-setup_ops: will assert "
	<< this_entry.str() << std::endl;
    assertions[idata.to_delete[i].max.encoded()] = pair<bufferlist, int>(
	to_bl(this_entry), CEPH_OSD_CMPXATTR_OP_EQ);
    to_remove.insert(idata.to_delete[i].max.encoded());
    it->first = pair<int, string>(REMOVE_OBJECT, idata.to_delete[i].obj);
    set_up_delete_object(it->second);
    ++it;
  }
  if ((int)idata.to_create.size() <= 2) {
    it->second->omap_cmp(assertions, err);
  }
  it->second->omap_rm_keys(to_remove);
  it->second->omap_set(to_insert);


  it->first = pair<int, string>(REMOVE_PREFIX, index_name);
}

void KvFlatBtreeAsync::set_up_make_object(
    const map<std::string, bufferlist> &to_set,
    librados::ObjectWriteOperation *owo) {
  bufferlist inbl;
  encode(to_set, inbl);
  owo->exec("kvs", "create_with_omap", inbl);
}

void KvFlatBtreeAsync::set_up_unwrite_object(
    const int &ver, librados::ObjectWriteOperation *owo) {
  if (ver > 0) {
    owo->assert_version(ver);
  }
  owo->cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  owo->setxattr("unwritable", to_bl("1"));
}

void KvFlatBtreeAsync::set_up_restore_object(
    librados::ObjectWriteOperation *owo) {
  owo->cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
  owo->setxattr("unwritable", to_bl("0"));
}

void KvFlatBtreeAsync::set_up_delete_object(
    librados::ObjectWriteOperation *owo) {
  owo->cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
  owo->remove();
}

int KvFlatBtreeAsync::perform_ops(const string &debug_prefix,
    const index_data &idata,
    vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > *ops) {
  int err = 0;
  vector<librados::AioCompletion*> aiocs(idata.to_create.size());
  int count = 0;
  for (vector<pair<pair<int, string>,
      librados::ObjectWriteOperation*> >::iterator it = ops->begin();
      it != ops->end(); ++it) {
    if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 ) {
      return -ESUICIDE;
    }
    switch (it->first.first) {
    case ADD_PREFIX://prefixing
      if (verbose) cout << debug_prefix << " adding prefix" << std::endl;
      err = io_ctx.operate(index_name, it->second);
      if (err < 0) {
        if (verbose) cout << debug_prefix << " prefixing the index failed with "
            << err << std::endl;
        return -EPREFIX;
      }
      if (verbose) cout << debug_prefix << " prefix added." << std::endl;
      break;
    case UNWRITE_OBJECT://marking
      if (verbose) cout << debug_prefix << " marking " << it->first.second
      << std::endl;
      err = io_ctx.operate(it->first.second, it->second);
      if (err < 0) {
	//most likely because it changed, in which case it will be -ERANGE
	if (verbose) cout << debug_prefix << " marking " << it->first.second
	    << "failed with code" << err << std::endl;
	if (it->first.second == (*idata.to_delete.begin()).max.encoded()) {
	  if (cleanup(idata, -EFIRSTOBJ) == -ESUICIDE) {
	    return -ESUICIDE;
	  }
	} else {
	  if (cleanup(idata, -ERANGE) == -ESUICIDE) {
	    return -ESUICIDE;
	  }
	}
	return err;
      }
      if (verbose) cout << debug_prefix << " marked " << it->first.second
	  << std::endl;
      break;
    case MAKE_OBJECT://creating
      if (verbose) cout << debug_prefix << " creating " << it->first.second
	 << std::endl;
      err = io_ctx.operate(it->first.second, it->second);
      if (err < 0) {
	//this can happen if someone else was cleaning up after us.
	if (verbose) cout << debug_prefix << " creating " << it->first.second
	    << " failed"
	    << " with code " << err << std::endl;
	if (err == -EEXIST) {
	  //someone thinks we died, so die
	  if (verbose) cout << client_name << " is suiciding!" << std::endl;
	  return -ESUICIDE;
	} else {
	  ceph_abort();
	}
	return err;
      }
      if (verbose || idata.to_create.size() > 2) {
	cout << debug_prefix << " created object " << it->first.second
	  << std::endl;
      }
      break;
    case AIO_MAKE_OBJECT:
      cout << debug_prefix << " launching asynchronous create "
	  << it->first.second << std::endl;
      aiocs[count] = rados.aio_create_completion();
      io_ctx.aio_operate(it->first.second, aiocs[count], it->second);
      count++;
      if ((int)idata.to_create.size() == count) {
	cout << "starting aiowrite waiting loop" << std::endl;
	  for (count -= 1; count >= 0; count--) {
	    aiocs[count]->wait_for_safe();
	    err = aiocs[count]->get_return_value();
	    if (err < 0) {
	      //this can happen if someone else was cleaning up after us.
	      cerr << debug_prefix << " a create failed"
		  << " with code " << err << std::endl;
	      if (err == -EEXIST) {
		//someone thinks we died, so die
		cerr << client_name << " is suiciding!" << std::endl;
		return -ESUICIDE;
	      } else {
		ceph_abort();
	      }
	      return err;
	    }
	    if (verbose || idata.to_create.size() > 2) {
	      cout << debug_prefix << " completed aio " << aiocs.size() - count
		  << "/" << aiocs.size() << std::endl;
	    }
	  }
      }
      break;
    case REMOVE_OBJECT://deleting
      if (verbose) cout << debug_prefix << " deleting " << it->first.second
      << std::endl;
      err = io_ctx.operate(it->first.second, it->second);
      if (err < 0) {
	//if someone else called cleanup on this prefix first
	if (verbose) cout << debug_prefix << " deleting " << it->first.second
	    << "failed with code" << err << std::endl;
      }
      if (verbose) cout << debug_prefix << " deleted " << it->first.second
	  << std::endl;
      break;
    case REMOVE_PREFIX://rewriting index
      if (verbose) cout << debug_prefix << " updating index " << std::endl;
      err = io_ctx.operate(index_name, it->second);
      if (err < 0) {
        if (verbose) cout << debug_prefix
    	<< " rewriting the index failed with code " << err
        << ". someone else must have thought we died, so dying" << std::endl;
        return -ETIMEDOUT;
      }
      if (verbose) cout << debug_prefix << " updated index." << std::endl;
      break;
    case RESTORE_OBJECT:
      if (verbose) cout << debug_prefix << " restoring " << it->first.second
      << std::endl;
      err = io_ctx.operate(it->first.second, it->second);
      if (err < 0) {
	if (verbose) cout << debug_prefix << "restoring " << it->first.second
	    << " failed"
	    << " with " << err << std::endl;
	return err;
      }
      if (verbose) cout << debug_prefix << " restored " << it->first.second
	  << std::endl;
      break;
    default:
      if (verbose) cout << debug_prefix << " performing unknown op on "
      << it->first.second
	<< std::endl;
      err = io_ctx.operate(index_name, it->second);
      if (err < 0) {
	if (verbose) cout << debug_prefix << " unknown op on "
	    << it->first.second
	    << " failed with " << err << std::endl;
	return err;
      }
      if (verbose) cout << debug_prefix << " unknown op on "
	  << it->first.second
	  << " succeeded." << std::endl;
      break;
    }
  }

  return err;
}

int KvFlatBtreeAsync::cleanup(const index_data &idata, const int &error) {
  if (verbose) cout << "\t\t" << client_name << ": cleaning up after "
      << idata.str()
      << std::endl;
  int err = 0;
  ceph_assert(idata.prefix != "");
  map<std::string,bufferlist> new_index;
  map<std::string, pair<bufferlist, int> > assertions;
  switch (error) {
  case -EFIRSTOBJ: {
    //this happens if the split or rebalance failed to mark the first object,
    //meaning only the index needs to be changed.
    //restore objects that had been marked unwritable.
    for(vector<delete_data >::const_iterator it =
	idata.to_delete.begin();
	it != idata.to_delete.end(); ++it) {
      index_data this_entry;
      this_entry.obj = (*it).obj;
      this_entry.min_kdata = it->min;
      this_entry.kdata = it->max;
      new_index[it->max.encoded()] = to_bl(this_entry);
      this_entry = idata;
      this_entry.obj = it->obj;
      this_entry.min_kdata = it->min;
      this_entry.kdata = it->max;
      if (verbose) cout << "\t\t\t" << client_name
	  << "-cleanup: will assert index contains "
  	<< this_entry.str() << std::endl;
      assertions[it->max.encoded()] =
	  pair<bufferlist, int>(to_bl(this_entry),
	      CEPH_OSD_CMPXATTR_OP_EQ);
    }

    //update the index
    librados::ObjectWriteOperation update_index;
    update_index.omap_cmp(assertions, &err);
    update_index.omap_set(new_index);
    if (verbose) cout << "\t\t\t" << client_name << "-cleanup: updating index"
	<< std::endl;
    if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 ) {
      return -ESUICIDE;
    }
    err = io_ctx.operate(index_name, &update_index);
    if (err < 0) {
      if (verbose) cout << "\t\t\t" << client_name
	  << "-cleanup: rewriting failed with "
	  << err << ". returning -ECANCELED" << std::endl;
      return -ECANCELED;
    }
    if (verbose) cout << "\t\t\t" << client_name
	<< "-cleanup: updated index. cleanup done."
	<< std::endl;
    break;
  }
  case -ERANGE: {
    //this happens if a split or rebalance fails to mark an object. It is a
    //special case of rolling back that does not have to deal with new objects.

    //restore objects that had been marked unwritable.
    vector<delete_data >::const_iterator it;
    for(it = idata.to_delete.begin();
	it != idata.to_delete.end(); ++it) {
      index_data this_entry;
      this_entry.obj = (*it).obj;
      this_entry.min_kdata = it->min;
      this_entry.kdata = it->max;
      new_index[it->max.encoded()] = to_bl(this_entry);
      this_entry = idata;
      this_entry.obj = it->obj;
      this_entry.min_kdata = it->min;
      this_entry.kdata = it->max;
      if (verbose) cout << "\t\t\t" << client_name
	  << "-cleanup: will assert index contains "
  	<< this_entry.str() << std::endl;
      assertions[it->max.encoded()] =
	  pair<bufferlist, int>(to_bl(this_entry),
	      CEPH_OSD_CMPXATTR_OP_EQ);
    }
    it = idata.to_delete.begin();
    librados::ObjectWriteOperation restore;
    set_up_restore_object(&restore);
    if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 ) {
      return -ESUICIDE;
    }
    if (verbose) cout << "\t\t\t" << client_name << "-cleanup: restoring "
	<< it->obj
	<< std::endl;
    err = io_ctx.operate(it->obj, &restore);
    if (err < 0) {
      //i.e., -ECANCELED because the object was already restored by someone
      //else
	if (verbose) cout << "\t\t\t" << client_name << "-cleanup: restoring "
	    << it->obj
	  << " failed with " << err << std::endl;
    } else {
      if (verbose) cout << "\t\t\t" << client_name << "-cleanup: restored "
	  << it->obj
	   << std::endl;
    }

    //update the index
    librados::ObjectWriteOperation update_index;
    update_index.omap_cmp(assertions, &err);
    update_index.omap_set(new_index);
    if (verbose) cout << "\t\t\t" << client_name << "-cleanup: updating index"
	<< std::endl;
    if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 ) {
      return -ESUICIDE;
    }
    err = io_ctx.operate(index_name, &update_index);
    if (err < 0) {
      if (verbose) cout << "\t\t\t" << client_name
	  << "-cleanup: rewriting failed with "
	  << err << ". returning -ECANCELED" << std::endl;
      return -ECANCELED;
    }
    if (verbose) cout << "\t\t\t" << client_name
	<< "-cleanup: updated index. cleanup done."
	<< std::endl;
    break;
  }
  case -ENOENT: {
    if (verbose) cout << "\t\t" << client_name << "-cleanup: rolling forward"
	<< std::endl;
    //all changes were created except for updating the index and possibly
    //deleting the objects. roll forward.
    vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > ops;
    vector<librados::ObjectWriteOperation> owos(idata.to_delete.size() + 1);
    for (int i = 0; i <= (int)idata.to_delete.size(); ++i) {
      ops.push_back(make_pair(pair<int, string>(0, ""), &owos[i]));
    }
    set_up_ops(vector<object_data>(),
	vector<object_data>(), &ops, idata, &err);
    err = perform_ops("\t\t" + client_name + "-cleanup:", idata, &ops);
    if (err < 0) {
      if (err == -ESUICIDE) {
	return -ESUICIDE;
      }
      if (verbose) cout << "\t\t\t" << client_name
	  << "-cleanup: rewriting failed with "
	  << err << ". returning -ECANCELED" << std::endl;
      return -ECANCELED;
    }
    if (verbose) cout << "\t\t\t" << client_name << "-cleanup: updated index"
	<< std::endl;
    break;
  }
  default: {
    //roll back all changes.
    if (verbose) cout << "\t\t" << client_name << "-cleanup: rolling back"
	<< std::endl;
    map<std::string,bufferlist> new_index;
    std::set<string> to_remove;
    map<std::string, pair<bufferlist, int> > assertions;

    //mark the objects to be created. if someone else already has, die.
    for(vector<create_data >::const_reverse_iterator it =
	idata.to_create.rbegin();
	it != idata.to_create.rend(); ++it) {
      librados::ObjectWriteOperation rm;
      set_up_unwrite_object(0, &rm);
      if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 )
      {
	return -ESUICIDE;
      }
      if (verbose) cout << "\t\t\t" << client_name << "-cleanup: marking "
	  << it->obj
	<< std::endl;
      err = io_ctx.operate(it->obj, &rm);
      if (err < 0) {
	if (verbose) cout << "\t\t\t" << client_name << "-cleanup: marking "
	    << it->obj
            << " failed with " << err << std::endl;
      } else {
      if (verbose) cout << "\t\t\t" << client_name << "-cleanup: marked "
	  << it->obj
        << std::endl;
      }
    }

    //restore objects that had been marked unwritable.
    for(vector<delete_data >::const_iterator it =
	idata.to_delete.begin();
	it != idata.to_delete.end(); ++it) {
      index_data this_entry;
      this_entry.obj = (*it).obj;
      this_entry.min_kdata = it->min;
      this_entry.kdata = it->max;
      new_index[it->max.encoded()] = to_bl(this_entry);
      this_entry = idata;
      this_entry.obj = it->obj;
      this_entry.min_kdata = it->min;
      this_entry.kdata = it->max;
      if (verbose) cout << "\t\t\t" << client_name
	  << "-cleanup: will assert index contains "
  	<< this_entry.str() << std::endl;
      assertions[it->max.encoded()] =
	  pair<bufferlist, int>(to_bl(this_entry),
	      CEPH_OSD_CMPXATTR_OP_EQ);
      librados::ObjectWriteOperation restore;
      set_up_restore_object(&restore);
      if (verbose) cout << "\t\t\t" << client_name
	  << "-cleanup: will assert index contains "
	  << this_entry.str() << std::endl;
      if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 )
      {
	return -ESUICIDE;
      }
      if (verbose) cout << "\t\t\t" << client_name << "-cleanup: restoring "
	  << it->obj
          << std::endl;
      err = io_ctx.operate(it->obj, &restore);
      if (err == -ENOENT) {
	//it had gotten far enough to be rolled forward - unmark the objects
	//and roll forward.
	if (verbose) cout << "\t\t\t" << client_name
	    << "-cleanup: roll forward instead"
	    << std::endl;
	for(vector<create_data >::const_iterator cit =
	    idata.to_create.begin();
	    cit != idata.to_create.end(); ++cit) {
	  librados::ObjectWriteOperation res;
	  set_up_restore_object(&res);
	  if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)()
	      == 1 ) {
	    return -ECANCELED;
	  }
	  if (verbose) cout << "\t\t\t" << client_name
	      << "-cleanup: restoring " << cit->obj
	    << std::endl;
	  err = io_ctx.operate(cit->obj, &res);
	  if (err < 0) {
	    if (verbose) cout << "\t\t\t" << client_name
		<< "-cleanup: restoring "
		<< cit->obj << " failed with " << err << std::endl;
	  }
	  if (verbose) cout << "\t\t\t" << client_name << "-cleanup: restored "
	      << cit->obj
	    << std::endl;
	}
	return cleanup(idata, -ENOENT);
      } else if (err < 0) {
	//i.e., -ECANCELED because the object was already restored by someone
	//else
	  if (verbose) cout << "\t\t\t" << client_name
	      << "-cleanup: restoring " << it->obj
	    << " failed with " << err << std::endl;
      } else {
	if (verbose) cout << "\t\t\t" << client_name << "-cleanup: restored "
	    << it->obj
             << std::endl;
      }
    }

    //remove the new objects
    for(vector<create_data >::const_reverse_iterator it =
	idata.to_create.rbegin();
	it != idata.to_create.rend(); ++it) {
      to_remove.insert(it->max.encoded());
      librados::ObjectWriteOperation rm;
      rm.remove();
      if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 )
      {
	return -ESUICIDE;
      }
      if (verbose) cout << "\t\t\t" << client_name << "-cleanup: removing "
	  << it->obj
          << std::endl;
      err = io_ctx.operate(it->obj, &rm);
      if (err < 0) {
	if (verbose) cout << "\t\t\t" << client_name
	    << "-cleanup: failed to remove "
	    << it->obj << std::endl;
      } else {
	if (verbose) cout << "\t\t\t" << client_name << "-cleanup: removed "
	    << it->obj
            << std::endl;
      }
    }

    //update the index
    librados::ObjectWriteOperation update_index;
    update_index.omap_cmp(assertions, &err);
    update_index.omap_rm_keys(to_remove);
    update_index.omap_set(new_index);
    if (verbose) cout << "\t\t\t" << client_name << "-cleanup: updating index"
	<< std::endl;
    if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 ) {
      return -ESUICIDE;
    }
    err = io_ctx.operate(index_name, &update_index);
    if (err < 0) {
      if (verbose) cout << "\t\t\t" << client_name
	  << "-cleanup: rewriting failed with "
	  << err << ". returning -ECANCELED" << std::endl;
      return -ECANCELED;
    }
    if (verbose) cout << "\t\t\t" << client_name
	<< "-cleanup: updated index. cleanup done."
	<< std::endl;
    break;
  }
  }
  return err;
}

string KvFlatBtreeAsync::to_string(string s, int i) {
  stringstream ret;
  ret << s << i;
  return ret.str();
}

string KvFlatBtreeAsync::get_name() {
  return rados_id;
}

void KvFlatBtreeAsync::set_inject(injection_t inject, int wait_time) {
  interrupt = inject;
  wait_ms = wait_time;
}

int KvFlatBtreeAsync::setup(int argc, const char** argv) {
  int r = rados.init(rados_id.c_str());
  if (r < 0) {
    cerr << "error during init" << r << std::endl;
    return r;
  }
  r = rados.conf_parse_argv(argc, argv);
  if (r < 0) {
    cerr << "error during parsing args" << r << std::endl;
    return r;
  }
  r = rados.conf_parse_env(NULL);
  if (r < 0) {
    cerr << "error during parsing env" << r << std::endl;
    return r;
  }
  r = rados.conf_read_file(NULL);
  if (r < 0) {
    cerr << "error during read file: " << r << std::endl;
    return r;
  }
  r = rados.connect();
  if (r < 0) {
    cerr << "error during connect: " << r << std::endl;
    return r;
  }
  r = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (r < 0) {
    cerr << "error creating io ctx: " << r << std::endl;
    rados.shutdown();
    return r;
  }

  librados::ObjectWriteOperation make_index;
  make_index.create(true);
  map<std::string,bufferlist> index_map;
  index_data idata;
  idata.obj = client_name;
  idata.min_kdata.raw_key = "";
  idata.kdata = key_data("");
  index_map["1"] = to_bl(idata);
  make_index.omap_set(index_map);
  r = io_ctx.operate(index_name, &make_index);
  if (r < 0) {
    if (verbose) cout << client_name << ": Making the index failed with code "
	<< r
	<< std::endl;
    return 0;
  }
  if (verbose) cout << client_name << ": created index object" << std::endl;

  librados::ObjectWriteOperation make_max_obj;
  make_max_obj.create(true);
  make_max_obj.setxattr("unwritable", to_bl("0"));
  make_max_obj.setxattr("size", to_bl("0"));
  r = io_ctx.operate(client_name, &make_max_obj);
  if (r < 0) {
    if (verbose) cout << client_name << ": Setting xattr failed with code "
	<< r
	<< std::endl;
  }

  return 0;
}

int KvFlatBtreeAsync::set(const string &key, const bufferlist &val,
    bool update_on_existing) {
  if (verbose) cout << client_name << " is "
      << (update_on_existing? "updating " : "setting ")
      << key << std::endl;
  int err = 0;
  utime_t mytime;
  index_data idata(key);

  if (verbose) cout << "\t" << client_name << ": finding oid" << std::endl;
  err = read_index(key, &idata, NULL, false);
  if (err < 0) {
    if (verbose) cout << "\t" << client_name
	<< ": getting oid failed with code "
	<< err << std::endl;
    return err;
  }
  if (verbose) cout << "\t" << client_name << ": index data is " << idata.str()
      << ", object is " << idata.obj << std::endl;

  err = set_op(key, val, update_on_existing, idata);

  if (verbose) cout << "\t" << client_name << ": finished set with " << err
      << std::endl;
  return err;
}

int KvFlatBtreeAsync::set_op(const string &key, const bufferlist &val,
    bool update_on_existing, index_data &idata) {
  //write

  bufferlist inbl;
  omap_set_args args;
  args.bound = 2 * k;
  args.exclusive = !update_on_existing;
  args.omap[key] = val;
  args.encode(inbl);

  librados::ObjectWriteOperation owo;
  owo.exec("kvs", "omap_insert", inbl);
  if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 ) {
    if (verbose) cout << client_name << " IS SUICIDING!" << std::endl;
    return -ESUICIDE;
  }
  if (verbose) cout << "\t" << client_name << ": inserting " << key
      << " into object "
      << idata.obj << std::endl;
  int err = io_ctx.operate(idata.obj, &owo);
  if (err < 0) {
    switch (err) {
    case -EEXIST: {
      //the key already exists and this is an exclusive insert.
      cerr << "\t" << client_name << ": writing key failed with "
  	<< err << std::endl;
      return err;
    }
    case -EKEYREJECTED: {
      //the object needs to be split.
      do {
	if (verbose) cout << "\t" << client_name << ": running split on "
	    << idata.obj
            << std::endl;
        err = read_index(key, &idata, NULL, true);
        if (err < 0) {
	  if (verbose) cout << "\t" << client_name
	      << ": getting oid failed with code "
	      << err << std::endl;
	  return err;
        }
        err = split(idata);
        if (err < 0 && err != -ENOENT && err != -EBALANCE) {
          if (verbose) cerr << "\t" << client_name << ": split failed with "
              << err << std::endl;
	  int ret = handle_set_rm_errors(err, idata.obj, key, &idata, NULL);
	  switch (ret) {
	  case -ESUICIDE:
	    if (verbose) cout << client_name << " IS SUICIDING!" << std::endl;
	    return ret;
	    break;
	  case 1:
	    return set_op(key, val, update_on_existing, idata);
	    break;
	  case 2:
	    return err;
	    break;
	  }
        }
      } while (err < 0 && err != -EBALANCE && err != -ENOENT);
      err = read_index(key, &idata, NULL, true);
      if (err < 0) {
	if (verbose) cout << "\t" << client_name
	    << ": getting oid failed with code "
	    << err << std::endl;
	return err;
      }
      return set_op(key, val, update_on_existing, idata);
    }
    default:
      if (verbose) cerr << "\t" << client_name << ": writing obj failed with "
  	<< err << std::endl;
      if (err == -ENOENT || err == -EACCES) {
	if (err == -ENOENT) {
	  if (verbose) cout << "CACHE FAILURE" << std::endl;
	}
	err = read_index(key, &idata, NULL, true);
	if (err < 0) {
	  if (verbose) cout << "\t" << client_name
	      << ": getting oid failed with code "
	      << err << std::endl;
	  return err;
	}
	if (verbose) cout << "\t" << client_name << ": index data is "
	    << idata.str()
	    << ", object is " << idata.obj << std::endl;
	return set_op(key, val, update_on_existing, idata);
      } else {
	return err;
      }
    }
  }
  return 0;
}

int KvFlatBtreeAsync::remove(const string &key) {
  if (verbose) cout << client_name << ": removing " << key << std::endl;
  int err = 0;
  string obj;
  utime_t mytime;
  index_data idata;
  index_data next_idata;

  if (verbose) cout << "\t" << client_name << ": finding oid" << std::endl;
  err = read_index(key, &idata, &next_idata, false);
  if (err < 0) {
    if (verbose) cout << "getting oid failed with code " << err << std::endl;
    return err;
  }
  obj = idata.obj;
  if (verbose) cout << "\t" << client_name << ": idata is " << idata.str()
      << ", next_idata is " << next_idata.str()
      << ", obj is " << obj << std::endl;

  err = remove_op(key, idata, next_idata);

  if (verbose) cout << "\t" << client_name << ": finished remove with " << err
      << " and exiting" << std::endl;
  return err;
}

int KvFlatBtreeAsync::remove_op(const string &key, index_data &idata,
    index_data &next_idata) {
  //write
  bufferlist inbl;
  omap_rm_args args;
  args.bound = k;
  args.omap.insert(key);
  args.encode(inbl);

  librados::ObjectWriteOperation owo;
  owo.exec("kvs", "omap_remove", inbl);
  if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 ) {
    if (verbose) cout << client_name << " IS SUICIDING!" << std::endl;
    return -ESUICIDE;
  }
  if (verbose) cout << "\t" << client_name << ": removing " << key << " from "
      << idata.obj
      << std::endl;
  int err = io_ctx.operate(idata.obj, &owo);
  if (err < 0) {
    if (verbose) cout << "\t" << client_name << ": writing obj failed with "
	<< err << std::endl;
    switch (err) {
    case -ENODATA: {
      //the key does not exist in the object
      return err;
    }
    case -EKEYREJECTED: {
      //the object needs to be split.
      do {
        if (verbose) cerr << "\t" << client_name << ": running rebalance on "
            << idata.obj << std::endl;
        err = read_index(key, &idata, &next_idata, true);
        if (err < 0) {
	  if (verbose) cout << "\t" << client_name
	      << ": getting oid failed with code "
	      << err << std::endl;
	  return err;
        }
        err = rebalance(idata, next_idata);
        if (err < 0 && err != -ENOENT && err != -EBALANCE) {
          if (verbose) cerr << "\t" << client_name << ": rebalance returned "
              << err << std::endl;
          int ret = handle_set_rm_errors(err, idata.obj, key, &idata,
              &next_idata);
          switch (ret) {
          case -ESUICIDE:
            if (verbose) cout << client_name << " IS SUICIDING!" << std::endl;
            return err;
            break;
          case 1:
            return remove_op(key, idata, next_idata);
            break;
          case 2:
            return err;
	    break;
          case -EUCLEAN:
            //this is the only node, so it's ok to go below k.
            librados::ObjectWriteOperation owo;
            bufferlist inbl;
            omap_rm_args args;
            args.bound = 0;
            args.omap.insert(key);
            args.encode(inbl);
            owo.exec("kvs", "omap_remove", inbl);
            if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)()
        	== 1 ) {
              if (verbose) cout << client_name << " IS SUICIDING!"
        	  << std::endl;
              return -ESUICIDE;
            }
            if (verbose) cout << "\t" << client_name << ": removing " << key
        	<< " from "
        	<< idata.obj
                << std::endl;
            int err = io_ctx.operate(idata.obj, &owo);
            if (err == 0) {
              return 0;
            }
          }
        }
      } while (err < 0 && err != -EBALANCE && err != -ENOENT);
      err = read_index(key, &idata, &next_idata, true);
      if (err < 0) {
	if (verbose) cout << "\t" << client_name
	    << ": getting oid failed with code "
	    << err << std::endl;
	return err;
      }
      return remove(key);
    }
    default:
      if (err == -ENOENT || err == -EACCES) {
	err = read_index(key, &idata, &next_idata, true);
	if (err < 0) {
	  if (verbose) cout << "\t" << client_name
	      << ": getting oid failed with code "
	      << err << std::endl;
	  return err;
	}
	if (verbose) cout << "\t" << client_name << ": index data is "
	    << idata.str()
	    << ", object is " << idata.obj << std::endl;
	//idea: we read the time every time we read the index anyway - store it.
	return remove_op(key, idata, next_idata);
      } else {
	return err;
      }
    }
  }
  return 0;
}

int KvFlatBtreeAsync::handle_set_rm_errors(int &err, string obj,
    string key,
    index_data * idata, index_data * next_idata) {
  if (err == -ESUICIDE) {
    return err;
  } else if (err == -ECANCELED //if an object was unwritable or index changed
      || err == -EPREFIX //if there is currently a prefix
      || err == -ETIMEDOUT// if the index changes during the op - i.e. cleanup
      || err == -EACCES) //possible if we were acting on old index data
  {
    err = read_index(key, idata, next_idata, true);
    if (err < 0) {
      return err;
    }
    if (verbose) cout << "\t" << client_name << ": prefix is " << idata->str()
	<< std::endl;
    if (idata->obj != obj) {
      //someone else has split or cleaned up or something. start over.
      return 1;//meaning repeat
    }
  } else if (err != -ETIMEDOUT && err != -ERANGE && err != -EACCES
      && err != -EUCLEAN){
    if (verbose) cout << "\t" << client_name
	<< ": split encountered an unexpected error: " << err
	<< std::endl;
    return 2;
  }
  return err;
}

int KvFlatBtreeAsync::get(const string &key, bufferlist *val) {
  opmap['g']++;
  if (verbose) cout << client_name << ": getting " << key << std::endl;
  int err = 0;
  index_data idata;
  utime_t mytime;

  if ((((KeyValueStructure *)this)->*KvFlatBtreeAsync::interrupt)() == 1 ) {
    return -ESUICIDE;
  }
  err = read_index(key, &idata, NULL, false);
  mytime = ceph_clock_now();
  if (err < 0) {
    if (verbose) cout << "getting oid failed with code " << err << std::endl;
    return err;
  }

  err = get_op(key, val, idata);

  if (verbose) cout << client_name << ": got " << key << " with " << err
      << std::endl;

  return err;
}

int KvFlatBtreeAsync::get_op(const string &key, bufferlist *val,
    index_data &idata) {
  int err = 0;
  std::set<std::string> key_set;
  key_set.insert(key);
  map<std::string,bufferlist> omap;
  librados::ObjectReadOperation read;
  read.omap_get_vals_by_keys(key_set, &omap, &err);
  err = io_ctx.operate(idata.obj, &read, NULL);
  if (err < 0) {
    if (err == -ENOENT) {
	err = read_index(key, &idata, NULL, true);
	if (err < 0) {
	  if (verbose) cout << "\t" << client_name
	      << ": getting oid failed with code "
	      << err << std::endl;
	  return err;
	}
	if (verbose) cout << "\t" << client_name << ": index data is "
	    << idata.str()
	    << ", object is " << idata.obj << std::endl;
	return get_op(key, val, idata);
    } else {
      if (verbose) cout << client_name
	  << ": get encountered an unexpected error: " << err
	  << std::endl;
      return err;
    }
  }

  *val = omap[key];
  return err;
}

void *KvFlatBtreeAsync::pset(void *ptr) {
  struct aio_set_args *args = (struct aio_set_args *)ptr;
  *args->err =
      args->kvba->KvFlatBtreeAsync::set((string)args->key,
	  (bufferlist)args->val, (bool)args->exc);
  args->cb(args->err, args->cb_args);
  delete args;
  return NULL;
}

void KvFlatBtreeAsync::aio_set(const string &key, const bufferlist &val,
    bool exclusive, callback cb, void * cb_args, int * err) {
  aio_set_args *args = new aio_set_args();
  args->kvba = this;
  args->key = key;
  args->val = val;
  args->exc = exclusive;
  args->cb = cb;
  args->cb_args = cb_args;
  args->err = err;
  pthread_t t;
  int r = pthread_create(&t, NULL, pset, (void*)args);
  if (r < 0) {
    *args->err = r;
    return;
  }
  pthread_detach(t);
}

void *KvFlatBtreeAsync::prm(void *ptr) {
  struct aio_rm_args *args = (struct aio_rm_args *)ptr;
  *args->err =
      args->kvba->KvFlatBtreeAsync::remove((string)args->key);
  args->cb(args->err, args->cb_args);
  delete args;
  return NULL;
}

void KvFlatBtreeAsync::aio_remove(const string &key,
    callback cb, void * cb_args, int * err) {
  aio_rm_args * args = new aio_rm_args();
  args->kvba = this;
  args->key = key;
  args->cb = cb;
  args->cb_args = cb_args;
  args->err = err;
  pthread_t t;
  int r = pthread_create(&t, NULL, prm, (void*)args);
  if (r < 0) {
    *args->err = r;
    return;
  }
  pthread_detach(t);
}

void *KvFlatBtreeAsync::pget(void *ptr) {
  struct aio_get_args *args = (struct aio_get_args *)ptr;
  *args->err =
      args->kvba->KvFlatBtreeAsync::get((string)args->key,
	  (bufferlist *)args->val);
  args->cb(args->err, args->cb_args);
  delete args;
  return NULL;
}

void KvFlatBtreeAsync::aio_get(const string &key, bufferlist *val,
    callback cb, void * cb_args, int * err) {
  aio_get_args * args = new aio_get_args();
  args->kvba = this;
  args->key = key;
  args->val = val;
  args->cb = cb;
  args->cb_args = cb_args;
  args->err = err;
  pthread_t t;
  int r = pthread_create(&t, NULL, pget, (void*)args);
  if (r < 0) {
    *args->err = r;
    return;
  }
  pthread_detach(t);
}

int KvFlatBtreeAsync::set_many(const map<string, bufferlist> &in_map) {
  int err = 0;
  bufferlist inbl;
  bufferlist outbl;
  std::set<string> keys;

  map<string, bufferlist> big_map;
  for (map<string, bufferlist>::const_iterator it = in_map.begin();
      it != in_map.end(); ++it) {
    keys.insert(it->first);
    big_map.insert(*it);
  }

  if (verbose) cout << "created key set and big_map" << std::endl;

  encode(keys, inbl);
  librados::AioCompletion * aioc = rados.aio_create_completion();
  io_ctx.aio_exec(index_name, aioc,  "kvs", "read_many", inbl, &outbl);
  aioc->wait_for_safe();
  err = aioc->get_return_value();
  aioc->release();
  if (err < 0) {
    cerr << "getting index failed with " << err << std::endl;
    return err;
  }

  map<string, bufferlist> imap;//read from the index
  auto blit = outbl.cbegin();
  decode(imap, blit);

  if (verbose) cout << "finished reading index for objects. there are "
      << imap.size() << " entries that need to be changed. " << std::endl;


  vector<object_data> to_delete;

  vector<object_data> to_create;

  if (verbose) cout << "setting up to_delete and to_create vectors from index "
      << "map" << std::endl;
  //set up to_delete from index map
  for (map<string, bufferlist>::iterator it = imap.begin(); it != imap.end();
      ++it){
    index_data idata;
    blit = it->second.begin();
    idata.decode(blit);
    to_delete.push_back(object_data(idata.min_kdata, idata.kdata, idata.obj));
    err = read_object(idata.obj, &to_delete[to_delete.size() - 1]);
    if (err < 0) {
      if (verbose) cout << "reading " << idata.obj << " failed with " << err
	  << std::endl;
      return set_many(in_map);
    }

    big_map.insert(to_delete[to_delete.size() - 1].omap.begin(),
	to_delete[to_delete.size() - 1].omap.end());
  }

  to_create.push_back(object_data(
	to_string(client_name, client_index++)));
  to_create[0].min_kdata = to_delete[0].min_kdata;

  for(map<string, bufferlist>::iterator it = big_map.begin();
      it != big_map.end(); ++it) {
    if (to_create[to_create.size() - 1].omap.size() == 1.5 * k) {
      to_create[to_create.size() - 1].max_kdata =
	  key_data(to_create[to_create.size() - 1]
	                         .omap.rbegin()->first);

      to_create.push_back(object_data(
	to_string(client_name, client_index++)));
      to_create[to_create.size() - 1].min_kdata =
	  to_create[to_create.size() - 2].max_kdata;
    }

    to_create[to_create.size() - 1].omap.insert(*it);
  }
  to_create[to_create.size() - 1].max_kdata =
      to_delete[to_delete.size() - 1].max_kdata;

  vector<librados::ObjectWriteOperation> owos(2 + 2 * to_delete.size()
					      + to_create.size());
  vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > ops;


  index_data idata;
  set_up_prefix_index(to_create, to_delete, &owos[0], &idata, &err);

  if (verbose) cout << "finished making to_create and to_delete. "
      << std::endl;

  ops.push_back(make_pair(
      pair<int, string>(ADD_PREFIX, index_name),
      &owos[0]));
  for (int i = 1; i < 2 + 2 * (int)to_delete.size() + (int)to_create.size();
      i++) {
    ops.push_back(make_pair(make_pair(0,""), &owos[i]));
  }

  set_up_ops(to_create, to_delete, &ops, idata, &err);

  cout << "finished setting up ops. Starting critical section..." << std::endl;

  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for idata.val
  err = perform_ops("\t\t" + client_name + "-set_many:", idata, &ops);
  if (err < 0) {
    return set_many(in_map);
  }
  if (verbose) cout << "\t\t" << client_name << "-split: done splitting."
      << std::endl;
  /////END CRITICAL SECTION/////
  icache_lock.Lock();
  for (vector<delete_data>::iterator it = idata.to_delete.begin();
      it != idata.to_delete.end(); ++it) {
    icache.erase(it->max);
  }
  for (vector<create_data>::iterator it = idata.to_create.begin();
      it != idata.to_create.end(); ++it) {
    icache.push(index_data(*it));
  }
  icache_lock.Unlock();
  return err;
}

int KvFlatBtreeAsync::remove_all() {
  if (verbose) cout << client_name << ": removing all" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  librados::AioCompletion * oro_aioc = rados.aio_create_completion();
  std::map<std::string, bufferlist> index_set;
  oro.omap_get_vals2("",LONG_MAX,&index_set, nullptr, &err);
  err = io_ctx.aio_operate(index_name, oro_aioc, &oro, NULL);
  if (err < 0){
    if (err == -ENOENT) {
      return 0;
    }
    if (verbose) cout << "getting keys failed with error " << err << std::endl;
    return err;
  }
  oro_aioc->wait_for_safe();
  oro_aioc->release();

  librados::ObjectWriteOperation rm_index;
  librados::AioCompletion * rm_index_aioc = rados.aio_create_completion();
  map<std::string,bufferlist> new_index;
  new_index["1"] = index_set["1"];
  rm_index.omap_clear();
  rm_index.omap_set(new_index);
  io_ctx.aio_operate(index_name, rm_index_aioc, &rm_index);
  err = rm_index_aioc->get_return_value();
  rm_index_aioc->release();
  if (err < 0) {
    if (verbose) cout << "rm index aioc failed with " << err
	<< std::endl;
    return err;
  }

  if (!index_set.empty()) {
    for (std::map<std::string,bufferlist>::iterator it = index_set.begin();
	it != index_set.end(); ++it){
      librados::ObjectWriteOperation sub;
      if (it->first == "1") {
	sub.omap_clear();
      } else {
	sub.remove();
      }
      index_data idata;
      auto b = it->second.cbegin();
      idata.decode(b);
      io_ctx.operate(idata.obj, &sub);
    }
  }

  icache.clear();

  return 0;
}

int KvFlatBtreeAsync::get_all_keys(std::set<std::string> *keys) {
  if (verbose) cout << client_name << ": getting all keys" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<std::string,bufferlist> index_set;
  oro.omap_get_vals2("",LONG_MAX,&index_set, nullptr, &err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    if (verbose) cout << "getting keys failed with error " << err << std::endl;
    return err;
  }
  for (std::map<std::string,bufferlist>::iterator it = index_set.begin();
      it != index_set.end(); ++it){
    librados::ObjectReadOperation sub;
    std::set<std::string> ret;
    sub.omap_get_keys2("",LONG_MAX,&ret, nullptr, &err);
    index_data idata;
    auto b = it->second.cbegin();
    idata.decode(b);
    io_ctx.operate(idata.obj, &sub, NULL);
    keys->insert(ret.begin(), ret.end());
  }
  return err;
}

int KvFlatBtreeAsync::get_all_keys_and_values(
    map<std::string,bufferlist> *kv_map) {
  if (verbose) cout << client_name << ": getting all keys and values"
      << std::endl;
  int err = 0;
  librados::ObjectReadOperation first_read;
  std::set<std::string> index_set;
  first_read.omap_get_keys2("",LONG_MAX,&index_set, nullptr, &err);
  io_ctx.operate(index_name, &first_read, NULL);
  if (err < 0){
    if (verbose) cout << "getting keys failed with error " << err << std::endl;
    return err;
  }
  for (std::set<std::string>::iterator it = index_set.begin();
      it != index_set.end(); ++it){
    librados::ObjectReadOperation sub;
    map<std::string, bufferlist> ret;
    sub.omap_get_vals2("",LONG_MAX,&ret, nullptr, &err);
    io_ctx.operate(*it, &sub, NULL);
    kv_map->insert(ret.begin(), ret.end());
  }
  return err;
}

bool KvFlatBtreeAsync::is_consistent() {
  int err;
  bool ret = true;
  if (verbose) cout << client_name << ": checking consistency" << std::endl;
  std::map<std::string,bufferlist> index;
  map<std::string, std::set<std::string> > sub_objs;
  librados::ObjectReadOperation oro;
  oro.omap_get_vals2("",LONG_MAX,&index, nullptr, &err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    //probably because the index doesn't exist - this might be ok.
    for (librados::NObjectIterator oit = io_ctx.nobjects_begin();
        oit != io_ctx.nobjects_end(); ++oit) {
      //if this executes, there are floating objects.
      cerr << "Not consistent! found floating object " << oit->get_oid()
             << std::endl;
      ret = false;
    }
    return ret;
  }

  std::map<std::string, string> parsed_index;
  std::set<std::string> onames;
  std::set<std::string> special_names;
  for (map<std::string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it) {
    if (it->first != "") {
      index_data idata;
      auto b = it->second.cbegin();
      idata.decode(b);
      if (idata.prefix != "") {
	for(vector<delete_data>::iterator dit = idata.to_delete.begin();
	    dit != idata.to_delete.end(); ++dit) {
	  librados::ObjectReadOperation oro;
	  librados::AioCompletion * aioc = rados.aio_create_completion();
	  bufferlist un;
	  oro.getxattr("unwritable", &un, &err);
	  io_ctx.aio_operate(dit->obj, aioc, &oro, NULL);
	  aioc->wait_for_safe();
	  err = aioc->get_return_value();
	  if (ceph_clock_now() - idata.ts > timeout) {
	    if (err < 0) {
	      aioc->release();
	      if (err == -ENOENT) {
		continue;
	      } else {
		cerr << "Not consistent! reading object " << dit->obj
		<< "returned " << err << std::endl;
		ret = false;
		break;
	      }
	    }
	    if (atoi(string(un.c_str(), un.length()).c_str()) != 1 &&
		aioc->get_version64() != dit->version) {
	      cerr << "Not consistent! object " << dit->obj << " has been "
		  << " modified since the client died was not cleaned up."
		  << std::endl;
	      ret = false;
	    }
	  }
	  special_names.insert(dit->obj);
	  aioc->release();
	}
	for(vector<create_data >::iterator cit = idata.to_create.begin();
	    cit != idata.to_create.end(); ++cit) {
	  special_names.insert(cit->obj);
	}
      }
      parsed_index.insert(make_pair(it->first, idata.obj));
      onames.insert(idata.obj);
    }
  }

  //make sure that an object exists iff it either is the index
  //or is listed in the index
  for (librados::NObjectIterator oit = io_ctx.nobjects_begin();
      oit != io_ctx.nobjects_end(); ++oit) {
    string name = oit->get_oid();
    if (name != index_name && onames.count(name) == 0
	&& special_names.count(name) == 0) {
      cerr << "Not consistent! found floating object " << name << std::endl;
      ret = false;
    }
  }

  //check objects
  string prev = "";
  for (std::map<std::string, string>::iterator it = parsed_index.begin();
      it != parsed_index.end();
      ++it) {
    librados::ObjectReadOperation read;
    read.omap_get_keys2("", LONG_MAX, &sub_objs[it->second], nullptr, &err);
    err = io_ctx.operate(it->second, &read, NULL);
    int size_int = (int)sub_objs[it->second].size();

    //check that size is in the right range
    if (it->first != "1" && special_names.count(it->second) == 0 &&
	err != -ENOENT && (size_int > 2*k|| size_int < k)
	&& parsed_index.size() > 1) {
      cerr << "Not consistent! Object " << *it << " has size " << size_int
	  << ", which is outside the acceptable range." << std::endl;
      ret = false;
    }

    //check that all keys belong in that object
    for(std::set<std::string>::iterator subit = sub_objs[it->second].begin();
	subit != sub_objs[it->second].end(); ++subit) {
      if ((it->first != "1"
	  && *subit > it->first.substr(1,it->first.length()))
	  || *subit <= prev) {
	cerr << "Not consistent! key " << *subit << " does not belong in "
	    << *it << std::endl;
	cerr << "not last element, i.e. " << it->first << " not equal to 1? "
	    << (it->first != "1") << std::endl
	    << "greater than " << it->first.substr(1,it->first.length())
	    <<"? " << (*subit > it->first.substr(1,it->first.length()))
	    << std::endl
	    << "less than or equal to " << prev << "? "
	    << (*subit <= prev) << std::endl;
	ret = false;
      }
    }

    prev = it->first.substr(1,it->first.length());
  }

  if (!ret) {
    if (verbose) cout << "failed consistency test - see error log"
	<< std::endl;
    cerr << str();
  } else {
    if (verbose) cout << "passed consistency test" << std::endl;
  }
  return ret;
}

string KvFlatBtreeAsync::str() {
  stringstream ret;
  ret << "Top-level map:" << std::endl;
  int err = 0;
  std::set<std::string> keys;
  std::map<std::string,bufferlist> index;
  librados::ObjectReadOperation oro;
  librados::AioCompletion * top_aioc = rados.aio_create_completion();
  oro.omap_get_vals2("",LONG_MAX,&index, nullptr, &err);
  io_ctx.aio_operate(index_name, top_aioc, &oro, NULL);
  top_aioc->wait_for_safe();
  err = top_aioc->get_return_value();
  top_aioc->release();
  if (err < 0 && err != -5){
    if (verbose) cout << "getting keys failed with error " << err << std::endl;
    return ret.str();
  }
  if(index.empty()) {
    ret << "There are no objects!" << std::endl;
    return ret.str();
  }

  for (map<std::string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it) {
    keys.insert(string(it->second.c_str(), it->second.length())
	.substr(1,it->second.length()));
  }

  vector<std::string> all_names;
  vector<int> all_sizes(index.size());
  vector<int> all_versions(index.size());
  vector<bufferlist> all_unwrit(index.size());
  vector<map<std::string,bufferlist> > all_maps(keys.size());
  vector<map<std::string,bufferlist>::iterator> its(keys.size());
  unsigned done = 0;
  vector<bool> dones(keys.size());
  ret << std::endl << string(150,'-') << std::endl;

  for (map<std::string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it){
    index_data idata;
    auto b = it->second.cbegin();
    idata.decode(b);
    string s = idata.str();
    ret << "|" << string((148 -
	((*it).first.length()+s.length()+3))/2,' ');
    ret << (*it).first;
    ret << " | ";
    ret << string(idata.str());
    ret << string((148 -
	((*it).first.length()+s.length()+3))/2,' ');
    ret << "|\t";
    all_names.push_back(idata.obj);
    ret << std::endl << string(150,'-') << std::endl;
  }

  int indexer = 0;

  //get the object names and sizes
  for(vector<std::string>::iterator it = all_names.begin(); it
  != all_names.end();
      ++it) {
    librados::ObjectReadOperation oro;
    librados::AioCompletion *aioc = rados.aio_create_completion();
    oro.omap_get_vals2("", LONG_MAX, &all_maps[indexer], nullptr, &err);
    oro.getxattr("unwritable", &all_unwrit[indexer], &err);
    io_ctx.aio_operate(*it, aioc, &oro, NULL);
    aioc->wait_for_safe();
    if (aioc->get_return_value() < 0) {
      ret << "reading" << *it << "failed: " << err << std::endl;
      //return ret.str();
    }
    all_sizes[indexer] = all_maps[indexer].size();
    all_versions[indexer] = aioc->get_version64();
    indexer++;
    aioc->release();
  }

  ret << "///////////////////OBJECT NAMES////////////////" << std::endl;
  //HEADERS
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
   ret << "---------------------------\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "|" << string((25 -
	(string("Bucket: ").length() + all_names[i].length()))/2, ' ');
    ret << "Bucket: " << all_names[i];
    ret << string((25 -
    	(string("Bucket: ").length() + all_names[i].length()))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    its[i] = all_maps[i].begin();
    ret << "|" << string((25 - (string("size: ").length()
	+ to_string("",all_sizes[i]).length()))/2, ' ');
    ret << "size: " << all_sizes[i];
    ret << string((25 - (string("size: ").length()
	  + to_string("",all_sizes[i]).length()))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    its[i] = all_maps[i].begin();
    ret << "|" << string((25 - (string("version: ").length()
	+ to_string("",all_versions[i]).length()))/2, ' ');
    ret << "version: " << all_versions[i];
    ret << string((25 - (string("version: ").length()
	  + to_string("",all_versions[i]).length()))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    its[i] = all_maps[i].begin();
    ret << "|" << string((25 - (string("unwritable? ").length()
	+ 1))/2, ' ');
    ret << "unwritable? " << string(all_unwrit[i].c_str(),
	all_unwrit[i].length());
    ret << string((25 - (string("unwritable? ").length()
	  + 1))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "---------------------------\t";
  }
  ret << std::endl;
  ret << "///////////////////THE ACTUAL BLOCKS////////////////" << std::endl;


  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "---------------------------\t";
  }
  ret << std::endl;
  //each time through this part is two lines
  while(done < keys.size()) {
    for(int i = 0; i < indexer; i++) {
      if(dones[i]){
	ret << "                          \t";
      } else {
	if (its[i] == all_maps[i].end()){
	  done++;
	  dones[i] = true;
	  ret << "                          \t";
	} else {
	  ret << "|" << string((25 -
	      ((*its[i]).first.length()+its[i]->second.length()+3))/2,' ');
	  ret << (*its[i]).first;
	  ret << " | ";
	  ret << string(its[i]->second.c_str(), its[i]->second.length());
	  ret << string((25 -
	      ((*its[i]).first.length()+its[i]->second.length()+3))/2,' ');
	  ret << "|\t";
	  ++(its[i]);
	}

      }
    }
    ret << std::endl;
    for (int i = 0; i < indexer; i++) {
      if(dones[i]){
	ret << "                          \t";
      } else {
	ret << "---------------------------\t";
      }
    }
    ret << std::endl;

  }
  return ret.str();
}
