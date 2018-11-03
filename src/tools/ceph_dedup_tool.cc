// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Myoungwon Oh <ohmyoungwon@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "include/types.h"

#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"

#include "acconfig.h"

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/obj_bencher.h"

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>
#include <stdexcept>
#include <climits>
#include <locale>
#include <memory>

#include "tools/RadosDump.h"
#include "cls/cas/cls_cas_client.h"

using namespace librados;
unsigned default_op_size = 1 << 22;
map< string, pair <uint64_t, uint64_t> > chunk_statistics; // < key, <count, chunk_size> >

void usage()
{
  cout << " usage: [--op <estimate|chunk_scrub|add_chunk_ref|get_chunk_ref>] [--pool <pool_name> ] " << std::endl;
  cout << "   --object <object_name> " << std::endl;
  cout << "   --chunk-size <size> chunk-size (byte) " << std::endl;
  cout << "   --chunk-algorithm <fixed> " << std::endl;
  cout << "   --fingerprint-algorithm <sha1> " << std::endl;
  exit(1);
}

[[noreturn]] static void usage_exit()
{
  usage();
  exit(1);
}

template <typename I, typename T>
static int rados_sistrtoll(I &i, T *val) {
  std::string err;
  *val = strict_iecstrtoll(i->second.c_str(), &err);
  if (err != "") {
    cerr << "Invalid value for " << i->first << ": " << err << std::endl;
    return -EINVAL;
  } else {
    return 0;
  }
}

static void print_dedup_estimate()
{
  uint64_t total_size = 0;
  uint64_t dedup_size = 0;
  for (auto p : chunk_statistics) {
    cout << " -- " << std::endl;
    cout << " key: " << p.first << std::endl;
    cout << " count: " << p.second.first << std::endl;
    cout << " chunk_size: " << p.second.second << std::endl;
    cout << " -- " << std::endl;
    total_size += p.second.second * p.second.first;
    dedup_size += p.second.second;
  }

  cout << " result: " << total_size << " / " << dedup_size << " (total size / dedup size) " << std::endl;
}

int estimate_dedup_ratio(const std::map < std::string, std::string > &opts,
			  std::vector<const char*> &nargs)
{
  Rados rados;
  IoCtx io_ctx;
  std::string object_name;
  std::string chunk_algo;
  string fp_algo;
  string pool_name;
  uint64_t chunk_size = 0;
  unsigned op_size = default_op_size;
  int ret;
  std::map<std::string, std::string>::const_iterator i;

  i = opts.find("pool");
  if (i != opts.end()) {
    pool_name = i->second.c_str();
  }
  i = opts.find("object");
  if (i != opts.end()) {
    object_name = i->second.c_str();
  }
  i = opts.find("chunk-algorithm");
  if (i != opts.end()) {
    chunk_algo = i->second.c_str();
    if (chunk_algo != "fixed") {
      usage_exit();
    }
  } else {
    usage_exit();
  }

  i = opts.find("fingerprint-algorithm");
  if (i != opts.end()) {
    fp_algo = i->second.c_str();
    if (fp_algo != "sha1") {
      usage_exit();
    }
  } else {
    usage_exit();
  }

  i = opts.find("chunk-size");
  if (i != opts.end()) {
    if (rados_sistrtoll(i, &chunk_size)) {
      return -EINVAL;
    }
  } else {
    usage_exit();
  }

  i = opts.find("pgid");
  boost::optional<pg_t> pgid(i != opts.end(), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  if (pool_name.empty()) {
    cerr << "--create-pool requested but pool_name was not specified!" << std::endl;
    usage_exit();
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  {
    try {
      librados::NObjectIterator i = pgid ? io_ctx.nobjects_begin(pgid->ps()) : io_ctx.nobjects_begin();
      librados::NObjectIterator i_end = io_ctx.nobjects_end();
      for (; i != i_end; ++i) {
	uint64_t offset = 0;
	while (true) {
	  bufferlist outdata;
	  ret = io_ctx.read(i->get_oid(), outdata, op_size, offset);
	  if (ret <= 0) {
	    break;
	  }

	  if (chunk_algo == "fixed") {
	    if (fp_algo == "sha1") {
		uint64_t c_offset = 0;
		while (c_offset < outdata.length()) {
		  bufferlist chunk;
		  if (outdata.length() - c_offset > chunk_size) {
		    bufferptr bptr(chunk_size);
		    chunk.push_back(std::move(bptr));
		    chunk.copy_in(0, chunk_size, outdata.c_str());	  
		  } else {
		    bufferptr bptr(outdata.length() - c_offset);
		    chunk.push_back(std::move(bptr));
		    chunk.copy_in(0, outdata.length() - c_offset, outdata.c_str());	  
		  }
		  sha1_digest_t sha1_val = chunk.sha1();
		  string fp = sha1_val.to_str();
		  auto p = chunk_statistics.find(fp);
		  if (p != chunk_statistics.end()) {
		    uint64_t count = p->second.first;
		    count++;
		    chunk_statistics[fp] = make_pair(count, chunk.length());
		  } else {
		    chunk_statistics[fp] = make_pair(1, chunk.length());
		  }
		  c_offset = c_offset + chunk_size;
		}
	    } else {
	      ceph_assert(0 == "no support fingerprint algorithm"); 
	    }
	  } else {
	    ceph_assert(0 == "no support chunk algorithm"); 
	  }
	  
	  if (outdata.length() < op_size) {
	    break;
	  }
	  offset += outdata.length();
	}
      }
    }
    catch (const std::runtime_error& e) {
      cerr << e.what() << std::endl;
      ret = -1;
      goto out;
    }
  }

  print_dedup_estimate();
 out:
  return (ret < 0) ? 1 : 0;
}

int chunk_scrub_common(const std::map < std::string, std::string > &opts,
			  std::vector<const char*> &nargs)
{
  Rados rados;
  IoCtx io_ctx, chunk_io_ctx;
  std::string object_name, target_object_name;
  string pool_name, chunk_pool_name, op_name;
  int ret;
  std::map<std::string, std::string>::const_iterator i;

  i = opts.find("pool");
  if (i != opts.end()) {
    pool_name = i->second.c_str();
  } else {
    usage_exit();
  }
  i = opts.find("op_name");
  if (i != opts.end()) {
    op_name= i->second.c_str();
  } else {
    usage_exit();
  }

  i = opts.find("chunk-pool");
  if (i != opts.end()) {
    chunk_pool_name = i->second.c_str();
  } else {
    usage_exit();
  }
  i = opts.find("pgid");
  boost::optional<pg_t> pgid(i != opts.end(), pg_t());

  ret = rados.init_with_context(g_ceph_context);
  if (ret < 0) {
     cerr << "couldn't initialize rados: " << cpp_strerror(ret) << std::endl;
     goto out;
  }
  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster: " << cpp_strerror(ret) << std::endl;
     ret = -1;
     goto out;
  }
  if (pool_name.empty()) {
    cerr << "--create-pool requested but pool_name was not specified!" << std::endl;
    usage_exit();
  }
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }
  ret = rados.ioctx_create(chunk_pool_name.c_str(), chunk_io_ctx);
  if (ret < 0) {
    cerr << "error opening pool "
	 << chunk_pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    goto out;
  }

  if (op_name == "add_chunk_ref") {
    string target_object_name;
    i = opts.find("object");
    if (i != opts.end()) {
      object_name = i->second.c_str();
    } else {
      usage_exit();
    }
    i = opts.find("target-ref");
    if (i != opts.end()) {
      target_object_name = i->second.c_str();
    } else {
      usage_exit();
    }

    set<hobject_t> refs;
    ret = cls_chunk_refcount_read(chunk_io_ctx, object_name, &refs);
    if (ret < 0) {
      cerr << " cls_chunk_refcount_read fail : " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    for (auto p : refs) {
      cout << " " << p.oid.name << " ";
    }

    uint32_t hash;
    ret = chunk_io_ctx.get_object_hash_position2(object_name, &hash);
    if (ret < 0) {
      return ret;
    }
    hobject_t oid(sobject_t(target_object_name, CEPH_NOSNAP), "", hash, -1, "");
    refs.insert(oid);

    ObjectWriteOperation op;
    cls_chunk_refcount_set(op, refs);
    ret = chunk_io_ctx.operate(object_name, &op);
    if (ret < 0) {
      cerr << " operate fail : " << cpp_strerror(ret) << std::endl;
    }

    return ret;

  } else if (op_name == "get_chunk_ref") {
    i = opts.find("object");
    if (i != opts.end()) {
      object_name = i->second.c_str();
    } else {
      usage_exit();
    }
    cout << __func__ << " " << __LINE__ << std::endl;
    set<hobject_t> refs;
    cout << " refs: " << std::endl;
    ret = cls_chunk_refcount_read(chunk_io_ctx, object_name, &refs);
    for (auto p : refs) {
      cout << " " << p.oid.name << " ";
    }
    cout << std::endl;
    return ret;
  }
  
  {
    try {
      librados::NObjectIterator i = pgid ? chunk_io_ctx.nobjects_begin(pgid->ps()) : chunk_io_ctx.nobjects_begin();
      librados::NObjectIterator i_end = chunk_io_ctx.nobjects_end();
      for (; i != i_end; ++i) {
	set<hobject_t> refs;
	set<hobject_t> real_refs;
	string oid = i->get_oid();
	ret = cls_chunk_refcount_read(chunk_io_ctx, oid, &refs);
	if (ret < 0) {
	  continue;
	}

	for (auto pp : refs) {
	  ret = cls_chunk_has_chunk(io_ctx, pp.oid.name, oid);
	  if (ret != -ENOENT) {
	    real_refs.insert(pp);
	  } 
	}

	if (refs.size() != real_refs.size()) {
	  ObjectWriteOperation op;
	  cls_chunk_refcount_set(op, real_refs);
	  ret = chunk_io_ctx.operate(oid, &op);
	  if (ret < 0) {
	    continue;
	  }
	}
      }
    }
    catch (const std::runtime_error& e) {
      cerr << e.what() << std::endl;
      ret = -1;
      goto out;
    }
  }

out:
  return (ret < 0) ? 1 : 0;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  std::string fn;
  string op_name;

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			     CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  std::map < std::string, std::string > opts;
  std::string val;
  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--op", (char*)NULL)) {
      opts["op_name"] = val;
      op_name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--pool", (char*)NULL)) {
      opts["pool"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--object", (char*)NULL)) {
      opts["object"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-algorithm", (char*)NULL)) {
      opts["chunk-algorithm"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-size", (char*)NULL)) {
      opts["chunk-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--fingerprint-algorithm", (char*)NULL)) {
      opts["fingerprint-algorithm"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--chunk-pool", (char*)NULL)) {
      opts["chunk-pool"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-ref", (char*)NULL)) {
      opts["target-ref"] = val;
    } else {
      if (val[0] == '-')
        usage_exit();
      ++i;
    }
  }

  if (op_name == "estimate") {
    return estimate_dedup_ratio(opts, args);
  } else if (op_name == "chunk_scrub") {
    return chunk_scrub_common(opts, args);
  } else if (op_name == "add_chunk_ref") {
    return chunk_scrub_common(opts, args);
  } else if (op_name == "get_chunk_ref") {
    return chunk_scrub_common(opts, args);
  } else {
    usage();
    exit(0);
  }
  
  return 0;
}
