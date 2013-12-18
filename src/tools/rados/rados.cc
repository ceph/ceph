// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/types.h"

#include "include/rados/librados.hpp"
#include "include/rados/rados_types.hpp"
#include "rados_sync.h"
using namespace librados;

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Cond.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "common/obj_bencher.h"
#include "mds/inode_backtrace.h"
#include "auth/Crypto.h"
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

#include "cls/lock/cls_lock_client.h"
#include "include/compat.h"

int rados_tool_sync(const std::map < std::string, std::string > &opts,
                             std::vector<const char*> &args);

// two steps seem to be necessary to do this right
#define STR(x) _STR(x)
#define _STR(x) #x

void usage(ostream& out)
{
  out <<					\
"usage: rados [options] [commands]\n"
"POOL COMMANDS\n"
"   lspools                          list pools\n"
"   mkpool <pool-name> [123[ 4]]     create pool <pool-name>'\n"
"                                    [with auid 123[and using crush rule 4]]\n"
"   cppool <pool-name> <dest-pool>   copy content of a pool\n"
"   rmpool <pool-name> [<pool-name> --yes-i-really-really-mean-it]\n"
"                                    remove pool <pool-name>'\n"
"   df                               show per-pool and total usage\n"
"   ls                               list objects in pool\n\n"
"   chown 123                        change the pool owner to auid 123\n"
"\n"
"OBJECT COMMANDS\n"
"   get <obj-name> [outfile]         fetch object\n"
"   put <obj-name> [infile]          write object\n"
"   truncate <obj-name> length       truncate object\n"
"   create <obj-name> [category]     create object\n"
"   rm <obj-name> ...                remove object(s)\n"
"   cp <obj-name> [target-obj]       copy object\n"
"   clonedata <src-obj> <dst-obj>    clone object data\n"
"   listxattr <obj-name>\n"
"   getxattr <obj-name> attr\n"
"   setxattr <obj-name> attr val\n"
"   rmxattr <obj-name> attr\n"
"   stat objname                     stat the named object\n"
"   mapext <obj-name>\n"
"   lssnap                           list snaps\n"
"   mksnap <snap-name>               create snap <snap-name>\n"
"   rmsnap <snap-name>               remove snap <snap-name>\n"
"   rollback <obj-name> <snap-name>  roll back object to snap <snap-name>\n"
"\n"
"   listsnaps <obj-name>             list the snapshots of this object\n"
"   bench <seconds> write|seq|rand [-t concurrent_operations] [--no-cleanup]\n"
"                                    default is 16 concurrent IOs and 4 MB ops\n"
"                                    default is to clean up after write benchmark\n"
"   cleanup <prefix>                 clean up a previous benchmark operation\n"
"   load-gen [options]               generate load on the cluster\n"
"   listomapkeys <obj-name>          list the keys in the object map\n"
"   listomapvals <obj-name>          list the keys and vals in the object map \n"
"   getomapval <obj-name> <key>      show the value for the specified key\n"
"                                    in the object's object map\n"
"   setomapval <obj-name> <key> <val>\n"
"   rmomapkey <obj-name> <key>\n"
"   getomapheader <obj-name>\n"
"   setomapheader <obj-name> <val>\n"
"   tmap-to-omap <obj-name>          convert tmap keys/values to omap\n"
"   listwatchers <obj-name>          list the watchers of this object\n"
"\n"
"IMPORT AND EXPORT\n"
"   import [options] <local-directory> <rados-pool>\n"
"       Upload <local-directory> to <rados-pool>\n"
"   export [options] rados-pool> <local-directory>\n"
"       Download <rados-pool> to <local-directory>\n"
"   options:\n"
"       -f / --force                 Copy everything, even if it hasn't changed.\n"
"       -d / --delete-after          After synchronizing, delete unreferenced\n"
"                                    files or objects from the target bucket\n"
"                                    or directory.\n"
"       --workers                    Number of worker threads to spawn \n"
"                                    (default " STR(DEFAULT_NUM_RADOS_WORKER_THREADS) ")\n"
"\n"
"ADVISORY LOCKS\n"
"   lock list <obj-name>\n"
"       List all advisory locks on an object\n"
"   lock get <obj-name> <lock-name>\n"
"       Try to acquire a lock\n"
"   lock break <obj-name> <lock-name> <locker-name>\n"
"       Try to break a lock acquired by another client\n"
"   lock info <obj-name> <lock-name>\n"
"       Show lock information\n"
"   options:\n"
"       --lock-tag                   Lock tag, all locks operation should use\n"
"                                    the same tag\n"
"       --lock-cookie                Locker cookie\n"
"       --lock-description           Description of lock\n"
"       --lock-duration              Lock duration (in seconds)\n"
"       --lock-type                  Lock type (shared, exclusive)\n"
"\n"
"GLOBAL OPTIONS:\n"
"   --object_locator object_locator\n"
"        set object_locator for operation\n"
"   -p pool\n"
"   --pool=pool\n"
"        select given pool by name\n"
"   --target-pool=pool\n"
"        select target pool by name\n"
"   -b op_size\n"
"        set the size of write ops for put or benchmarking\n"
"   -s name\n"
"   --snap name\n"
"        select given snap name for (read) IO\n"
"   -i infile\n"
"   -o outfile\n"
"        specify input or output file (for certain commands)\n"
"   --create\n"
"        create the pool or directory that was specified\n"
"   -N namespace\n"
"   --namespace=namespace\n"
"        specify the namespace to use for the object\n"
"\n"
"BENCH OPTIONS:\n"
"   -t N\n"
"   --concurrent-ios=N\n"
"        Set number of concurrent I/O operations\n"
"   --show-time\n"
"        prefix output with date/time\n"
"\n"
"LOAD GEN OPTIONS:\n"
"   --num-objects                    total number of objects\n"
"   --min-object-size                min object size\n"
"   --max-object-size                max object size\n"
"   --min-ops                        min number of operations\n"
"   --max-ops                        max number of operations\n"
"   --max-backlog                    max backlog (in MB)\n"
"   --percent                        percent of operations that are read\n"
"   --target-throughput              target throughput (in MB)\n"
"   --run-length                     total time (in seconds)\n";

}

static void usage_exit()
{
  usage(cerr);
  exit(1);
}

static int do_get(IoCtx& io_ctx, const char *objname, const char *outfile, unsigned op_size)
{
  string oid(objname);

  int fd;
  if (strcmp(outfile, "-") == 0) {
    fd = 1;
  } else {
    fd = TEMP_FAILURE_RETRY(::open(outfile, O_WRONLY|O_CREAT|O_TRUNC, 0644));
    if (fd < 0) {
      int err = errno;
      cerr << "failed to open file: " << cpp_strerror(err) << std::endl;
      return -err;
    }
  }

  uint64_t offset = 0;
  int ret;
  while (true) {
    bufferlist outdata;
    ret = io_ctx.read(oid, outdata, op_size, offset);
    if (ret <= 0) {
      goto out;
    }
    ret = outdata.write_fd(fd);
    if (ret < 0) {
      cerr << "error writing to file: " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    if (outdata.length() < op_size)
      break;
    offset += outdata.length();
  }
  ret = 0;

 out:
  if (fd != 1)
    TEMP_FAILURE_RETRY(::close(fd));
  return ret;
}

static int do_copy(IoCtx& io_ctx, const char *objname, IoCtx& target_ctx, const char *target_obj)
{
  string oid(objname);
  bufferlist outdata;
  librados::ObjectReadOperation read_op;
  string start_after;

#define COPY_CHUNK_SIZE (4 * 1024 * 1024)
  read_op.read(0, COPY_CHUNK_SIZE, &outdata, NULL);

  map<std::string, bufferlist> attrset;
  read_op.getxattrs(&attrset, NULL);

  bufferlist omap_header;
  read_op.omap_get_header(&omap_header, NULL);

#define OMAP_CHUNK 1000
  map<string, bufferlist> omap;
  read_op.omap_get_vals(start_after, OMAP_CHUNK, &omap, NULL);

  bufferlist opbl;
  int ret = io_ctx.operate(oid, &read_op, &opbl);
  if (ret < 0) {
    return ret;
  }

  librados::ObjectWriteOperation write_op;
  string target_oid(target_obj);

  /* reset dest if exists */
  write_op.create(false);
  write_op.remove();

  write_op.write_full(outdata);
  write_op.omap_set_header(omap_header);

  map<std::string, bufferlist>::iterator iter;
  for (iter = attrset.begin(); iter != attrset.end(); ++iter) {
    write_op.setxattr(iter->first.c_str(), iter->second);
  }
  if (!omap.empty()) {
    write_op.omap_set(omap);
  }
  ret = target_ctx.operate(target_oid, &write_op);
  if (ret < 0) {
    return ret;
  }

  uint64_t off = 0;

  while (outdata.length() == COPY_CHUNK_SIZE) {
    off += outdata.length();
    outdata.clear();
    ret = io_ctx.read(oid, outdata, COPY_CHUNK_SIZE, off); 
    if (ret < 0)
      goto err;

    ret = target_ctx.write(target_oid, outdata, outdata.length(), off);
    if (ret < 0)
      goto err;
  }

  /* iterate through source omap and update target. This is not atomic */
  while (omap.size() == OMAP_CHUNK) {
    /* now start_after should point at the last entry */    
    map<string, bufferlist>::iterator iter = omap.end();
    --iter;
    start_after = iter->first;

    omap.clear();
    ret = io_ctx.omap_get_vals(oid, start_after, OMAP_CHUNK, &omap);
    if (ret < 0)
      goto err;

    if (omap.empty())
      break;

    ret = target_ctx.omap_set(target_oid, omap);
    if (ret < 0)
      goto err;
  }

  return 0;

err:
  target_ctx.remove(target_oid);
  return ret;
}

static int do_clone_data(IoCtx& io_ctx, const char *objname, IoCtx& target_ctx, const char *target_obj)
{
  string oid(objname);

  // get size
  uint64_t size;
  int r = target_ctx.stat(oid, &size, NULL);
  if (r < 0)
    return r;

  librados::ObjectWriteOperation write_op;
  string target_oid(target_obj);

  /* reset data stream only */
  write_op.create(false);
  write_op.truncate(0);
  write_op.clone_range(0, oid, 0, size);
  return target_ctx.operate(target_oid, &write_op);
}

static int do_copy_pool(Rados& rados, const char *src_pool, const char *target_pool)
{
  IoCtx src_ctx, target_ctx;
  int ret = rados.ioctx_create(src_pool, src_ctx);
  if (ret < 0) {
    cerr << "cannot open source pool: " << src_pool << std::endl;
    return ret;
  }
  ret = rados.ioctx_create(target_pool, target_ctx);
  if (ret < 0) {
    cerr << "cannot open target pool: " << target_pool << std::endl;
    return ret;
  }
  librados::ObjectIterator i = src_ctx.objects_begin();
  librados::ObjectIterator i_end = src_ctx.objects_end();
  for (; i != i_end; ++i) {
    string oid = i->first;
    string locator = i->second;
    if (i->second.size())
      cout << src_pool << ":" << oid << "(@" << locator << ")" << " => "
           << target_pool << ":" << oid << "(@" << locator << ")" << std::endl;
    else
      cout << src_pool << ":" << oid << " => "
           << target_pool << ":" << oid << std::endl;


    target_ctx.locator_set_key(locator);
    ret = do_copy(src_ctx, oid.c_str(), target_ctx, oid.c_str());
    if (ret < 0) {
      char buf[64];
      cerr << "error copying object: " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
      return ret;
    }
  }

  return 0;
}

static int do_put(IoCtx& io_ctx, const char *objname, const char *infile, int op_size)
{
  string oid(objname);
  bufferlist indata;
  bool stdio = false;
  if (strcmp(infile, "-") == 0)
    stdio = true;

  int ret;
  int fd = 0;
  if (!stdio)
    fd = open(infile, O_RDONLY);
  if (fd < 0) {
    char buf[80];
    cerr << "error reading input file " << infile << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
    return 1;
  }
  char *buf = new char[op_size];
  int count = op_size;
  uint64_t offset = 0;
  while (count != 0) {
    count = read(fd, buf, op_size);
    if (count < 0) {
      ret = -errno;
      cerr << "error reading input file " << infile << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    if (count == 0) {
      if (!offset) {
	ret = io_ctx.create(oid, true);
	if (ret < 0) {
	  cerr << "WARNING: could not create object: " << oid << std::endl;
	  goto out;
	}
      }
      continue;
    }
    indata.append(buf, count);
    if (offset == 0)
      ret = io_ctx.write_full(oid, indata);
    else
      ret = io_ctx.write(oid, indata, count, offset);
    indata.clear();

    if (ret < 0) {
      goto out;
    }
    offset += count;
  }
  ret = 0;
 out:
  TEMP_FAILURE_RETRY(close(fd));
  delete[] buf;
  return ret;
}

class RadosWatchCtx : public librados::WatchCtx {
  string name;
public:
  RadosWatchCtx(const char *imgname) : name(imgname) {}
  virtual ~RadosWatchCtx() {}
  virtual void notify(uint8_t opcode, uint64_t ver, bufferlist& bl) {
    string s;
    try {
      bufferlist::iterator iter = bl.begin();
      ::decode(s, iter);
    } catch (buffer::error *err) {
      cout << "could not decode bufferlist, buffer length=" << bl.length() << std::endl;
    }
    cout << name << " got notification opcode=" << (int)opcode << " ver=" << ver << " msg='" << s << "'" << std::endl;
  }
};

static const char alphanum_table[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

int gen_rand_alphanumeric(char *dest, int size) /* size should be the required string size + 1 */
{
  int ret = get_random_bytes(dest, size);
  if (ret < 0) {
    cerr << "cannot get random bytes: " << cpp_strerror(-ret) << std::endl;
    return -1;
  }

  int i;
  for (i=0; i<size - 1; i++) {
    int pos = (unsigned)dest[i];
    dest[i] = alphanum_table[pos & 63];
  }
  dest[i] = '\0';

  return 0;
}

struct obj_info {
  string name;
  size_t len;
};

class LoadGen {
  size_t total_sent;
  size_t total_completed;

  IoCtx io_ctx;
  Rados *rados;

  map<int, obj_info> objs;

  utime_t start_time;

  bool going_down;

public:
  int read_percent;
  int num_objs;
  size_t min_obj_len;
  uint64_t max_obj_len;
  size_t min_op_len;
  size_t max_op_len;
  size_t max_ops;
  size_t max_backlog;
  size_t target_throughput;
  int run_length;

  enum {
    OP_READ,
    OP_WRITE,
  };

  struct LoadGenOp {
    int id;
    int type;
    string oid;
    size_t off;
    size_t len;
    bufferlist bl;
    LoadGen *lg;
    librados::AioCompletion *completion;

    LoadGenOp() {}
    LoadGenOp(LoadGen *_lg) : lg(_lg), completion(NULL) {}
  };

  int max_op;

  map<int, LoadGenOp *> pending_ops;

  void gen_op(LoadGenOp *op);
  uint64_t gen_next_op();
  void run_op(LoadGenOp *op);

  uint64_t cur_sent_rate() {
    return total_sent / time_passed();
  }

  uint64_t cur_completed_rate() {
    return total_completed / time_passed();
  }

  uint64_t total_expected() {
    return target_throughput * time_passed();
  }

  float time_passed() {
    utime_t now = ceph_clock_now(g_ceph_context);
    now -= start_time;
    uint64_t ns = now.nsec();
    float total = ns / 1000000000;
    total += now.sec();
    return total;
  }

  Mutex lock;
  Cond cond;

  LoadGen(Rados *_rados) : rados(_rados), going_down(false), lock("LoadGen") {
    read_percent = 80;
    min_obj_len = 1024;
    max_obj_len = 5ull * 1024ull * 1024ull * 1024ull;
    min_op_len = 1024;
    target_throughput = 5 * 1024 * 1024; // B/sec
    max_op_len = 2 * 1024 * 1024;
    max_backlog = target_throughput * 2;
    run_length = 60;

    total_sent = 0;
    total_completed = 0;
    num_objs = 200;
    max_op = 16;
  }
  int bootstrap(const char *pool);
  int run();
  void cleanup();

  void io_cb(completion_t c, LoadGenOp *op) {
    total_completed += op->len;

    Mutex::Locker l(lock);

    double rate = (double)cur_completed_rate() / (1024 * 1024);
    cout.precision(3);
    cout << "op " << op->id << " completed, throughput=" << rate  << "MB/sec" << std::endl;

    map<int, LoadGenOp *>::iterator iter = pending_ops.find(op->id);
    if (iter != pending_ops.end())
      pending_ops.erase(iter);

    if (!going_down)
      op->completion->release();

    delete op;

    cond.Signal();
  }
};

static void _load_gen_cb(completion_t c, void *param)
{
  LoadGen::LoadGenOp *op = (LoadGen::LoadGenOp *)param;
  op->lg->io_cb(c, op);
}

int LoadGen::bootstrap(const char *pool)
{
  char buf[128];
  int i;

  if (!pool) {
    cerr << "ERROR: pool name was not specified" << std::endl;
    return -EINVAL;
  }

  int ret = rados->ioctx_create(pool, io_ctx);
  if (ret < 0) {
    cerr << "error opening pool " << pool << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
    return ret;
  }

  int buf_len = 1;
  bufferptr p = buffer::create(buf_len);
  bufferlist bl;
  memset(p.c_str(), 0, buf_len);
  bl.push_back(p);

  list<librados::AioCompletion *> completions;
  for (i = 0; i < num_objs; i++) {
    obj_info info;
    gen_rand_alphanumeric(buf, 16);
    info.name = "obj-";
    info.name.append(buf);
    info.len = get_random(min_obj_len, max_obj_len);

    // throttle...
    while (completions.size() > max_ops) {
      AioCompletion *c = completions.front();
      c->wait_for_complete();
      ret = c->get_return_value();
      c->release();
      completions.pop_front();
      if (ret < 0) {
	cerr << "aio_write failed" << std::endl;
	return ret;
      }
    }

    librados::AioCompletion *c = rados->aio_create_completion(NULL, NULL, NULL);
    completions.push_back(c);
    // generate object
    ret = io_ctx.aio_write(info.name, c, bl, buf_len, info.len - buf_len);
    if (ret < 0) {
      cerr << "couldn't write obj: " << info.name << " ret=" << ret << std::endl;
      return ret;
    }
    objs[i] = info;
  }

  list<librados::AioCompletion *>::iterator iter;
  for (iter = completions.begin(); iter != completions.end(); ++iter) {
    AioCompletion *c = *iter;
    c->wait_for_complete();
    ret = c->get_return_value();
    c->release();
    if (ret < 0) { // yes, we leak.
      cerr << "aio_write failed" << std::endl;
      return ret;
    }
  }
  return 0;
}

void LoadGen::run_op(LoadGenOp *op)
{
  op->completion = rados->aio_create_completion(op, _load_gen_cb, NULL);

  switch (op->type) {
  case OP_READ:
    io_ctx.aio_read(op->oid, op->completion, &op->bl, op->len, op->off);
    break;
  case OP_WRITE:
    bufferptr p = buffer::create(op->len);
    memset(p.c_str(), 0, op->len);
    op->bl.push_back(p);
    
    io_ctx.aio_write(op->oid, op->completion, op->bl, op->len, op->off);
    break;
  }

  total_sent += op->len;
}

void LoadGen::gen_op(LoadGenOp *op)
{
  int i = get_random(0, objs.size() - 1);
  obj_info& info = objs[i];
  op->oid = info.name;

  size_t len = get_random(min_op_len, max_op_len);
  if (len > info.len)
    len = info.len;
  size_t off = get_random(0, info.len);

  if (off + len > info.len)
    off = info.len - len;

  op->off = off;
  op->len = len;

  i = get_random(1, 100);
  if (i > read_percent)
    op->type = OP_WRITE;
  else
    op->type = OP_READ;

  cout << (op->type == OP_READ ? "READ" : "WRITE") << " : oid=" << op->oid << " off=" << op->off << " len=" << op->len << std::endl;
}

uint64_t LoadGen::gen_next_op()
{
  lock.Lock();

  LoadGenOp *op = new LoadGenOp(this);
  gen_op(op);
  op->id = max_op++;
  pending_ops[op->id] = op;

  lock.Unlock();

  run_op(op);

  return op->len;
}

int LoadGen::run()
{
  start_time = ceph_clock_now(g_ceph_context);
  utime_t end_time = start_time;
  end_time += run_length;
  utime_t stamp_time = start_time;
  uint32_t total_sec = 0;

  while (1) {
    lock.Lock();
    utime_t one_second(1, 0);
    cond.WaitInterval(g_ceph_context, lock, one_second);
    lock.Unlock();
    utime_t now = ceph_clock_now(g_ceph_context);

    if (now > end_time)
      break;

    uint64_t expected = total_expected();  
    lock.Lock();
    uint64_t sent = total_sent;
    uint64_t completed = total_completed;
    lock.Unlock();

    if (now - stamp_time >= utime_t(1, 0)) {
      double rate = (double)cur_completed_rate() / (1024 * 1024);
      ++total_sec;
      cout.precision(3);
      cout << setw(5) << total_sec << ": throughput=" << rate  << "MB/sec" << " pending data=" << sent - completed << std::endl;
      stamp_time = now; 
    }

    while (sent < expected &&
           sent - completed < max_backlog &&
	   pending_ops.size() < max_ops) {
      sent += gen_next_op();
    }
  }

  // get a reference to all pending requests
  vector<librados::AioCompletion *> completions;
  lock.Lock();
  going_down = true;
  map<int, LoadGenOp *>::iterator iter;
  for (iter = pending_ops.begin(); iter != pending_ops.end(); ++iter) {
    LoadGenOp *op = iter->second;
    completions.push_back(op->completion);
  }
  lock.Unlock();

  cout << "waiting for all operations to complete" << std::endl;

  // now wait on all the pending requests
  for (vector<librados::AioCompletion *>::iterator citer = completions.begin(); citer != completions.end(); ++citer) {
    librados::AioCompletion *c = *citer;
    c->wait_for_complete();
    c->release();
  }

  return 0;
}

void LoadGen::cleanup()
{
  cout << "cleaning up objects" << std::endl;
  map<int, obj_info>::iterator iter;
  for (iter = objs.begin(); iter != objs.end(); ++iter) {
    obj_info& info = iter->second;
    int ret = io_ctx.remove(info.name);
    if (ret < 0)
      cerr << "couldn't remove obj: " << info.name << " ret=" << ret << std::endl;
  }
}


class RadosBencher : public ObjBencher {
  librados::AioCompletion **completions;
  librados::Rados& rados;
  librados::IoCtx& io_ctx;
  librados::ObjectIterator oi;
  bool iterator_valid;
protected:
  int completions_init(int concurrentios) {
    completions = new librados::AioCompletion *[concurrentios];
    return 0;
  }
  void completions_done() {
    delete[] completions;
    completions = NULL;
  }
  int create_completion(int slot, void (*cb)(void *, void*), void *arg) {
    completions[slot] = rados.aio_create_completion((void *) arg, 0, cb);

    if (!completions[slot])
      return -EINVAL;

    return 0;
  }
  void release_completion(int slot) {
    completions[slot]->release();
    completions[slot] = 0;
  }

  int aio_read(const std::string& oid, int slot, bufferlist *pbl, size_t len) {
    return io_ctx.aio_read(oid, completions[slot], pbl, len, 0);
  }

  int aio_write(const std::string& oid, int slot, bufferlist& bl, size_t len) {
    return io_ctx.aio_write(oid, completions[slot], bl, len, 0);
  }

  int aio_remove(const std::string& oid, int slot) {
    return io_ctx.aio_remove(oid, completions[slot]);
  }

  int sync_read(const std::string& oid, bufferlist& bl, size_t len) {
    return io_ctx.read(oid, bl, len, 0);
  }
  int sync_write(const std::string& oid, bufferlist& bl, size_t len) {
    return io_ctx.write(oid, bl, len, 0);
  }

  int sync_remove(const std::string& oid) {
    return io_ctx.remove(oid);
  }

  bool completion_is_done(int slot) {
    return completions[slot]->is_safe();
  }

  int completion_wait(int slot) {
    return completions[slot]->wait_for_safe_and_cb();
  }
  int completion_ret(int slot) {
    return completions[slot]->get_return_value();
  }

  bool get_objects(std::list<std::string>* objects, int num) {
    int count = 0;

    if (!iterator_valid) {
      oi = io_ctx.objects_begin();
      iterator_valid = true;
    }

    librados::ObjectIterator ei = io_ctx.objects_end();

    if (oi == ei) {
      iterator_valid = false;
      return false;
    }

    objects->clear();
    for ( ; oi != ei && count < num; ++oi) {
      objects->push_back(oi->first);
      ++count;
    }

    return true;
  }

public:
  RadosBencher(CephContext *cct_, librados::Rados& _r, librados::IoCtx& _i)
    : ObjBencher(cct_), completions(NULL), rados(_r), io_ctx(_i), iterator_valid(false) {}
  ~RadosBencher() { }
};

static int do_lock_cmd(std::vector<const char*> &nargs,
                       const std::map < std::string, std::string > &opts,
                       IoCtx *ioctx,
		       Formatter *formatter)
{
  char buf[128];

  if (nargs.size() < 3)
    usage_exit();

  string cmd(nargs[1]);
  string oid(nargs[2]);

  string lock_tag;
  string lock_cookie;
  string lock_description;
  int lock_duration = 0;
  ClsLockType lock_type = LOCK_EXCLUSIVE;

  map<string, string>::const_iterator i;
  i = opts.find("lock-tag");
  if (i != opts.end()) {
    lock_tag = i->second;
  }
  i = opts.find("lock-cookie");
  if (i != opts.end()) {
    lock_cookie = i->second;
  }
  i = opts.find("lock-description");
  if (i != opts.end()) {
    lock_description = i->second;
  }
  i = opts.find("lock-duration");
  if (i != opts.end()) {
    lock_duration = strtol(i->second.c_str(), NULL, 10);
  }
  i = opts.find("lock-type");
  if (i != opts.end()) {
    const string& type_str = i->second;
    if (type_str.compare("exclusive") == 0) {
      lock_type = LOCK_EXCLUSIVE;
    } else if (type_str.compare("shared") == 0) {
      lock_type = LOCK_SHARED;
    } else {
      cerr << "unknown lock type was specified, aborting" << std::endl;
      return -EINVAL;
    }
  }

  if (cmd.compare("list") == 0) {
    list<string> locks;
    int ret = rados::cls::lock::list_locks(ioctx, oid, &locks);
    if (ret < 0) {
      cerr << "ERROR: rados_list_locks(): " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return ret;
    }

    formatter->open_object_section("object");
    formatter->dump_string("objname", oid);
    formatter->open_array_section("locks");
    list<string>::iterator iter;
    for (iter = locks.begin(); iter != locks.end(); ++iter) {
      formatter->open_object_section("lock");
      formatter->dump_string("name", *iter);
      formatter->close_section();
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
    return 0;
  }

  if (nargs.size() < 4)
    usage_exit();

  string lock_name(nargs[3]);

  if (cmd.compare("info") == 0) {
    map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t> lockers;
    ClsLockType type = LOCK_NONE;
    string tag;
    int ret = rados::cls::lock::get_lock_info(ioctx, oid, lock_name, &lockers, &type, &tag);
    if (ret < 0) {
      cerr << "ERROR: rados_lock_get_lock_info(): " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return ret;
    }

    formatter->open_object_section("lock");
    formatter->dump_string("name", lock_name);
    formatter->dump_string("type", cls_lock_type_str(type));
    formatter->dump_string("tag", tag);
    formatter->open_array_section("lockers");
    map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t>::iterator iter;
    for (iter = lockers.begin(); iter != lockers.end(); ++iter) {
      const rados::cls::lock::locker_id_t& id = iter->first;
      const rados::cls::lock::locker_info_t& info = iter->second;
      formatter->open_object_section("locker");
      formatter->dump_stream("name") << id.locker;
      formatter->dump_string("cookie", id.cookie);
      formatter->dump_string("description", info.description);
      formatter->dump_stream("expiration") << info.expiration;
      formatter->dump_stream("addr") << info.addr;
      formatter->close_section();
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
    
    return ret;
  } else if (cmd.compare("get") == 0) {
    rados::cls::lock::Lock l(lock_name);
    l.set_cookie(lock_cookie);
    l.set_tag(lock_tag);
    l.set_duration(utime_t(lock_duration, 0));
    l.set_description(lock_description);
    int ret;
    switch (lock_type) {
    case LOCK_SHARED:
      ret = l.lock_shared(ioctx, oid);
      break;
    default:
      ret = l.lock_exclusive(ioctx, oid);
    }
    if (ret < 0) {
      cerr << "ERROR: failed locking: " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return ret;
    }

    return ret;
  }

  if (nargs.size() < 5)
    usage_exit();

  if (cmd.compare("break") == 0) {
    string locker(nargs[4]);
    rados::cls::lock::Lock l(lock_name);
    l.set_cookie(lock_cookie);
    l.set_tag(lock_tag);
    entity_name_t name;
    if (!name.parse(locker)) {
      cerr << "ERROR: failed to parse locker name (" << locker << ")" << std::endl;
      return -EINVAL;
    }
    int ret = l.break_lock(ioctx, oid, name);
    if (ret < 0) {
      cerr << "ERROR: failed breaking lock: " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return ret;
    }
  } else {
    usage_exit();
  }

  return 0;
}

/**********************************************

**********************************************/
static int rados_tool_common(const std::map < std::string, std::string > &opts,
                             std::vector<const char*> &nargs)
{
  int ret;
  bool create_pool = false;
  const char *pool_name = NULL;
  const char *target_pool_name = NULL;
  string oloc, target_oloc, nspace;
  int concurrent_ios = 16;
  int op_size = 1 << 22;
  bool cleanup = true;
  const char *snapname = NULL;
  snap_t snapid = CEPH_NOSNAP;
  std::map<std::string, std::string>::const_iterator i;
  std::string category;

  uint64_t min_obj_len = 0;
  uint64_t max_obj_len = 0;
  uint64_t min_op_len = 0;
  uint64_t max_op_len = 0;
  uint64_t max_ops = 0;
  uint64_t max_backlog = 0;
  uint64_t target_throughput = 0;
  int64_t read_percent = -1;
  uint64_t num_objs = 0;
  int run_length = 0;

  bool show_time = false;

  Formatter *formatter = NULL;
  bool pretty_format = false;

  Rados rados;
  IoCtx io_ctx;

  i = opts.find("create");
  if (i != opts.end()) {
    create_pool = true;
  }
  i = opts.find("pool");
  if (i != opts.end()) {
    pool_name = i->second.c_str();
  }
  i = opts.find("target_pool");
  if (i != opts.end()) {
    target_pool_name = i->second.c_str();
  }
  i = opts.find("object_locator");
  if (i != opts.end()) {
    oloc = i->second;
  }
  i = opts.find("target_locator");
  if (i != opts.end()) {
    target_oloc = i->second;
  }
  i = opts.find("category");
  if (i != opts.end()) {
    category = i->second;
  }
  i = opts.find("concurrent-ios");
  if (i != opts.end()) {
    concurrent_ios = strtol(i->second.c_str(), NULL, 10);
  }
  i = opts.find("block-size");
  if (i != opts.end()) {
    op_size = strtol(i->second.c_str(), NULL, 10);
  }
  i = opts.find("snap");
  if (i != opts.end()) {
    snapname = i->second.c_str();
  }
  i = opts.find("snapid");
  if (i != opts.end()) {
    snapid = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("min-object-size");
  if (i != opts.end()) {
    min_obj_len = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("max-object-size");
  if (i != opts.end()) {
    max_obj_len = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("min-op-len");
  if (i != opts.end()) {
    min_op_len = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("max-op-len");
  if (i != opts.end()) {
    max_op_len = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("max-ops");
  if (i != opts.end()) {
    max_ops = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("max-backlog");
  if (i != opts.end()) {
    max_backlog = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("target-throughput");
  if (i != opts.end()) {
    target_throughput = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("read-percent");
  if (i != opts.end()) {
    read_percent = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("num-objects");
  if (i != opts.end()) {
    num_objs = strtoll(i->second.c_str(), NULL, 10);
  }
  i = opts.find("run-length");
  if (i != opts.end()) {
    run_length = strtol(i->second.c_str(), NULL, 10);
  }
  i = opts.find("show-time");
  if (i != opts.end()) {
    show_time = true;
  }
  i = opts.find("no-cleanup");
  if (i != opts.end()) {
    cleanup = false;
  }
  i = opts.find("pretty-format");
  if (i != opts.end()) {
    pretty_format = true;
  }
  i = opts.find("format");
  if (i != opts.end()) {
    const char *format = i->second.c_str();
    if (strcmp(format, "xml") == 0)
      formatter = new XMLFormatter(pretty_format);
    else if (strcmp(format, "json") == 0)
      formatter = new JSONFormatter(pretty_format);
    else {
      cerr << "unrecognized format: " << format << std::endl;
      return -EINVAL;
    }
  }
  i = opts.find("namespace");
  if (i != opts.end()) {
    nspace = i->second;
  }


  // open rados
  ret = rados.init_with_context(g_ceph_context);
  if (ret) {
     cerr << "couldn't initialize rados! error " << ret << std::endl;
     ret = -1;
     goto out;
  }

  ret = rados.connect();
  if (ret) {
     cerr << "couldn't connect to cluster! error " << ret << std::endl;
     ret = -1;
     goto out;
  }
  char buf[80];

  if (create_pool && !pool_name) {
    cerr << "--create-pool requested but pool_name was not specified!" << std::endl;
    usage_exit();
  }

  if (create_pool) {
    ret = rados.pool_create(pool_name, 0, 0);
    if (ret < 0) {
      cerr << "error creating pool " << pool_name << ": "
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }

  // open io context.
  if (pool_name) {
    ret = rados.ioctx_create(pool_name, io_ctx);
    if (ret < 0) {
      cerr << "error opening pool " << pool_name << ": "
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }

  // snapname?
  if (snapname) {
    ret = io_ctx.snap_lookup(snapname, &snapid);
    if (ret < 0) {
      cerr << "error looking up snap '" << snapname << "': " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }
  if (oloc.size()) {
    io_ctx.locator_set_key(oloc);
  }
  if (!nspace.empty()) {
    io_ctx.set_namespace(nspace);
  }
  if (snapid != CEPH_NOSNAP) {
    string name;
    ret = io_ctx.snap_get_name(snapid, &name);
    if (ret < 0) {
      cerr << "snapid " << snapid << " doesn't exist in pool "
	   << io_ctx.get_pool_name() << std::endl;
      goto out;
    }
    io_ctx.snap_set_read(snapid);
    cout << "selected snap " << snapid << " '" << snapname << "'" << std::endl;
  }

  assert(!nargs.empty());

  // list pools?
  if (strcmp(nargs[0], "lspools") == 0) {
    list<string> vec;
    rados.pool_list(vec);
    for (list<string>::iterator i = vec.begin(); i != vec.end(); ++i)
      cout << *i << std::endl;
  }
  else if (strcmp(nargs[0], "df") == 0) {
    // pools
    list<string> vec;

    if (!pool_name)
      rados.pool_list(vec);
    else
      vec.push_back(pool_name);

    map<string, map<string, pool_stat_t> > stats;
    rados.get_pool_stats(vec, category, stats);

    if (!formatter) {
      printf("%-15s %-15s"
	     "%12s %12s %12s %12s "
	     "%12s %12s %12s %12s %12s\n",
	     "pool name",
	     "category",
	     "KB", "objects", "clones", "degraded",
	     "unfound", "rd", "rd KB", "wr", "wr KB");
    } else {
      formatter->open_object_section("stats");
      formatter->open_array_section("pools");
    }
    for (map<string, librados::stats_map>::iterator c = stats.begin(); c != stats.end(); ++c) {
      const char *pool_name = c->first.c_str();
      stats_map& m = c->second;
      if (formatter) {
        formatter->open_object_section("pool");
        int64_t pool_id = rados.pool_lookup(pool_name);
        formatter->dump_string("name", pool_name);
        if (pool_id >= 0)
          formatter->dump_format("id", "%lld", pool_id);
        else
          cerr << "ERROR: lookup_pg_pool_name for name=" << pool_name << " returned " << pool_id << std::endl;
        formatter->open_array_section("categories");
      }
      for (stats_map::iterator i = m.begin(); i != m.end(); ++i) {
        const char *category = (i->first.size() ? i->first.c_str() : "");
        pool_stat_t& s = i->second;
        if (!formatter) {
          if (!*category)
            category = "-";
          printf("%-15s "
                 "%-15s "
	         "%12lld %12lld %12lld %12lld"
	         "%12lld %12lld %12lld %12lld %12lld\n",
	         pool_name,
                 category,
	         (long long)s.num_kb,
	         (long long)s.num_objects,
	         (long long)s.num_object_clones,
	         (long long)s.num_objects_degraded,
	         (long long)s.num_objects_unfound,
	         (long long)s.num_rd, (long long)s.num_rd_kb,
	         (long long)s.num_wr, (long long)s.num_wr_kb);
        } else {
          formatter->open_object_section("category");
          if (category)
            formatter->dump_string("name", category);
          formatter->dump_format("size_bytes", "%lld", s.num_bytes);
          formatter->dump_format("size_kb", "%lld", s.num_kb);
          formatter->dump_format("num_objects", "%lld", s.num_objects);
          formatter->dump_format("num_object_clones", "%lld", s.num_object_clones);
          formatter->dump_format("num_object_copies", "%lld", s.num_object_copies);
          formatter->dump_format("num_objects_missing_on_primary", "%lld", s.num_objects_missing_on_primary);
          formatter->dump_format("num_objects_unfound", "%lld", s.num_objects_unfound);
          formatter->dump_format("num_objects_degraded", "%lld", s.num_objects_degraded);
          formatter->dump_format("read_bytes", "%lld", s.num_rd);
          formatter->dump_format("read_kb", "%lld", s.num_rd_kb);
          formatter->dump_format("write_bytes", "%lld", s.num_wr);
          formatter->dump_format("write_kb", "%lld", s.num_wr_kb);
          formatter->flush(cout);
        }
        if (formatter) {
          formatter->close_section();
        }
      }
      if (formatter) {
        formatter->close_section();
        formatter->close_section();
        formatter->flush(cout);
      }
    }

    // total
    cluster_stat_t tstats;
    rados.cluster_stat(tstats);
    if (!formatter) {
      printf("  total used    %12lld %12lld\n", (long long unsigned)tstats.kb_used,
	     (long long unsigned)tstats.num_objects);
      printf("  total avail   %12lld\n", (long long unsigned)tstats.kb_avail);
      printf("  total space   %12lld\n", (long long unsigned)tstats.kb);
    } else {
      formatter->close_section();
      formatter->dump_format("total_objects", "%lld", (long long unsigned)tstats.num_objects);
      formatter->dump_format("total_used", "%lld", (long long unsigned)tstats.kb_used);
      formatter->dump_format("total_avail", "%lld", (long long unsigned)tstats.kb_avail);
      formatter->dump_format("total_space", "%lld", (long long unsigned)tstats.kb);
      formatter->close_section();
      formatter->flush(cout);
    }
  }

  else if (strcmp(nargs[0], "ls") == 0) {
    if (!pool_name) {
      cerr << "pool name was not specified" << std::endl;
      ret = -1;
      goto out;
    }

    bool stdout = (nargs.size() < 2) || (strcmp(nargs[1], "-") == 0);
    ostream *outstream;
    if(stdout)
      outstream = &cout;
    else
      outstream = new ofstream(nargs[1]);

    {
      try {
	librados::ObjectIterator i = io_ctx.objects_begin();
	librados::ObjectIterator i_end = io_ctx.objects_end();
	for (; i != i_end; ++i) {
	  if (i->second.size())
	    *outstream << i->first << "\t" << i->second << std::endl;
	  else
	    *outstream << i->first << std::endl;
	}
      }
      catch (const std::runtime_error& e) {
	cerr << e.what() << std::endl;
	ret = -1;
	goto out;
      }
    }
    if (!stdout)
      delete outstream;
  }
  else if (strcmp(nargs[0], "chown") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    uint64_t new_auid = strtol(nargs[1], 0, 10);
    ret = io_ctx.set_auid(new_auid);
    if (ret < 0) {
      cerr << "error changing auid on pool " << io_ctx.get_pool_name() << ':'
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
    } else cerr << "changed auid on pool " << io_ctx.get_pool_name()
		<< " to " << new_auid << std::endl;
  }
  else if (strcmp(nargs[0], "mapext") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();
    string oid(nargs[1]);
    std::map<uint64_t,uint64_t> m;
    ret = io_ctx.mapext(oid, 0, -1, m);
    if (ret < 0) {
      cerr << "mapext error on " << pool_name << "/" << oid << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    std::map<uint64_t,uint64_t>::iterator iter;
    for (iter = m.begin(); iter != m.end(); ++iter) {
      cout << hex << iter->first << "\t" << iter->second << dec << std::endl;
    }
  }
  else if (strcmp(nargs[0], "stat") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();
    string oid(nargs[1]);
    uint64_t size;
    time_t mtime;
    ret = io_ctx.stat(oid, &size, &mtime);
    if (ret < 0) {
      cerr << " error stat-ing " << pool_name << "/" << oid << ": "
           << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    } else {
      cout << pool_name << "/" << oid
           << " mtime " << mtime << ", size " << size << std::endl;
    }
  }
  else if (strcmp(nargs[0], "get") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();
    ret = do_get(io_ctx, nargs[1], nargs[2], op_size);
    if (ret < 0) {
      cerr << "error getting " << pool_name << "/" << nargs[1] << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }
  else if (strcmp(nargs[0], "put") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();
    ret = do_put(io_ctx, nargs[1], nargs[2], op_size);
    if (ret < 0) {
      cerr << "error putting " << pool_name << "/" << nargs[1] << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }
  else if (strcmp(nargs[0], "truncate") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    long size = atol(nargs[2]);
    if (size < 0) {
      cerr << "error, cannot truncate to negative value" << std::endl;
      usage_exit();
    }
    ret = io_ctx.trunc(oid, size);
    if (ret < 0) {
      cerr << "error truncating oid "
	   << oid << " to " << size << ": "
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
    } else {
      ret = 0;
    }
  }
  else if (strcmp(nargs[0], "setxattr") == 0) {
    if (!pool_name || nargs.size() < 4)
      usage_exit();

    string oid(nargs[1]);
    string attr_name(nargs[2]);
    string attr_val(nargs[3]);

    bufferlist bl;
    bl.append(attr_val.c_str(), attr_val.length());

    ret = io_ctx.setxattr(oid, attr_name.c_str(), bl);
    if (ret < 0) {
      cerr << "error setting xattr " << pool_name << "/" << oid << "/" << attr_name << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    else
      ret = 0;
  }
  else if (strcmp(nargs[0], "getxattr") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string attr_name(nargs[2]);

    bufferlist bl;
    ret = io_ctx.getxattr(oid, attr_name.c_str(), bl);
    if (ret < 0) {
      cerr << "error getting xattr " << pool_name << "/" << oid << "/" << attr_name << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    else
      ret = 0;
    string s(bl.c_str(), bl.length());
    cout << s << std::endl;
  } else if (strcmp(nargs[0], "rmxattr") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string attr_name(nargs[2]);

    ret = io_ctx.rmxattr(oid, attr_name.c_str());
    if (ret < 0) {
      cerr << "error removing xattr " << pool_name << "/" << oid << "/" << attr_name << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  } else if (strcmp(nargs[0], "listxattr") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);
    map<std::string, bufferlist> attrset;
    bufferlist bl;
    ret = io_ctx.getxattrs(oid, attrset);
    if (ret < 0) {
      cerr << "error getting xattr set " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }

    for (map<std::string, bufferlist>::iterator iter = attrset.begin();
         iter != attrset.end(); ++iter) {
      cout << iter->first << std::endl;
    }
  } else if (strcmp(nargs[0], "getomapheader") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);

    bufferlist header;
    ret = io_ctx.omap_get_header(oid, &header);
    if (ret < 0) {
      cerr << "error getting omap header " << pool_name << "/" << oid
	   << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      cout << "header (" << header.length() << " bytes) :\n";
      header.hexdump(cout);
      cout << std::endl;
      ret = 0;
    }
  } else if (strcmp(nargs[0], "setomapheader") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string val(nargs[2]);

    bufferlist bl;
    bl.append(val);

    ret = io_ctx.omap_set_header(oid, bl);
    if (ret < 0) {
      cerr << "error setting omap value " << pool_name << "/" << oid
	   << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = 0;
    }
  } else if (strcmp(nargs[0], "setomapval") == 0) {
    if (!pool_name || nargs.size() < 4)
      usage_exit();

    string oid(nargs[1]);
    string key(nargs[2]);
    string val(nargs[3]);

    map<string, bufferlist> values;
    bufferlist bl;
    bl.append(val);
    values[key] = bl;

    ret = io_ctx.omap_set(oid, values);
    if (ret < 0) {
      cerr << "error setting omap value " << pool_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = 0;
    }
  } else if (strcmp(nargs[0], "getomapval") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string key(nargs[2]);
    set<string> keys;
    keys.insert(key);

    map<string, bufferlist> values;
    ret = io_ctx.omap_get_vals_by_keys(oid, keys, &values);
    if (ret < 0) {
      cerr << "error getting omap value " << pool_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = 0;
    }

    if (values.size() && values.begin()->first == key) {
      cout << " (length " << values.begin()->second.length() << ") : ";
      values.begin()->second.hexdump(cout);
      cout << std::endl;
    } else {
      cout << "No such key: " << pool_name << "/" << oid << "/" << key
	   << std::endl;
      ret = -1;
      goto out;
    }
  } else if (strcmp(nargs[0], "rmomapkey") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();

    string oid(nargs[1]);
    string key(nargs[2]);
    set<string> keys;
    keys.insert(key);

    ret = io_ctx.omap_rm_keys(oid, keys);
    if (ret < 0) {
      cerr << "error removing omap key " << pool_name << "/" << oid << "/"
	   << key << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    } else {
      ret = 0;
    }
  } else if (strcmp(nargs[0], "listomapvals") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);
    string last_read = "";
    int MAX_READ = 512;
    do {
      map<string, bufferlist> values;
      ret = io_ctx.omap_get_vals(oid, last_read, MAX_READ, &values);
      if (ret < 0) {
	cerr << "error getting omap keys " << pool_name << "/" << oid << ": "
	     << cpp_strerror(ret) << std::endl;
	return 1;
      }
      for (map<string, bufferlist>::const_iterator it = values.begin();
	   it != values.end(); ++it) {
	// dump key in hex if it contains nonprintable characters
	if (std::count_if(it->first.begin(), it->first.end(),
	    (int (*)(int))isprint) < (int)it->first.length()) {
	  cout << "key: (" << it->first.length() << " bytes):\n";
	  bufferlist keybl;
	  keybl.append(it->first);
	  keybl.hexdump(cout);
	} else {
	  cout << it->first;
	}
	cout << std::endl;
	cout << "value: (" << it->second.length() << " bytes) :\n";
	it->second.hexdump(cout);
	cout << std::endl;
      }
    } while (ret == MAX_READ);
    ret = 0;
  }
  else if (strcmp(nargs[0], "cp") == 0) {
    if (!pool_name)
      usage_exit();

    if (nargs.size() < 2 || nargs.size() > 3)
      usage_exit();

    const char *target = target_pool_name;
    if (!target)
      target = pool_name;

    const char *target_obj;
    if (nargs.size() < 3) {
      if (strcmp(target, pool_name) == 0) {
        cerr << "cannot copy object into itself" << std::endl;
	ret = -1;
	goto out;
      }
      target_obj = nargs[1];
    } else {
      target_obj = nargs[2];
    }

    // open io context.
    IoCtx target_ctx;
    ret = rados.ioctx_create(target, target_ctx);
    if (ret < 0) {
      cerr << "error opening target pool " << target << ": "
           << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    if (target_oloc.size()) {
      target_ctx.locator_set_key(target_oloc);
    }

    ret = do_copy(io_ctx, nargs[1], target_ctx, target_obj);
    if (ret < 0) {
      cerr << "error copying " << pool_name << "/" << nargs[1] << " => " << target << "/" << target_obj << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }
  else if (strcmp(nargs[0], "clonedata") == 0) {
    if (!pool_name)
      usage_exit();

    if (nargs.size() < 2 || nargs.size() > 3)
      usage_exit();

    const char *target = target_pool_name;
    if (!target)
      target = pool_name;

    const char *target_obj;
    if (nargs.size() < 3) {
      if (strcmp(target, pool_name) == 0) {
        cerr << "cannot copy object into itself" << std::endl;
        ret = -1;
	goto out;
      }
      target_obj = nargs[1];
    } else {
      target_obj = nargs[2];
    }

    // open io context.
    IoCtx target_ctx;
    ret = rados.ioctx_create(target, target_ctx);
    if (ret < 0) {
      cerr << "error opening target pool " << target << ": "
           << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    if (oloc.size()) {
      target_ctx.locator_set_key(oloc);
    } else {
      cerr << "must specify locator for clone" << std::endl;
      ret = -1;
      goto out;
    }

    ret = do_clone_data(io_ctx, nargs[1], target_ctx, target_obj);
    if (ret < 0) {
      cerr << "error cloning " << pool_name << "/" << nargs[1] << " => " << target << "/" << target_obj << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  } else if (strcmp(nargs[0], "rm") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();
    vector<const char *>::iterator iter = nargs.begin();
    ++iter;
    for (; iter != nargs.end(); ++iter) {
      const string & oid = *iter;
      ret = io_ctx.remove(oid);
      if (ret < 0) {
        cerr << "error removing " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
        goto out;
      }
    }
  }
  else if (strcmp(nargs[0], "create") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();
    string oid(nargs[1]);
    if (nargs.size() > 2) {
      string category(nargs[2]);
      ret = io_ctx.create(oid, true, category);
    } else {
      ret = io_ctx.create(oid, true);
    }
    if (ret < 0) {
      cerr << "error creating " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
  }

  else if (strcmp(nargs[0], "tmap") == 0) {
    if (nargs.size() < 3)
      usage_exit();
    if (strcmp(nargs[1], "dump") == 0) {
      bufferlist outdata;
      string oid(nargs[2]);
      ret = io_ctx.read(oid, outdata, 0, 0);
      if (ret < 0) {
	cerr << "error reading " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
	goto out;
      }
      bufferlist::iterator p = outdata.begin();
      bufferlist header;
      map<string, bufferlist> kv;
      try {
	::decode(header, p);
	::decode(kv, p);
      }
      catch (buffer::error& e) {
	cerr << "error decoding tmap " << pool_name << "/" << oid << std::endl;
	ret = -EINVAL;
	goto out;
      }
      cout << "header (" << header.length() << " bytes):\n";
      header.hexdump(cout);
      cout << "\n";
      cout << kv.size() << " keys\n";
      for (map<string,bufferlist>::iterator q = kv.begin(); q != kv.end(); ++q) {
	cout << "key '" << q->first << "' (" << q->second.length() << " bytes):\n";
	q->second.hexdump(cout);
	cout << "\n";
      }
    }
    else if (strcmp(nargs[1], "set") == 0 ||
	     strcmp(nargs[1], "create") == 0) {
      if (nargs.size() < 5)
	usage_exit();
      string oid(nargs[2]);
      string k(nargs[3]);
      string v(nargs[4]);
      bufferlist bl;
      char c = (strcmp(nargs[1], "set") == 0) ? CEPH_OSD_TMAP_SET : CEPH_OSD_TMAP_CREATE;
      ::encode(c, bl);
      ::encode(k, bl);
      ::encode(v, bl);
      ret = io_ctx.tmap_update(oid, bl);
    }
  }

  else if (strcmp(nargs[0], "tmap-to-omap") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();
    string oid(nargs[1]);

    bufferlist bl;
    int r = io_ctx.tmap_get(oid, bl);
    if (r < 0) {
      ret = r;
      cerr << "error reading tmap " << pool_name << "/" << oid
	   << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    bufferlist hdr;
    map<string, bufferlist> kv;
    bufferlist::iterator p = bl.begin();
    try {
      ::decode(hdr, p);
      ::decode(kv, p);
    }
    catch (buffer::error& e) {
      cerr << "error decoding tmap " << pool_name << "/" << oid << std::endl;
      ret = -EINVAL;
      goto out;
    }
    if (!p.end()) {
      cerr << "error decoding tmap (stray trailing data) in " << pool_name << "/" << oid << std::endl;
      ret = -EINVAL;
      goto out;
    }
    librados::ObjectWriteOperation wr;
    wr.omap_set_header(hdr);
    wr.omap_set(kv);
    wr.truncate(0);  // delete the old tmap data
    r = io_ctx.operate(oid, &wr);
    if (r < 0) {
      ret = r;
      cerr << "error writing tmap data as omap on " << pool_name << "/" << oid
	   << ": " << cpp_strerror(ret) << std::endl;
      goto out;
    }
    ret = 0;
  }

  else if (strcmp(nargs[0], "mkpool") == 0) {
    int auid = 0;
    __u8 crush_rule = 0;
    if (nargs.size() < 2)
      usage_exit();
    if (nargs.size() > 2) {
      auid = strtol(nargs[2], 0, 10);
      cerr << "setting auid:" << auid << std::endl;
      if (nargs.size() > 3) {
	crush_rule = (__u8)strtol(nargs[3], 0, 10);
	cerr << "using crush rule " << (int)crush_rule << std::endl;
      }
    }
    ret = rados.pool_create(nargs[1], auid, crush_rule);
    if (ret < 0) {
      cerr << "error creating pool " << nargs[1] << ": "
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    cout << "successfully created pool " << nargs[1] << std::endl;
  }
  else if (strcmp(nargs[0], "cppool") == 0) {
    if (nargs.size() != 3)
      usage_exit();
    const char *src_pool = nargs[1];
    const char *target_pool = nargs[2];

    if (strcmp(src_pool, target_pool) == 0) {
      cerr << "cannot copy pool into itself" << std::endl;
      ret = -1;
      goto out;
    }

    ret = do_copy_pool(rados, src_pool, target_pool);
    if (ret < 0) {
      cerr << "error copying pool " << src_pool << " => " << target_pool << ": "
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    cout << "successfully copied pool " << nargs[1] << std::endl;
  }
  else if (strcmp(nargs[0], "rmpool") == 0) {
    if (nargs.size() < 2)
      usage_exit();
    if (nargs.size() < 4 ||
	strcmp(nargs[1], nargs[2]) != 0 ||
	strcmp(nargs[3], "--yes-i-really-really-mean-it") != 0) {
      cerr << "WARNING:\n"
	   << "  This will PERMANENTLY DESTROY an entire pool of objects with no way back.\n"
	   << "  To confirm, pass the pool to remove twice, followed by\n"
	   << "  --yes-i-really-really-mean-it" << std::endl;
      ret = -1;
      goto out;
    }
    ret = rados.pool_delete(nargs[1]);
    if (ret >= 0) {
      cout << "successfully deleted pool " << nargs[1] << std::endl;
    } else { //error
      cerr << "pool " << nargs[1] << " does not exist" << std::endl;
    }
  }
  else if (strcmp(nargs[0], "lssnap") == 0) {
    if (!pool_name || nargs.size() != 1)
      usage_exit();

    vector<snap_t> snaps;
    io_ctx.snap_list(&snaps);
    for (vector<snap_t>::iterator i = snaps.begin();
	 i != snaps.end();
	 ++i) {
      string s;
      time_t t;
      if (io_ctx.snap_get_name(*i, &s) < 0)
	continue;
      if (io_ctx.snap_get_stamp(*i, &t) < 0)
	continue;
      struct tm bdt;
      localtime_r(&t, &bdt);
      cout << *i << "\t" << s << "\t";

      cout.setf(std::ios::right);
      cout.fill('0');
      cout << std::setw(4) << (bdt.tm_year+1900)
	   << '.' << std::setw(2) << (bdt.tm_mon+1)
	   << '.' << std::setw(2) << bdt.tm_mday
	   << ' '
	   << std::setw(2) << bdt.tm_hour
	   << ':' << std::setw(2) << bdt.tm_min
	   << ':' << std::setw(2) << bdt.tm_sec
	   << std::endl;
      cout.unsetf(std::ios::right);
    }
    cout << snaps.size() << " snaps" << std::endl;
  }

  else if (strcmp(nargs[0], "mksnap") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    ret = io_ctx.snap_create(nargs[1]);
    if (ret < 0) {
      cerr << "error creating pool " << pool_name << " snapshot " << nargs[1]
	   << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    cout << "created pool " << pool_name << " snap " << nargs[1] << std::endl;
  }

  else if (strcmp(nargs[0], "rmsnap") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    ret = io_ctx.snap_remove(nargs[1]);
    if (ret < 0) {
      cerr << "error removing pool " << pool_name << " snapshot " << nargs[1]
	   << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    cout << "removed pool " << pool_name << " snap " << nargs[1] << std::endl;
  }

  else if (strcmp(nargs[0], "rollback") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();

    ret = io_ctx.rollback(nargs[1], nargs[2]);
    if (ret < 0) {
      cerr << "error rolling back pool " << pool_name << " to snapshot " << nargs[1]
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    cout << "rolled back pool " << pool_name
	 << " to snapshot " << nargs[2] << std::endl;
  }
  else if (strcmp(nargs[0], "bench") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();
    int seconds = atoi(nargs[1]);
    int operation = 0;
    if (strcmp(nargs[2], "write") == 0)
      operation = OP_WRITE;
    else if (strcmp(nargs[2], "seq") == 0)
      operation = OP_SEQ_READ;
    else if (strcmp(nargs[2], "rand") == 0)
      operation = OP_RAND_READ;
    else
      usage_exit();
    RadosBencher bencher(g_ceph_context, rados, io_ctx);
    bencher.set_show_time(show_time);
    ret = bencher.aio_bench(operation, seconds, num_objs,
			    concurrent_ios, op_size, cleanup);
    if (ret != 0)
      cerr << "error during benchmark: " << ret << std::endl;
  }
  else if (strcmp(nargs[0], "cleanup") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();
    const char *prefix = nargs[1];
    RadosBencher bencher(g_ceph_context, rados, io_ctx);
    ret = bencher.clean_up(prefix, concurrent_ios);
    if (ret != 0)
      cerr << "error during cleanup: " << ret << std::endl;
  }
  else if (strcmp(nargs[0], "watch") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();
    string oid(nargs[1]);
    RadosWatchCtx ctx(oid.c_str());
    uint64_t cookie;
    ret = io_ctx.watch(oid, 0, &cookie, &ctx);
    if (ret != 0)
      cerr << "error calling watch: " << ret << std::endl;
    else {
      cout << "press enter to exit..." << std::endl;
      getchar();
    }
  }
  else if (strcmp(nargs[0], "notify") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage_exit();
    string oid(nargs[1]);
    string msg(nargs[2]);
    bufferlist bl;
    ::encode(msg, bl);
    ret = io_ctx.notify(oid, 0, bl);
    if (ret != 0)
      cerr << "error calling notify: " << ret << std::endl;
  } else if (strcmp(nargs[0], "load-gen") == 0) {
    if (!pool_name) {
      cerr << "error: must specify pool" << std::endl;
      usage_exit();
    }
    LoadGen lg(&rados);
    if (min_obj_len)
      lg.min_obj_len = min_obj_len;
    if (max_obj_len)
      lg.max_obj_len = max_obj_len;
    if (min_op_len)
      lg.min_op_len = min_op_len;
    if (max_op_len)
      lg.max_op_len = max_op_len;
    if (max_ops)
      lg.max_ops = max_ops;
    if (max_backlog)
      lg.max_backlog = max_backlog;
    if (target_throughput)
      lg.target_throughput = target_throughput << 20;
    if (read_percent >= 0)
      lg.read_percent = read_percent;
    if (num_objs)
      lg.num_objs = num_objs;
    if (run_length)
      lg.run_length = run_length;

    cout << "run length " << run_length << " seconds" << std::endl;
    cout << "preparing " << lg.num_objs << " objects" << std::endl;
    ret = lg.bootstrap(pool_name);
    if (ret < 0) {
      cerr << "load-gen bootstrap failed" << std::endl;
      exit(1);
    }
    cout << "load-gen will run " << lg.run_length << " seconds" << std::endl;
    lg.run();
    lg.cleanup();
  } else if (strcmp(nargs[0], "listomapkeys") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    librados::ObjectReadOperation read;
    set<string> out_keys;
    read.omap_get_keys("", LONG_MAX, &out_keys, &ret);
    io_ctx.operate(nargs[1], &read, NULL);
    if (ret < 0) {
      cerr << "error getting omap key set " << pool_name << "/"
	   << nargs[1] << ": "  << cpp_strerror(ret) << std::endl;
      goto out;
    }

    for (set<string>::iterator iter = out_keys.begin();
	 iter != out_keys.end(); ++iter) {
      cout << *iter << std::endl;
    }
  } else if (strcmp(nargs[0], "lock") == 0) {
    if (!pool_name)
      usage_exit();

    if (!formatter) {
      formatter = new JSONFormatter(pretty_format);
    }
    ret = do_lock_cmd(nargs, opts, &io_ctx, formatter);
  } else if (strcmp(nargs[0], "listwatchers") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);
    std::list<obj_watch_t> lw;

    ret = io_ctx.list_watchers(oid, &lw);
    if (ret < 0) {
      cerr << "error listing watchers " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    else
      ret = 0;
    
    for (std::list<obj_watch_t>::iterator i = lw.begin(); i != lw.end(); ++i) {
      cout << "watcher=" << i->addr << " client." << i->watcher_id << " cookie=" << i->cookie << std::endl;
    }
  } else if (strcmp(nargs[0], "listsnaps") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage_exit();

    string oid(nargs[1]);
    snap_set_t ls;

    io_ctx.snap_set_read(LIBRADOS_SNAP_DIR);
    ret = io_ctx.list_snaps(oid, &ls);
    if (ret < 0) {
      cerr << "error listing snap shots " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      goto out;
    }
    else
      ret = 0;

    map<snap_t,string> snamemap;
    if (formatter || pretty_format) {
      vector<snap_t> snaps;
      io_ctx.snap_list(&snaps);
      for (vector<snap_t>::iterator i = snaps.begin();
          i != snaps.end(); ++i) {
        string s;
        if (io_ctx.snap_get_name(*i, &s) < 0)
          continue;
        snamemap.insert(pair<snap_t,string>(*i, s));
      }
    }

    if (formatter) {
      formatter->open_object_section("object");
      formatter->dump_string("name", oid);
      formatter->open_array_section("clones");
    } else {
      cout << oid << ":" << std::endl;
      cout << "cloneid	snaps	size	overlap" << std::endl;
    }

    for (std::vector<clone_info_t>::iterator ci = ls.clones.begin();
          ci != ls.clones.end(); ++ci) {

      if (formatter) formatter->open_object_section("clone");

      if (ci->cloneid == librados::SNAP_HEAD) {
        if (formatter)
          formatter->dump_string("id", "head");
        else
          cout << "head";
      } else {
        if (formatter)
          formatter->dump_unsigned("id", ci->cloneid);
        else
          cout << ci->cloneid;
      }

      if (formatter)
        formatter->open_array_section("snapshots");
      else
        cout << "\t";

      if (!formatter && ci->snaps.empty()) {
        cout << "-";
      }
      for (std::vector<snap_t>::const_iterator snapindex = ci->snaps.begin();
          snapindex != ci->snaps.end(); ++snapindex) {

        map<snap_t,string>::iterator si;

        if (formatter || pretty_format) si = snamemap.find(*snapindex);

        if (formatter) {
          formatter->open_object_section("snapshot");
          formatter->dump_unsigned("id", *snapindex);
          if (si != snamemap.end())
            formatter->dump_string("name", si->second);
          formatter->close_section(); //snapshot
        } else {
          if (snapindex != ci->snaps.begin()) cout << ",";
          if (!pretty_format || (si == snamemap.end()))
            cout << *snapindex;
          else
            cout << si->second << "(" << *snapindex << ")";
        }
      }

      if (formatter) {
        formatter->close_section();	//Snapshots
        formatter->dump_unsigned("size", ci->size);
      } else {
        cout << "\t" << ci->size;
      }

      if (ci->cloneid != librados::SNAP_HEAD) {
        if (formatter)
          formatter->open_array_section("overlaps");
        else
          cout << "\t[";

        for (std::vector< std::pair<uint64_t,uint64_t> >::iterator ovi = ci->overlap.begin();
            ovi != ci->overlap.end(); ++ovi) {
          if (formatter) {
            formatter->open_object_section("section");
            formatter->dump_unsigned("start", ovi->first);
            formatter->dump_unsigned("length", ovi->second);
            formatter->close_section(); //section
          } else {
            if (ovi != ci->overlap.begin()) cout << ",";
            cout << ovi->first << "~" << ovi->second;
          }
        }
        if (formatter)
          formatter->close_section(); //overlaps
        else
          cout << "]" << std::endl;
      }
      if (formatter) formatter->close_section(); //clone
    }
    if (formatter) {
      formatter->close_section(); //clones
      formatter->close_section(); //object
      formatter->flush(cout);
    } else {
      cout << std::endl;
    }

  } else {
    cerr << "unrecognized command " << nargs[0] << "; -h or --help for usage" << std::endl;
    ret = -EINVAL;
    goto out;
  }

  if (ret < 0)
    cerr << "error " << (-ret) << ": " << cpp_strerror(ret) << std::endl;

out:
  delete formatter;
  return (ret < 0) ? 1 : 0;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  std::map < std::string, std::string > opts;
  std::vector<const char*>::iterator i;
  std::string val;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage(cout);
      exit(0);
    } else if (ceph_argparse_flag(args, i, "-f", "--force", (char*)NULL)) {
      opts["force"] = "true";
    } else if (ceph_argparse_flag(args, i, "-d", "--delete-after", (char*)NULL)) {
      opts["delete-after"] = "true";
    } else if (ceph_argparse_flag(args, i, "-C", "--create", "--create-pool",
				  (char*)NULL)) {
      opts["create"] = "true";
    } else if (ceph_argparse_flag(args, i, "--pretty-format", (char*)NULL)) {
      opts["pretty-format"] = "true";
    } else if (ceph_argparse_flag(args, i, "--show-time", (char*)NULL)) {
      opts["show-time"] = "true";
    } else if (ceph_argparse_flag(args, i, "--no-cleanup", (char*)NULL)) {
      opts["no-cleanup"] = "true";
    } else if (ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*)NULL)) {
      opts["pool"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-pool", (char*)NULL)) {
      opts["target_pool"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--object-locator" , (char *)NULL)) {
      opts["object_locator"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-locator" , (char *)NULL)) {
      opts["target_locator"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--category", (char*)NULL)) {
      opts["category"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-t", "--concurrent-ios", (char*)NULL)) {
      opts["concurrent-ios"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--block-size", (char*)NULL)) {
      opts["block-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-b", (char*)NULL)) {
      opts["block-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-s", "--snap", (char*)NULL)) {
      opts["snap"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-S", "--snapid", (char*)NULL)) {
      opts["snapid"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--min-object-size", (char*)NULL)) {
      opts["min-object-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-object-size", (char*)NULL)) {
      opts["max-object-size"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--min-op-len", (char*)NULL)) {
      opts["min-op-len"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-op-len", (char*)NULL)) {
      opts["max-op-len"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-ops", (char*)NULL)) {
      opts["max-ops"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--max-backlog", (char*)NULL)) {
      opts["max-backlog"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--target-throughput", (char*)NULL)) {
      opts["target-throughput"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--read-percent", (char*)NULL)) {
      opts["read-percent"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--num-objects", (char*)NULL)) {
      opts["num-objects"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--run-length", (char*)NULL)) {
      opts["run-length"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--workers", (char*)NULL)) {
      opts["workers"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--format", (char*)NULL)) {
      opts["format"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-tag", (char*)NULL)) {
      opts["lock-tag"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-cookie", (char*)NULL)) {
      opts["lock-cookie"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-description", (char*)NULL)) {
      opts["lock-description"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-duration", (char*)NULL)) {
      opts["lock-duration"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--lock-type", (char*)NULL)) {
      opts["lock-type"] = val;
    } else if (ceph_argparse_witharg(args, i, &val, "-N", "--namespace", (char*)NULL)) {
      opts["namespace"] = val;
    } else {
      if (val[0] == '-')
        usage_exit();
      ++i;
    }
  }

  if (args.empty()) {
    cerr << "rados: you must give an action. Try --help" << std::endl;
    return 1;
  }
  if ((strcmp(args[0], "import") == 0) || (strcmp(args[0], "export") == 0))
    return rados_tool_sync(opts, args);
  else
    return rados_tool_common(opts, args);
}
