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
using namespace librados;

#include "osdc/rados_bencher.h"

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/Cond.h"
#include "mds/inode_backtrace.h"
#include <iostream>
#include <fstream>

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <errno.h>
#include <dirent.h>

void usage()
{
  cerr << "usage: rados [options] [commands]" << std::endl;
  /*  cerr << "If no commands are specified, enter interactive mode.\n";
  cerr << "Commands:" << std::endl;
  cerr << "   stop              -- cleanly shut down file system" << std::endl
       << "   (osd|pg|mds) stat -- get monitor subsystem status" << std::endl
       << "   ..." << std::endl;
  */
  cerr << "Commands:\n";
  cerr << "   lspools                         list pools\n";
  cerr << "   df                              show per-pool and total usage\n\n";

  cerr << "Pool commands:\n";
  cerr << "   get <obj-name> [outfile]         fetch object\n";
  cerr << "   put <obj-name> [infile]          write object\n";
  cerr << "   create <obj-name>                create object\n";
  cerr << "   rm <obj-name>                    remove object\n";
  cerr << "   listxattr <obj-name>\n";
  cerr << "   getxattr <obj-name> attr\n";
  cerr << "   setxattr <obj-name> attr val\n";
  cerr << "   rmxattr <obj-name> attr\n";
  cerr << "   stat objname                     stat the named object\n";
  cerr << "   ls                               list objects in pool\n\n";
  cerr << "   chown 123                        change the pool owner to auid 123\n";
  cerr << "   mapext <obj-name>\n";
  cerr << "   mkpool <pool-name> [123[ 4]]     create pool <pool-name>'\n"
       << "                                    [with auid 123[and using crush rule 4]]\n";
  cerr << "   rmpool <pool-name>               remove pool <pool-name>'\n";
  cerr << "   mkpool <pool-name>               create the pool <pool-name>\n";
  cerr << "   lssnap                           list snaps\n";
  cerr << "   mksnap <snap-name>               create snap <snap-name>\n";
  cerr << "   rmsnap <snap-name>               remove snap <snap-name>\n";
  cerr << "   rollback <obj-name> <snap-name>  roll back object to snap <snap-name>\n\n";
  cerr << "   import <dir>                     import pool from a directory\n";
  cerr << "   export <dir>                     export pool into a directory\n";
  cerr << "   bench <seconds> write|seq|rand [-t concurrent_operations]\n";
  cerr << "                                    default is 16 concurrent IOs and 4 MB op size\n\n";

  cerr << "Options:\n";
  cerr << "   -p pool\n";
  cerr << "   --pool=pool\n";
  cerr << "        select given pool by name\n";
  cerr << "   -b op_size\n";
  cerr << "        set the size of write ops for put or benchmarking";
  cerr << "   -s name\n";
  cerr << "   --snap name\n";
  cerr << "        select given snap name for (read) IO\n";
  cerr << "   -i infile\n";
  cerr << "   -o outfile\n";
  cerr << "        specify input or output file (for certain commands)\n";
  cerr << "   --create-pool\n";
  cerr << "        create the pool that was specified\n";
  exit(1);
}

static int do_get(IoCtx& io_ctx, const char *objname, const char *outfile, bool check_stdio)
{
  string oid(objname);
  bufferlist outdata;
  int ret = io_ctx.read(oid, outdata, 0, 0);
  if (ret < 0) {
    return ret;
  }

  if (check_stdio && strcmp(outfile, "-") == 0) {
    fwrite(outdata.c_str(), outdata.length(), 1, stdout);
  } else {
    outdata.write_file(outfile);
    generic_dout(0) << "wrote " << outdata.length() << " byte payload to " << outfile << dendl;
  }

  return 0;
}

static int do_put(IoCtx& io_ctx, const char *objname, const char *infile, int op_size, bool check_stdio)
{
  string oid(objname);
  bufferlist indata;
  bool stdio = false;
  if (check_stdio && strcmp(infile, "-") == 0)
    stdio = true;

  if (stdio) {
    char buf[256];
    while(!cin.eof()) {
      cin.getline(buf, 256);
      indata.append(buf);
      indata.append('\n');
    }
  } else {
    int fd = open(infile, O_RDONLY);
    if (fd < 0) {
      char buf[80];
      cerr << "error reading input file " << infile << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
      return 1;
    }
    char *buf = new char[op_size];
    int count = op_size;
    uint64_t offset = 0;
    while (count == op_size) {
      count = read(fd, buf, op_size);
      if (count == 0) {
        if (!offset) {
          int ret = io_ctx.create(oid, true);
          if (ret < 0)
            cerr << "WARNING: could not create object: " << oid << std::endl;
        }
        continue;
      }
      indata.append(buf, count);
      int ret = io_ctx.write(oid, indata, count, offset);
      indata.clear();

      if (ret < 0) {
        close(fd);
        return ret;
      }
      offset += count;
    }
    close(fd);
  }
  return 0;
}

static int import_dir(IoCtx& io_ctx, string path, const char *name, int prefix_len, int op_size)
{
  string dir_str;
  if (!path.empty())
    dir_str = path + "/";
  dir_str += name;

  DIR *dir = opendir(dir_str.c_str());
  if (!dir) {
    int err = -errno;
    generic_dout(0) << "couldn't open " << dir_str << ": " << strerror(-err) << dendl;
    return -errno;
  }

  struct dirent *dent;
  dent = readdir(dir);
  while (dent) {
    if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
      goto next;
    char buf[dir_str.size() + 1 + strlen(dent->d_name) + 1];
    sprintf(buf, "%s/%s", dir_str.c_str(), dent->d_name);
    generic_dout(0) << buf << dendl;
    struct stat s;
    if (stat(buf, &s) < 0) {
      int err = -errno;
      generic_dout(0) << "WARNING: failed to stat " << buf << ": " << strerror(-err) << dendl;
    }
    if (s.st_mode & S_IFDIR) {
      import_dir(io_ctx, dir_str, dent->d_name, prefix_len, op_size); 
    } else {
      const char *oid = buf + prefix_len + 1; // cut out the the dir name from the object name
      do_put(io_ctx, oid, buf, op_size, false);
    }
next:
    dent = readdir(dir);
  }
  closedir(dir);
  return 0;
}

static int do_import(IoCtx& io_ctx, const char *dir_name, int op_size)
{
  string empty;
  return import_dir(io_ctx, empty, dir_name, strlen(dir_name), op_size);
}

static int do_export(IoCtx& io_ctx, const char *dir_name)
{
  map<string, bool> dircache;
  map<string, bool>::iterator iter;
  string path = dir_name;
  path += "/";

  bool skip;
  librados::ObjectIterator i = io_ctx.objects_begin();
  librados::ObjectIterator i_end = io_ctx.objects_end();
  int r = mkdir(dir_name, 0700);
  if (r < 0) {
    r = -errno;
    if (r != -EEXIST)
      return r;
  }
  for (; i != i_end; ++i) {
    string name = *i;
    int start = 0;
    int pos = name.find('/');
    skip = false;
    while (pos >= 0) {
      string dest = path;
      dest += name.substr(0, pos);
      if (dest.compare("..") == 0) {
        skip = true;
        break;
      }
      iter = dircache.find(dest);
      if (iter == dircache.end()) {
        generic_dout(0) << "mkdir: " << dest << dendl;
        mkdir(dest.c_str(), 0700);
        dircache[dest] = true;
      }
      start = pos + 1;
      if ((size_t)start < name.size())
        pos = name.find('/', start);
      else
        break;
    }
    if (skip)
      continue;
    if ((size_t)start == name.size())
      continue;
    string newfile = path + name;
    generic_dout(0) << "exporting " << name << " => " << newfile << dendl;
    do_get(io_ctx, name.c_str(), newfile.c_str(), false);
  }

  return 0;
}

/**********************************************

**********************************************/

int main(int argc, const char **argv)
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  keyring_init(&g_conf);

  vector<const char*> nargs;
  bufferlist indata, outdata;

  const char *pool_name = 0;

  int concurrent_ios = 16;
  int op_size = 1 << 22;

  const char *snapname = 0;
  snap_t snapid = CEPH_NOSNAP;

  bool create_pool = false;

  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("pool", 'p')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&pool_name, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("snapid", 'S')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&snapid, OPT_LONGLONG);
    } else if (CEPH_ARGPARSE_EQ("snap", 's')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&snapname, OPT_STR);
    } else if (CEPH_ARGPARSE_EQ("help", 'h')) {
      usage();
    } else if (CEPH_ARGPARSE_EQ("concurrent-ios", 't')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&concurrent_ios, OPT_INT);
    } else if (CEPH_ARGPARSE_EQ("block-size", 'b')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&op_size, OPT_INT);
    } else if (CEPH_ARGPARSE_EQ("create-pool", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&create_pool, OPT_BOOL);
    } else if (args[i][0] == '-' && nargs.empty()) {
      cerr << "unrecognized option " << args[i] << std::endl;
      usage();
    } else
      nargs.push_back(args[i]);
  }

  if (nargs.empty())
    usage();

  // open rados
  Rados rados;
  if (rados.init_with_config(&g_conf) < 0) {
     cerr << "couldn't initialize rados!" << std::endl;
     exit(1);
  }

  if (rados.connect() < 0) {
     cerr << "couldn't connect to cluster!" << std::endl;
     exit(1);
  }

  int ret = 0;
  char buf[80];

  if (create_pool && !pool_name) {
    cerr << "--create-pool requested but pool_name was not specified!" << std::endl;
    usage();
  }

  if (create_pool) {
    ret = rados.pool_create(pool_name, 0, 0);
    if (ret < 0) {
      cerr << "error creating pool " << pool_name << ": "
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }

  // open io context.
  IoCtx io_ctx;
  if (pool_name) {
    ret = rados.ioctx_create(pool_name, io_ctx);
    if (ret < 0) {
      cerr << "error opening pool " << pool_name << ": "
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }

  // snapname?
  if (snapname) {
    ret = io_ctx.snap_lookup(snapname, &snapid);
    if (ret < 0) {
      cerr << "error looking up snap '" << snapname << "': " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }
  if (snapid != CEPH_NOSNAP) {
    string name;
    ret = io_ctx.snap_get_name(snapid, &name);
    if (ret < 0) {
      cerr << "snapid " << snapid << " doesn't exist in pool "
	   << io_ctx.get_pool_name() << std::endl;
      return 1;
    }
    io_ctx.snap_set_read(snapid);
    cout << "selected snap " << snapid << " '" << snapname << "'" << std::endl;
  }

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
    rados.pool_list(vec);

    map<string,pool_stat_t> stats;
    rados.get_pool_stats(vec, stats);

    printf("%-15s "
	   "%12s %12s %12s %12s "
	   "%12s %12s %12s %12s %12s\n",
	   "pool name",
	   "KB", "objects", "clones", "degraded",
	   "unfound", "rd", "rd KB", "wr", "wr KB");
    for (map<string,pool_stat_t>::iterator i = stats.begin(); i != stats.end(); ++i) {
      printf("%-15s "
	     "%12lld %12lld %12lld %12lld"
	     "%12lld %12lld %12lld %12lld %12lld\n",
	     i->first.c_str(),
	     (long long)i->second.num_kb,
	     (long long)i->second.num_objects,
	     (long long)i->second.num_object_clones,
	     (long long)i->second.num_objects_degraded,
	     (long long)i->second.num_objects_unfound,
	     (long long)i->second.num_rd, (long long)i->second.num_rd_kb,
	     (long long)i->second.num_wr, (long long)i->second.num_wr_kb);
    }

    // total
    cluster_stat_t tstats;
    rados.cluster_stat(tstats);
    printf("  total used    %12lld %12lld\n", (long long unsigned)tstats.kb_used,
	   (long long unsigned)tstats.num_objects);
    printf("  total avail   %12lld\n", (long long unsigned)tstats.kb_avail);
    printf("  total space   %12lld\n", (long long unsigned)tstats.kb);
  }

  else if (strcmp(nargs[0], "ls") == 0) {
    if (!pool_name) {
      cerr << "pool name was not specified" << std::endl;
      return 1;
    }

    bool stdout = (nargs.size() < 2) || (strcmp(nargs[1], "-") == 0);
    ostream *outstream;
    if(stdout)
      outstream = &cout;
    else
      outstream = new ofstream(nargs[1]);

    {
      librados::ObjectIterator i = io_ctx.objects_begin();
      librados::ObjectIterator i_end = io_ctx.objects_end();
      for (; i != i_end; ++i) {
	*outstream << *i << std::endl;
      }
    }
    if (!stdout)
      delete outstream;
  }
  else if (strcmp(nargs[0], "chown") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage();

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
      usage();
    string oid(nargs[1]);
    std::map<uint64_t,uint64_t> m;
    ret = io_ctx.mapext(oid, 0, -1, m);
    if (ret < 0) {
      cerr << "mapext error on " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
    std::map<uint64_t,uint64_t>::iterator iter;
    for (iter = m.begin(); iter != m.end(); ++iter) {
      cout << hex << iter->first << "\t" << iter->second << dec << std::endl;
    }
  }
  else if (strcmp(nargs[0], "stat") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage();
    string oid(nargs[1]);
    uint64_t size;
    time_t mtime;
    ret = io_ctx.stat(oid, &size, &mtime);
    if (ret < 0) {
      cerr << " error stat-ing " << pool_name << "/" << oid << ": "
           << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    } else {
      cout << pool_name << "/" << oid
           << " mtime " << mtime << ", size " << size << std::endl;
    }
  }
  else if (strcmp(nargs[0], "get") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage();
    ret = do_get(io_ctx, nargs[1], nargs[2], true);
    if (ret < 0) {
      cerr << "error getting " << pool_name << "/" << nargs[1] << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "put") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage();
    ret = do_put(io_ctx, nargs[1], nargs[2], op_size, true);
    if (ret < 0) {
      cerr << "error putting " << pool_name << "/" << nargs[1] << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "setxattr") == 0) {
    if (!pool_name || nargs.size() < 4)
      usage();

    string oid(nargs[1]);
    string attr_name(nargs[2]);
    string attr_val(nargs[3]);

    bufferlist bl;
    bl.append(attr_val.c_str(), attr_val.length());

    ret = io_ctx.setxattr(oid, attr_name.c_str(), bl);
    if (ret < 0) {
      cerr << "error setting xattr " << pool_name << "/" << oid << "/" << attr_name << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "getxattr") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage();

    string oid(nargs[1]);
    string attr_name(nargs[2]);

    bufferlist bl;
    ret = io_ctx.getxattr(oid, attr_name.c_str(), bl);
    if (ret < 0) {
      cerr << "error getting xattr " << pool_name << "/" << oid << "/" << attr_name << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
    string s(bl.c_str(), bl.length());
    cout << s << std::endl;
  } else if (strcmp(nargs[0], "rmxattr") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage();

    string oid(nargs[1]);
    string attr_name(nargs[2]);

    ret = io_ctx.rmxattr(oid, attr_name.c_str());
    if (ret < 0) {
      cerr << "error removing xattr " << pool_name << "/" << oid << "/" << attr_name << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  } else if (strcmp(nargs[0], "listxattr") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage();

    string oid(nargs[1]);
    map<std::string, bufferlist> attrset;
    bufferlist bl;
    ret = io_ctx.getxattrs(oid, attrset);
    if (ret < 0) {
      cerr << "error getting xattr set " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }

    for (map<std::string, bufferlist>::iterator iter = attrset.begin();
         iter != attrset.end(); ++iter) {
      cout << iter->first << std::endl;
    }
  }
  else if (strcmp(nargs[0], "rm") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage();
    string oid(nargs[1]);
    ret = io_ctx.remove(oid);
    if (ret < 0) {
      cerr << "error removing " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "create") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage();
    string oid(nargs[1]);
    ret = io_ctx.create(oid, true);
    if (ret < 0) {
      cerr << "error creating " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }

  else if (strcmp(nargs[0], "tmap") == 0) {
    if (nargs.size() < 3)
      usage();
    if (strcmp(nargs[1], "dump") == 0) {
      string oid(nargs[2]);
      ret = io_ctx.read(oid, outdata, 0, 0);
      if (ret < 0) {
	cerr << "error reading " << pool_name << "/" << oid << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
	return 1;
      }
      bufferlist::iterator p = outdata.begin();
      bufferlist header;
      map<string, bufferlist> kv;
      ::decode(header, p);
      ::decode(kv, p);
      cout << "header (" << header.length() << " bytes):\n";
      header.hexdump(cout);
      cout << "\n";
      cout << kv.size() << " keys\n";
      for (map<string,bufferlist>::iterator q = kv.begin(); q != kv.end(); q++) {
	cout << "key '" << q->first << "' (" << q->second.length() << " bytes):\n";
	q->second.hexdump(cout);
	cout << "\n";
      }
    }
    else if (strcmp(nargs[1], "set") == 0 ||
	     strcmp(nargs[1], "create") == 0) {
      if (nargs.size() < 5)
	usage();
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

  else if (strcmp(nargs[0], "mkpool") == 0) {
    int auid = 0;
    __u8 crush_rule = 0;
    if (nargs.size() < 2)
      usage();
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
      return 1;
    }
    cout << "successfully created pool " << nargs[1] << std::endl;
  }
  else if (strcmp(nargs[0], "rmpool") == 0) {
    if (nargs.size() < 2)
      usage();
    ret = rados.pool_delete(nargs[1]);
    if (ret >= 0) {
      cout << "successfully deleted pool " << nargs[1] << std::endl;
    } else { //error
      cerr << "pool " << nargs[1] << " does not exist" << std::endl;
    }
  }
  else if (strcmp(nargs[0], "lssnap") == 0) {
    if (!pool_name || nargs.size() != 1)
      usage();

    vector<snap_t> snaps;
    io_ctx.snap_list(&snaps);
    for (vector<snap_t>::iterator i = snaps.begin();
	 i != snaps.end();
	 i++) {
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
      usage();

    ret = io_ctx.snap_create(nargs[1]);
    if (ret < 0) {
      cerr << "error creating pool " << pool_name << " snapshot " << nargs[1]
	   << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
    cout << "created pool " << pool_name << " snap " << nargs[1] << std::endl;
  }

  else if (strcmp(nargs[0], "rmsnap") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage();

    ret = io_ctx.snap_remove(nargs[1]);
    if (ret < 0) {
      cerr << "error removing pool " << pool_name << " snapshot " << nargs[1]
	   << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
    cout << "removed pool " << pool_name << " snap " << nargs[1] << std::endl;
  }

  else if (strcmp(nargs[0], "rollback") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage();

    ret = io_ctx.rollback(nargs[1], nargs[2]);
    if (ret < 0) {
      cerr << "error rolling back pool " << pool_name << " to snapshot " << nargs[1]
	   << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
    cout << "rolled back pool " << pool_name
	 << " to snapshot " << nargs[2] << std::endl;
  }
  else if (strcmp(nargs[0], "import") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage();

    ret = do_import(io_ctx, nargs[1], op_size);
    if (ret < 0) {
      cerr << "error importing " << pool_name << "/" << nargs[1] << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "export") == 0) {
    if (!pool_name || nargs.size() < 2)
      usage();

    ret = do_export(io_ctx, nargs[1]);
    if (ret < 0) {
      cerr << "error exporting " << pool_name << "/" << nargs[1] << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
      return 1;
    }
  }
  else if (strcmp(nargs[0], "bench") == 0) {
    if (!pool_name || nargs.size() < 3)
      usage();
    int seconds = atoi(nargs[1]);
    int operation = 0;
    if (strcmp(nargs[2], "write") == 0)
      operation = OP_WRITE;
    else if (strcmp(nargs[2], "seq") == 0)
      operation = OP_SEQ_READ;
    else if (strcmp(nargs[2], "rand") == 0)
      operation = OP_RAND_READ;
    else
      usage();
    ret = aio_bench(rados, io_ctx, operation, seconds, concurrent_ios, op_size);
    if (ret != 0)
      cerr << "error during benchmark: " << ret << std::endl;
  }
  else {
    cerr << "unrecognized command " << nargs[0] << std::endl;
    usage();
  }

  if (ret)
    cerr << "error " << (-ret) << ": " << strerror_r(-ret, buf, sizeof(buf)) << std::endl;
  return (ret < 0) ? 1 : 0;
}

