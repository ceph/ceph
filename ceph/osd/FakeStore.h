// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef __FAKESTORE_H
#define __FAKESTORE_H

#include "ObjectStore.h"
#include "common/ThreadPool.h"
#include "common/Mutex.h"

#include "Fake.h"
#include "FakeStoreBDBCollections.h"


#include <map>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


// fake attributes in memory, if we need to.


class FakeStore : public ObjectStore, 
				  public FakeStoreAttrs,
				  public FakeStoreCollections {
  string basedir;
  int whoami;
  
  Mutex lock;

  // fns
  void get_dir(string& dir);
  void get_oname(object_t oid, string& fn);
  void wipe_dir(string mydir);


  /*
  // async fsync
  class ThreadPool<class FakeStore, pair<int, class Context*> >  *fsync_threadpool;
  void queue_fsync(int fd, class Context *c) {
	fsync_threadpool->put_op(new pair<int, class Context*>(fd,c));
  }
 public:
  void do_fsync(int fd, class Context *c);
  static void dofsync(class FakeStore *f, pair<int, class Context*> *af) {
	f->do_fsync(af->first, af->second);
	delete af;
  }
  */

 public:
  FakeStore(char *base, int whoami);

  int mount();
  int umount();
  int mkfs();

  int statfs(struct statfs *buf);

  // ------------------
  // objects
  bool exists(object_t oid);
  int stat(object_t oid, struct stat *st);
  int remove(object_t oid);
  int truncate(object_t oid, off_t size);
  int read(object_t oid, 
		   size_t len, off_t offset,
		   bufferlist& bl);
  int write(object_t oid,
			size_t len, off_t offset,
			bufferlist& bl,
			bool fsync);
  int write(object_t oid, 
			size_t len, off_t offset, 
			bufferlist& bl, 
			Context *onsafe);

};

#endif
