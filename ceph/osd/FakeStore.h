#ifndef __FAKESTORE_H
#define __FAKESTORE_H

#include "ObjectStore.h"
#include "BDBMap.h"
#include "common/ThreadPool.h"

#include <map>
using namespace std;


class FakeStore : public ObjectStore {
  string basedir;
  int whoami;

  // fns
  void get_dir(string& dir);
  void get_oname(object_t oid, string& fn);
  void wipe_dir(string mydir);


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

  int setattr(object_t oid, const char *name,
				void *value, size_t size);
  int getattr(object_t oid, const char *name,
			  void *value, size_t size);
  int listattr(object_t oid, char *attrs, size_t size);


  // -------------------
  // collections

 private:
  // collection dbs
  BDBMap<coll_t, int>                 collections;
  map<coll_t, BDBMap<object_t, int>*> collection_map;

  void get_collfn(coll_t c, string &fn);
  int open_collection(coll_t c);

  void open_collections();
  void close_collections();

 public:
  int list_collections(list<coll_t>& ls);
  int create_collection(coll_t c);
  int destroy_collection(coll_t c);
  int collection_stat(coll_t c, struct stat *st);
  bool collection_exists(coll_t c);
  int collection_add(coll_t c, object_t o);
  int collection_remove(coll_t c, object_t o);
  int collection_list(coll_t c, list<object_t>& o);

  int collection_setattr(coll_t c, const char *name,
						 void *value, size_t size);
  int collection_getattr(coll_t c, const char *name,
						 void *value, size_t size);
  int collection_listattr(coll_t c, char *attrs, size_t size);
  

};

#endif
