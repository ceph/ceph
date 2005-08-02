#ifndef __FAKESTORE_H
#define __FAKESTORE_H

#include "ObjectStore.h"
#include "BDBMap.h"

#include <map>
using namespace std;

class FakeStore : public ObjectStore {
  string basedir;
  int whoami;

  // fns
  void get_dir(string& dir);
  void get_oname(object_t oid, string& fn);
  void wipe_dir(string mydir);


 public:
  FakeStore(char *base, int whoami);

  int init();
  int finalize();
  int mkfs();


  // ------------------
  // objects
  bool exists(object_t oid);
  int stat(object_t oid, struct stat *st);
  int remove(object_t oid);
  int truncate(object_t oid, off_t size);
  int read(object_t oid, 
		   size_t len, off_t offset,
		   char *buffer);
  int write(object_t oid,
			size_t len, off_t offset,
			char *buffer,
			bool fsync);

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
  int collection_stat(coll_t c, struct stat *st);
  int collection_create(coll_t c);
  int collection_destroy(coll_t c);
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
