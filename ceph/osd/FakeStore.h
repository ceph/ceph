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


  // -------------------
  // collections

 private:
  // collection dbs
  BDBMap<coll_t, int>                 collections;
  map<coll_t, BDBMap<object_t, int>*> collection_map;

  void get_collfn(coll_t c, string &fn);
  void open_collection(coll_t c);

 public:
  int collection_create(coll_t c);
  int collection_destroy(coll_t c);
  int collection_add(coll_t c, object_t o);
  int collection_remove(coll_t c, object_t o);
  int collection_list(coll_t c, list<object_t>& o);


  // -------------------
  // attributes
  
  int setattr(object_t oid, const char *name,
				void *value, size_t size);
  int getattr(object_t oid, const char *name,
			  void *value, size_t size);
  int listattr(object_t oid, char *attrs, size_t size);

};

#endif
