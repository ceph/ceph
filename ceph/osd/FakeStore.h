#ifndef __FAKESTORE_H
#define __FAKESTORE_H

#include "ObjectStore.h"

class FakeStore : public ObjectStore {
  string basedir;
  int whoami;

  void get_dir(string& dir);
  void get_oname(object_t oid, string& fn);
  void wipe_dir(string mydir);

 public:
  FakeStore(char *base, int whoami);

  int init();
  int finalize();
  int mkfs();

  bool exists(object_t oid);
  int stat(object_t oid,
		   struct stat *st);

  int remove(object_t oid);
  int truncate(object_t oid, off_t size);

  int read(object_t oid, 
		   size_t len, off_t offset,
		   char *buffer);
  int write(object_t oid,
			size_t len, off_t offset,
			char *buffer);
};

#endif
