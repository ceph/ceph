#ifndef __FAKESTORE_H
#define __FAKESTORE_H

#include "ObjectStore.h"

class FakeStore : public ObjectStore {
  string basedir;
  int whoami;

  void make_dir(string& dir) {
	static char s[30];
	sprintf(s, "%d", whoami);
	dir = basedir + "/" + s;
  }
  void make_oname(object_t oid, string& fn) {
	static char s[100];
	sprintf(s, "%d/%lld", whoami, oid);
	fn = basedir + "/" + s;
  }

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
