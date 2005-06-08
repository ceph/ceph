#ifndef __FAKESTORE_H
#define __FAKESTORE_H

#include "ObjectStore.h"

class FakeStore : public ObjectStore {
  string basedir;
  int whoami;

  void get_dir(string& dir);
  void get_oname(object_t oid, string& fn, bool shadow=false);
  void wipe_dir(string mydir);

  /* shadow: copy-on-write behavior against a "starting" clean object store... 
	 
  if (is_shadow == true), 
    shadowdir has same layout as basedir
	
    if the (normal, live) object file:
	 - doesn't exist, then use the shadow file if it exists
	 - does exist, use the live file (in its entirety, COW is on object granularity)
	 - is a symlink to a nonexistant file, the object doesn't exist (even if it does in the shadow dir)

	write, truncate initiate a copy from shadow -> live.
	unlink may create a bad symlink if the shadow file exists
	
	etc.

	wipe wipes the live dir, effectively revertiing to the shadow fs, so be careful as 
	   this isn't what a MDS mkfs expects!
  */
  string shadowdir;
  bool is_shadow;
  void shadow_copy_maybe(object_t oid);   // do copy-on-write.. called by write(), truncate()

 public:
  FakeStore(char *base, int whoami, char *shadow = 0);

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
