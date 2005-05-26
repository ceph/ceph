

#include "ObjectStore.h"

class FakeStore : public ObjectStore {
  string basedir;

  void make_oname(object_t oid, string& fn) {
	static char oname[50];
	sprintf(oname, "%lld", oid);
	fn = basedir + "/" + oname;
  }

 public:
  FakeStore(string& basedir) {
	this->basedir = basedir;
  }

  bool exists(object_t oid) {
	string fn;
	make_oname(oid, fn);
	
	// stat
  }

  int stat(object_t oid,
		   struct stat *st);

  int read(object_t oid, 
		   size_t len, off_t offset,
		   char *buffer);
  int write(object_t oid,
			size_t len, off_t offset,
			char *buffer);
  int unlink(object_t oid);

};
