
#ifndef _OBFSSTORE_H_
#define _OBFSSTORE_H_

#include "ObjectStore.h"

class OBFSStore : public ObjectStore {

  class FakeAttrSet {
  public:
	map<const char*, bufferptr> attrs;
	
	int getattr(const char *name, void *value, size_t size) {
	  if (attrs.count(name)) {
		size_t l = attrs[name].length();
		if (l > size) l = size;
		bufferlist bl;
		bl.append(attrs[name]);
		bl.copy(0, l, (char*)value);
		return l;
	  }
	  return -1;
	}
	
	int setattr(const char *name, void *value, size_t size) {
	  bufferptr bp(new buffer((char*)value,size));
	  attrs[name] = bp;
	  return 0;
	}
	
	int listattr(char *attrs, size_t size) {
	  assert(0);
	}
	
	bool empty() { return attrs.empty(); }
  };

  Mutex lock;
  hash_map<object_t, FakeAttrSet> fakeoattrs;
  hash_map<object_t, FakeAttrSet> fakecattrs;
  hash_map<coll_t, set<object_t> > fakecollections;

  int	whoami;
  int	bdev_id;
  int	mounted;
  char	dev[128];
  char	param[128];
  
 public:
  OBFSStore(int whoami, char *param, char *dev);
  
  int mount(void);
  int umount(void);
  int mkfs(void);
  
  bool exists(object_t oid);
  int stat(object_t oid, struct stat *st);
  
  int remove(object_t oid);
  int truncate(object_t oid, off_t size);
  
  int read(object_t oid, size_t len, 
		   off_t offset, char *buffer);
  int write(object_t oid, size_t len, 
			off_t offset,char *buffer,
			bool fsync);
  
  // faked attrs
  int setattr(object_t oid, const char *name,
				void *value, size_t size);
  int getattr(object_t oid, const char *name,
			  void *value, size_t size);
  int listattr(object_t oid, char *attrs, size_t size);


  // faked collections
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
