

class FakeStoreCollections {
 private:
  Mutex lock;
  hash_map<coll_t, set<object_t> > fakecollections;

 public:
  // faked collections
  int list_collections(list<coll_t>& ls) {
	lock.Lock();
	int r = 0;
	for (hash_map< coll_t, set<object_t> >::iterator p = fakecollections.begin();
		 p != fakecollections.end();
		 p++) {
	  r++;
	  ls.push_back(p->first);
	}
	lock.Unlock();
	return r;
  }

  int create_collection(coll_t c) {
	lock.Lock();
	fakecollections[c].size();
	lock.Unlock();
	return 0;
  }

  int destroy_collection(coll_t c) {
	int r = 0;
	lock.Lock();
	if (fakecollections.count(c)) {
	  fakecollections.erase(c);
	  fakecattr.erase(c);
	} else 
	  r = -1;
	lock.Unlock();
	return r;
  }

  int collection_stat(coll_t c, struct stat *st) {
	return collection_exists(c) ? 0:-1;
  }

  bool collection_exists(coll_t c) {
	lock.Lock();
	int r = fakecollections.count(c);
	lock.Unlock();
	return r;
  }

  int collection_add(coll_t c, object_t o) {
	lock.Lock();
	fakecollections[c].insert(o);
	lock.Unlock();
	return 0;
  }

  int collection_remove(coll_t c, object_t o) {
	lock.Lock();
	fakecollections[c].erase(o);
	lock.Unlock();
	return 0;
  }

  int collection_list(coll_t c, list<object_t>& o) {
	lock.Lock();
	int r = 0;
	for (set<object_t>::iterator p = fakecollections[c].begin();
		 p != collectsion[c].end();
		 p++) {
	  o.push_back(*p);
	  r++;
	}
	lock.Unlock();
	return r;
  }

};

class FakeStoreAttrs {
 private:
  
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


 public:
  // faked attrs
  int setattr(object_t oid, const char *name,
			  void *value, size_t size) {
	lock.Lock();
	int r = fakeoattrs[oid].setattr(name, value, size);
	lock.Unlock();
	return r;
  }
  int getattr(object_t oid, const char *name,
			  void *value, size_t size) {
	lock.Lock();
	int r = fakeoattrs[oid].getattr(name, value, size);
	lock.Unlock();
	return r;
  }
  int listattr(object_t oid, char *attrs, size_t size) {
	lock.Lock();
	int r = fakeoattrs[oid].listattr(attrs,size);
	lock.Unlock();
	return r;
  }


  int collection_setattr(coll_t c, const char *name,
						 void *value, size_t size) {
	lock.Lock();
	int r = fakecattrs[cid].setattr(name, value, size);
	lock.Unlock();
	return r;
  }
  int collection_getattr(coll_t c, const char *name,
						 void *value, size_t size) {
	lock.Lock();
	int r = fakecattrs[cid].getattr(name, value, size);
	lock.Unlock();
	return r;
  }
  int collection_listattr(coll_t c, char *attrs, size_t size) {
	lock.Lock();
	int r = fakecattrs[cid].listattr(attrs,size);
	lock.Unlock();
	return r;
  }

};



int OBFSStore::list_collections(list<coll_t>& ls)
{
  lock.Lock();
  int r = 0;
  for (hash_map< coll_t, set<object_t> >::iterator p = fakecollections.begin();
	   p != fakecollections.end();
	   p++) {
	r++;
	ls.push_back(p->first);
  }
  lock.Unlock();
  return r;
}

int OBFSStore::create_collection(coll_t c)
{
  lock.Lock();
  fakecollections[c].size();
  lock.Unlock();
  return 0;
}

int OBFSStore::destroy_collection(coll_t c)
{

}

int OBFSStore::collection_stat(coll_t c, struct stat *st)
{

}

bool OBFSStore::collection_exists(coll_t c) 
{
  lock.Lock();
  int r = fakecollections.count(c);
  lock.Unlock();
  return r;
}

int OBFSStore::collection_add(coll_t c, object_t o) 
{
  lock.Lock();
  fakecollections[c].insert(o);
  lock.Unlock();
  return 0;
}

int OBFSStore::collection_remove(coll_t c, object_t o)
{
  lock.Lock();
  fakecollections[c].erase(o);
  lock.Unlock();
  return 0;
}

int OBFSStore::collection_list(coll_t c, list<object_t>& o)
{
  lock.Lock();
  int r = 0;
  for (set<object_t>::iterator p = fakecollections[c].begin();
	   p != collectsion[c].end();
	   p++) {
	o.push_back(*p);
	r++;
  }
  lock.Unlock();
  return r;
}

int OBFSStore::collection_setattr(coll_t c, const char *name,
								  void *value, size_t size)
{
  lock.Lock();
  int r = fakecattr[c].setattr(name, value, size);
  lock.Unlock();
  return r;
}

int OBFSStore::collection_getattr(coll_t c, const char *name,
								  void *value, size_t size)
{
  lock.Lock();
  int r = fakecattr[c].getattr(name,value,size);
  lock.Unlock();
  return r;
}

int OBFSStore::collection_listattr(coll_t c, char *attrs, size_t size)
{
  lock.Lock();
  int r = fakecattr[c].listattr(attrs,size);
  lock.Unlock();
  return r;
}
