#ifndef __BERKELEYDB_H
#define __BERKELEYDB_H

#include <db.h>
#include <list>
using namespace std;

template<typename K, typename D>
class BDBMap {
 private:
  DB *dbp;
  
 public:
  BDBMap() {
	int r;
	if ((r = db_create(&dbp, NULL, 0)) != 0) {
	  cerr << "db_create: " << db_strerror(r) << endl;
	  assert(0);
	}
  }
  ~BDBMap() {
	close();
  }

  // open/close
  int open(const char *fn) {
	int r = dbp->open(dbp, NULL, fn, NULL, DB_BTREE, DB_CREATE, 0644);
	assert(r == 0);
	return 0;
  }
  void close() {
	dbp->close(dbp,0);
	dbp = 0;
  }
  void remove(const char *fn) {
	dbp->remove(dbp, fn, 0, 0);
	dbp = 0;
  }
  
  // accessors
  int put(K key,
		  D data) {
	DBT k;
	k.data = &key;
	k.size = sizeof(K);
	DBT d;
	d.data = &data;
	d.size = sizeof(data);
	return dbp->put(dbp, NULL, &k, &d, 0);
  }

  int get(K key,
		  D& data) {
	DBT k;
	k.data = &key;
	k.size = sizeof(key);
	DBT d;
	d.data = &data;
	d.size = sizeof(data);
	int r = dbp->get(dbp, NULL, &k, &d, 0);
	return r;
  }

  int del(K key) {
	DBT k;
	k.data = &key;
	k.size = sizeof(key);
	return dbp->del(dbp, NULL, &k, 0);
  }

  int list_keys(list<K>& ls) {
	DBC *cursor = 0;
	int r = dbp->cursor(dbp, NULL, &cursor, 0);
	assert(r == 0);

	K key;
	D data;

	DBT k,d;
	k.data = &key;
	k.size = sizeof(key);
	d.data = &data;
	d.size = sizeof(data);

	while (1) {
	  int r = cursor->c_get(cursor, &k, &d, DB_NEXT);
	  if (r == DB_NOTFOUND) break;
	  assert(r == 0);
	  ls.push_back(key);
	}
	cursor->c_close(cursor);
	return 0;
  }
  
};

#endif
