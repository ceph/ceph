// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <tr1/memory>
#include <map>
#include <set>
#include <boost/scoped_ptr.hpp>

#include "include/buffer.h"
#include "test/ObjectMap/KeyValueDBMemory.h"
#include "os/KeyValueDB.h"
#include "os/DBObjectMap.h"
#include "os/HashIndex.h"
#include "os/LevelDBStore.h"
#include <sys/types.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include <dirent.h>

#include "gtest/gtest.h"
#include "stdlib.h"

using namespace std;

template <typename T>
typename T::iterator rand_choose(T &cont) {
  if (cont.size() == 0) {
    return cont.end();
  }
  int index = rand() % cont.size();
  typename T::iterator retval = cont.begin();

  for (; index > 0; --index) ++retval;
  return retval;
}

string num_str(unsigned i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%.10u", i);
  return string(buf);
}

class ObjectMapTester {
public:
  ObjectMap *db;
  set<string> key_space;
  set<string> object_name_space;
  map<string, map<string, string> > omap;
  map<string, string > hmap;
  map<string, map<string, string> > xattrs;
  unsigned seq;

  ObjectMapTester() : db(0), seq(0) {}

  string val_from_key(const string &object, const string &key) {
    return object + "_" + key + "_" + num_str(seq++);
  }

  void set_key(const string &objname, const string &key, const string &value) {
    set_key(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
	    key, value);
  }

  void set_xattr(const string &objname, const string &key, const string &value) {
    set_xattr(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
	      key, value);
  }

  void set_key(hobject_t hoid,
	       string key, string value) {
    map<string, bufferlist> to_write;
    bufferptr bp(value.c_str(), value.size());
    bufferlist bl;
    bl.append(bp);
    to_write.insert(make_pair(key, bl));
    db->set_keys(hoid, to_write);
  }

  void set_xattr(hobject_t hoid,
		 string key, string value) {
    map<string, bufferlist> to_write;
    bufferptr bp(value.c_str(), value.size());
    bufferlist bl;
    bl.append(bp);
    to_write.insert(make_pair(key, bl));
    db->set_xattrs(hoid, to_write);
  }

  void set_header(const string &objname, const string &value) {
    set_header(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
	       value);
  }

  void set_header(hobject_t hoid,
		  const string &value) {
    bufferlist header;
    header.append(bufferptr(value.c_str(), value.size() + 1));
    db->set_header(hoid, header);
  }

  int get_header(const string &objname, string *value) {
    return get_header(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
		      value);
  }

  int get_header(hobject_t hoid,
		 string *value) {
    bufferlist header;
    int r = db->get_header(hoid, &header);
    if (r < 0)
      return r;
    if (header.length())
      *value = string(header.c_str());
    else
      *value = string("");
    return 0;
  }

  int get_xattr(const string &objname, const string &key, string *value) {
    return get_xattr(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
		     key, value);
  }

  int get_xattr(hobject_t hoid,
		string key, string *value) {
    set<string> to_get;
    to_get.insert(key);
    map<string, bufferlist> got;
    db->get_xattrs(hoid, to_get, &got);
    if (!got.empty()) {
      *value = string(got.begin()->second.c_str(),
		      got.begin()->second.length());
      return 1;
    } else {
      return 0;
    }
  }

  int get_key(const string &objname, const string &key, string *value) {
    return get_key(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
		   key, value);
  }

  int get_key(hobject_t hoid,
	      string key, string *value) {
    set<string> to_get;
    to_get.insert(key);
    map<string, bufferlist> got;
    db->get_values(hoid, to_get, &got);
    if (!got.empty()) {
      *value = string(got.begin()->second.c_str(),
		      got.begin()->second.length());
      return 1;
    } else {
      return 0;
    }
  }

  void remove_key(const string &objname, const string &key) {
    remove_key(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
	       key);
  }

  void remove_key(hobject_t hoid,
		  string key) {
    set<string> to_remove;
    to_remove.insert(key);
    db->rm_keys(hoid, to_remove);
  }

  void remove_xattr(const string &objname, const string &key) {
    remove_xattr(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
		 key);
  }

  void remove_xattr(hobject_t hoid,
		    string key) {
    set<string> to_remove;
    to_remove.insert(key);
    db->remove_xattrs(hoid, to_remove);
  }

  void clone(const string &objname, const string &target) {
    clone(hobject_t(sobject_t(objname, CEPH_NOSNAP)),
	  hobject_t(sobject_t(target, CEPH_NOSNAP)));
  }

  void clone(hobject_t hoid,
	     hobject_t hoid2) {
    db->clone(hoid, hoid2);
  }

  void clear(const string &objname) {
    clear(hobject_t(sobject_t(objname, CEPH_NOSNAP)));
  }

  void clear(hobject_t hoid) {
    db->clear(hoid);
  }

  void def_init() {
    for (unsigned i = 0; i < 1000; ++i) {
      key_space.insert("key_" + num_str(i));
    }
    for (unsigned i = 0; i < 1000; ++i) {
      object_name_space.insert("name_" + num_str(i));
    }
  }

  void init_key_set(const set<string> &keys) {
    key_space = keys;
  }

  void init_object_name_space(const set<string> &onamespace) {
    object_name_space = onamespace;
  }

  void auto_set_xattr(ostream &out) {
    set<string>::iterator key = rand_choose(key_space);
    set<string>::iterator object = rand_choose(object_name_space);

    string value = val_from_key(*object, *key);

    xattrs[*object][*key] = value;
    set_xattr(*object, *key, value);

    out << "auto_set_xattr " << *object << ": " << *key << " -> "
	<< value << std::endl;
  }

  void auto_set_key(ostream &out) {
    set<string>::iterator key = rand_choose(key_space);
    set<string>::iterator object = rand_choose(object_name_space);

    string value = val_from_key(*object, *key);

    omap[*object][*key] = value;
    set_key(*object, *key, value);

    out << "auto_set_key " << *object << ": " << *key << " -> "
	<< value << std::endl;
  }

  void xattrs_on_object(const string &object, set<string> *out) {
    if (!xattrs.count(object))
      return;
    const map<string, string> &xmap = xattrs.find(object)->second;
    for (map<string, string>::const_iterator i = xmap.begin();
	 i != xmap.end();
	 ++i) {
      out->insert(i->first);
    }
  }

  void keys_on_object(const string &object, set<string> *out) {
    if (!omap.count(object))
      return;
    const map<string, string> &kmap = omap.find(object)->second;
    for (map<string, string>::const_iterator i = kmap.begin();
	 i != kmap.end();
	 ++i) {
      out->insert(i->first);
    }
  }

  void xattrs_off_object(const string &object, set<string> *out) {
    *out = key_space;
    set<string> xspace;
    xattrs_on_object(object, &xspace);
    for (set<string>::iterator i = xspace.begin();
	 i != xspace.end();
	 ++i) {
      out->erase(*i);
    }
  }

  void keys_off_object(const string &object, set<string> *out) {
    *out = key_space;
    set<string> kspace;
    keys_on_object(object, &kspace);
    for (set<string>::iterator i = kspace.begin();
	 i != kspace.end();
	 ++i) {
      out->erase(*i);
    }
  }

  int auto_check_present_xattr(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    set<string> xspace;
    xattrs_on_object(*object, &xspace);
    set<string>::iterator key = rand_choose(xspace);
    if (key == xspace.end()) {
      return 1;
    }

    string result;
    int r = get_xattr(*object, *key, &result);
    if (!r) {
      out << "auto_check_present_key: failed to find key "
	  << *key << " on object " << *object << std::endl;
      return 0;
    }

    if (result != xattrs[*object][*key]) {
      out << "auto_check_present_key: for key "
	  << *key << " on object " << *object
	  << " found value " << result << " where we should have found "
	  << xattrs[*object][*key] << std::endl;
      return 0;
    }

    out << "auto_check_present_key: for key "
	<< *key << " on object " << *object
	<< " found value " << result << " where we should have found "
	<< xattrs[*object][*key] << std::endl;
    return 1;
  }


  int auto_check_present_key(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    set<string> kspace;
    keys_on_object(*object, &kspace);
    set<string>::iterator key = rand_choose(kspace);
    if (key == kspace.end()) {
      return 1;
    }

    string result;
    int r = get_key(*object, *key, &result);
    if (!r) {
      out << "auto_check_present_key: failed to find key "
	  << *key << " on object " << *object << std::endl;
      return 0;
    }

    if (result != omap[*object][*key]) {
      out << "auto_check_present_key: for key "
	  << *key << " on object " << *object
	  << " found value " << result << " where we should have found "
	  << omap[*object][*key] << std::endl;
      return 0;
    }

    out << "auto_check_present_key: for key "
	<< *key << " on object " << *object
	<< " found value " << result << " where we should have found "
	<< omap[*object][*key] << std::endl;
    return 1;
  }

  int auto_check_absent_xattr(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    set<string> xspace;
    xattrs_off_object(*object, &xspace);
    set<string>::iterator key = rand_choose(xspace);
    if (key == xspace.end()) {
      return 1;
    }

    string result;
    int r = get_xattr(*object, *key, &result);
    if (!r) {
      out << "auto_check_absent_key: did not find key "
	  << *key << " on object " << *object << std::endl;
      return 1;
    }

    out << "auto_check_basent_key: for key "
	<< *key << " on object " << *object
	<< " found value " << result << " where we should have found nothing"
	<< std::endl;
    return 0;
  }

  int auto_check_absent_key(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    set<string> kspace;
    keys_off_object(*object, &kspace);
    set<string>::iterator key = rand_choose(kspace);
    if (key == kspace.end()) {
      return 1;
    }

    string result;
    int r = get_key(*object, *key, &result);
    if (!r) {
      out << "auto_check_absent_key: did not find key "
	  << *key << " on object " << *object << std::endl;
      return 1;
    }

    out << "auto_check_basent_key: for key "
	<< *key << " on object " << *object
	<< " found value " << result << " where we should have found nothing"
	<< std::endl;
    return 0;
  }

  void auto_clone_key(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    set<string>::iterator target = rand_choose(object_name_space);
    while (target == object) {
      target = rand_choose(object_name_space);
    }
    out << "clone " << *object << " to " << *target;
    clone(*object, *target);
    if (!omap.count(*object)) {
      out << " source missing.";
      omap.erase(*target);
    } else {
      out << " source present.";
      omap[*target] = omap[*object];
    }
    if (!hmap.count(*object)) {
      out << " hmap source missing." << std::endl;
      hmap.erase(*target);
    } else {
      out << " hmap source present." << std::endl;
      hmap[*target] = hmap[*object];
    }
    if (!xattrs.count(*object)) {
      out << " hmap source missing." << std::endl;
      xattrs.erase(*target);
    } else {
      out << " hmap source present." << std::endl;
      xattrs[*target] = xattrs[*object];
    }
  }

  void auto_remove_key(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    set<string> kspace;
    keys_on_object(*object, &kspace);
    set<string>::iterator key = rand_choose(kspace);
    if (key == kspace.end()) {
      return;
    }
    out << "removing " << *key << " from " << *object << std::endl;
    omap[*object].erase(*key);
    remove_key(*object, *key);
  }

  void auto_remove_xattr(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    set<string> kspace;
    xattrs_on_object(*object, &kspace);
    set<string>::iterator key = rand_choose(kspace);
    if (key == kspace.end()) {
      return;
    }
    out << "removing xattr " << *key << " from " << *object << std::endl;
    xattrs[*object].erase(*key);
    remove_xattr(*object, *key);
  }

  void auto_delete_object(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    out << "auto_delete_object " << *object << std::endl;
    clear(*object);
    omap.erase(*object);
    hmap.erase(*object);
    xattrs.erase(*object);
  }

  void auto_write_header(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    string header = val_from_key(*object, "HEADER");
    out << "auto_write_header: " << *object << " -> " << header << std::endl;
    set_header(*object, header);
    hmap[*object] = header;
  }

  int auto_verify_header(ostream &out) {
    set<string>::iterator object = rand_choose(object_name_space);
    out << "verify_header: " << *object << " ";
    string header;
    int r = get_header(*object, &header);
    if (r < 0) {
      assert(0);
    }
    if (header.size() == 0) {
      if (hmap.count(*object)) {
	out << " failed to find header " << hmap[*object] << std::endl;
	return 0;
      } else {
	out << " found no header" << std::endl;
	return 1;
      }
    }

    if (!hmap.count(*object)) {
      out << " found header " << header << " should have been empty"
	      << std::endl;
      return 0;
    } else if (header == hmap[*object]) {
      out << " found correct header " << header << std::endl;
      return 1;
    } else {
      out << " found incorrect header " << header
	  << " where we should have found " << hmap[*object] << std::endl;
      return 0;
    }
  }
};

class ObjectMapTest : public ::testing::Test {
public:
  boost::scoped_ptr< ObjectMap > db;
  ObjectMapTester tester;
  virtual void SetUp() {
    char *path = getenv("OBJECT_MAP_PATH");
    if (!path) {
      db.reset(new DBObjectMap(new KeyValueDBMemory()));
      tester.db = db.get();
      return;
    }

    string strpath(path);

    cerr << "using path " << strpath << std::endl;;
    LevelDBStore *store = new LevelDBStore(g_ceph_context, strpath);
    assert(!store->create_and_open(cerr));

    db.reset(new DBObjectMap(store));
    tester.db = db.get();
  }

  virtual void TearDown() {
    std::cerr << "Checking..." << std::endl;
    assert(db->check(std::cerr));
  }
};


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST_F(ObjectMapTest, CreateOneObject) {
  hobject_t hoid(sobject_t("foo", CEPH_NOSNAP));
  map<string, bufferlist> to_set;
  string key("test");
  string val("test_val");
  bufferptr bp(val.c_str(), val.size());
  bufferlist bl;
  bl.append(bp);
  to_set.insert(make_pair(key, bl));
  ASSERT_FALSE(db->set_keys(hoid, to_set));

  map<string, bufferlist> got;
  set<string> to_get;
  to_get.insert(key);
  to_get.insert("not there");
  db->get_values(hoid, to_get, &got);
  ASSERT_EQ(got.size(), (unsigned)1);
  ASSERT_EQ(string(got[key].c_str(), got[key].length()), val);

  bufferlist header;
  got.clear();
  db->get(hoid, &header, &got);
  ASSERT_EQ(got.size(), (unsigned)1);
  ASSERT_EQ(string(got[key].c_str(), got[key].length()), val);
  ASSERT_EQ(header.length(), (unsigned)0);

  db->rm_keys(hoid, to_get);
  got.clear();
  db->get(hoid, &header, &got);
  ASSERT_EQ(got.size(), (unsigned)0);

  db->clear(hoid);
  db->get(hoid, &header, &got);
  ASSERT_EQ(got.size(), (unsigned)0);
}

TEST_F(ObjectMapTest, CloneOneObject) {
  hobject_t hoid(sobject_t("foo", CEPH_NOSNAP));
  hobject_t hoid2(sobject_t("foo2", CEPH_NOSNAP));

  tester.set_key(hoid, "foo", "bar");
  tester.set_key(hoid, "foo2", "bar2");
  string result;
  int r = tester.get_key(hoid, "foo", &result);
  ASSERT_EQ(r, 1);
  ASSERT_EQ(result, "bar");

  db->clone(hoid, hoid2);
  r = tester.get_key(hoid, "foo", &result);
  ASSERT_EQ(r, 1);
  ASSERT_EQ(result, "bar");
  r = tester.get_key(hoid2, "foo", &result);
  ASSERT_EQ(r, 1);
  ASSERT_EQ(result, "bar");

  tester.remove_key(hoid, "foo");
  r = tester.get_key(hoid2, "foo", &result);
  ASSERT_EQ(r, 1);
  ASSERT_EQ(result, "bar");
  r = tester.get_key(hoid, "foo", &result);
  ASSERT_EQ(r, 0);
  r = tester.get_key(hoid, "foo2", &result);
  ASSERT_EQ(r, 1);
  ASSERT_EQ(result, "bar2");

  tester.set_key(hoid, "foo", "baz");
  tester.remove_key(hoid, "foo");
  r = tester.get_key(hoid, "foo", &result);
  ASSERT_EQ(r, 0);

  tester.set_key(hoid, "foo2", "baz");
  tester.remove_key(hoid, "foo2");
  r = tester.get_key(hoid, "foo2", &result);
  ASSERT_EQ(r, 0);

  map<string, bufferlist> got;
  bufferlist header;

  got.clear();
  db->clear(hoid);
  db->get(hoid, &header, &got);
  ASSERT_EQ(got.size(), (unsigned)0);

  got.clear();
  r = db->clear(hoid2);
  ASSERT_EQ(0, r);
  db->get(hoid2, &header, &got);
  ASSERT_EQ(got.size(), (unsigned)0);

  tester.set_key(hoid, "baz", "bar");
  got.clear();
  db->get(hoid, &header, &got);
  ASSERT_EQ(got.size(), (unsigned)1);
  db->clear(hoid);
  db->clear(hoid2);
}

TEST_F(ObjectMapTest, OddEvenClone) {
  hobject_t hoid(sobject_t("foo", CEPH_NOSNAP));
  hobject_t hoid2(sobject_t("foo2", CEPH_NOSNAP));

  for (unsigned i = 0; i < 1000; ++i) {
    tester.set_key(hoid, "foo" + num_str(i), "bar" + num_str(i));
  }

  db->clone(hoid, hoid2);

  int r = 0;
  for (unsigned i = 0; i < 1000; ++i) {
    string result;
    r = tester.get_key(hoid, "foo" + num_str(i), &result);
    ASSERT_EQ(1, r);
    ASSERT_EQ("bar" + num_str(i), result);
    r = tester.get_key(hoid2, "foo" + num_str(i), &result);
    ASSERT_EQ(1, r);
    ASSERT_EQ("bar" + num_str(i), result);

    if (i % 2) {
      tester.remove_key(hoid, "foo" + num_str(i));
    } else {
      tester.remove_key(hoid2, "foo" + num_str(i));
    }
  }

  for (unsigned i = 0; i < 1000; ++i) {
    string result;
    string result2;
    r = tester.get_key(hoid, "foo" + num_str(i), &result);
    int r2 = tester.get_key(hoid2, "foo" + num_str(i), &result2);
    if (i % 2) {
      ASSERT_EQ(0, r);
      ASSERT_EQ(1, r2);
      ASSERT_EQ("bar" + num_str(i), result2);
    } else {
      ASSERT_EQ(0, r2);
      ASSERT_EQ(1, r);
      ASSERT_EQ("bar" + num_str(i), result);
    }
  }

  {
    ObjectMap::ObjectMapIterator iter = db->get_iterator(hoid);
    iter->seek_to_first();
    for (unsigned i = 0; i < 1000; ++i) {
      if (!(i % 2)) {
	ASSERT_TRUE(iter->valid());
	ASSERT_EQ("foo" + num_str(i), iter->key());
	iter->next();
      }
    }
  }

  {
    ObjectMap::ObjectMapIterator iter2 = db->get_iterator(hoid2);
    iter2->seek_to_first();
    for (unsigned i = 0; i < 1000; ++i) {
      if (i % 2) {
	ASSERT_TRUE(iter2->valid());
	ASSERT_EQ("foo" + num_str(i), iter2->key());
	iter2->next();
      }
    }
  }

  db->clear(hoid);
  db->clear(hoid2);
}

TEST_F(ObjectMapTest, RandomTest) {
  tester.def_init();
  for (unsigned i = 0; i < 5000; ++i) {
    unsigned val = rand();
    val <<= 8;
    val %= 100;
    if (!(i%100))
      std::cout << "on op " << i
		<< " val is " << val << std::endl;

    if (val < 7) {
      tester.auto_write_header(std::cerr);
    } else if (val < 14) {
      ASSERT_TRUE(tester.auto_verify_header(std::cerr));
    } else if (val < 30) {
      tester.auto_set_key(std::cerr);
    } else if (val < 42) {
      tester.auto_set_xattr(std::cerr);
    } else if (val < 55) {
      ASSERT_TRUE(tester.auto_check_present_key(std::cerr));
    } else if (val < 62) {
      ASSERT_TRUE(tester.auto_check_present_xattr(std::cerr));
    } else if (val < 70) {
      ASSERT_TRUE(tester.auto_check_absent_key(std::cerr));
    } else if (val < 73) {
      ASSERT_TRUE(tester.auto_check_absent_xattr(std::cerr));
    } else if (val < 76) {
      tester.auto_delete_object(std::cerr);
    } else if (val < 85) {
      tester.auto_clone_key(std::cerr);
    } else if (val < 92) {
      tester.auto_remove_xattr(std::cerr);
    } else {
      tester.auto_remove_key(std::cerr);
    }
  }
}
