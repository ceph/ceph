// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <gtest/gtest.h>
#include <stdlib.h>
#include <limits.h>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "global/global_init.h"
#include "common/config.h"
#include "common/Finisher.h"
#include "os/filestore/FileJournal.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "common/safe_io.h"
#include "os/filestore/JournalingObjectStore.h"

Finisher *finisher;
Cond sync_cond;
char path[200];
uuid_d fsid;
struct test_info {
    bool directio, aio, faio;
    const char *description;
} subtests[3] = {
    { false, false, false, "DIRECTIO OFF  AIO OFF" },
    { true, false, false, "DIRECTIO ON  AIO OFF" },
    { true, true, true, "DIRECTIO ON  AIO ON"}
};

// ----
Cond cond;
Mutex wait_lock("lock");
bool done;

void wait()
{
  wait_lock.Lock();
  while (!done)
    cond.Wait(wait_lock);
  wait_lock.Unlock();
}

// ----
class C_Sync {
public:
  Cond cond;
  Mutex lock;
  bool done;
  C_SafeCond *c;

  C_Sync()
    : lock("C_Sync::lock"), done(false) {
    c = new C_SafeCond(&lock, &cond, &done);
  }
  ~C_Sync() {
    lock.Lock();
    //cout << "wait" << std::endl;
    while (!done)
      cond.Wait(lock);
    //cout << "waited" << std::endl;
    lock.Unlock();
  }
};

unsigned size_mb = 200;
//Gtest argument prefix
const char GTEST_PRFIX[] = "--gtest_";

int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  char mb[10];
  sprintf(mb, "%u", size_mb);
  g_ceph_context->_conf->set_val("osd_journal_size", mb);
  g_ceph_context->_conf->apply_changes(NULL);

  finisher = new Finisher(g_ceph_context);
  
  path[0] = '\0';
  if (!args.empty()) {
    for ( unsigned int i = 0; i < args.size(); ++i) {
      if (strncmp(args[i], GTEST_PRFIX, sizeof(GTEST_PRFIX) - 1)) {
      	//Non gtest argument, set to path.
        size_t copy_len = std::min(sizeof(path) - 1, strlen(args[i]));
        strncpy(path, args[i], copy_len);
        path[copy_len] = '\0';
        break;
      }
    }
  }
  if ( path[0] == '\0') {
    srand(getpid() + time(0));
    snprintf(path, sizeof(path), "/var/tmp/ceph_test_filejournal.tmp.%d", rand());
  }
  cout << "path " << path << std::endl;

  ::testing::InitGoogleTest(&argc, argv);

  finisher->start();

  int r = RUN_ALL_TESTS();
  
  finisher->stop();

  unlink(path);
  
  return r;
}

TEST(TestFileJournal, Create) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "false");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);

  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
  }
}

TEST(TestFileJournal, WriteSmall) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "false");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);

  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    vector<ObjectStore::Transaction> tls;
    bufferlist bl;
    bl.append("small");
    int orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(1, bl, orig_len, new C_SafeCond(&wait_lock, &cond, &done));
    wait();

    j.close();
  }
}

TEST(TestFileJournal, WriteBig) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "false");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);

  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    bufferlist bl;
    while (bl.length() < size_mb*1000/2) {
      char foo[1024*1024];
      memset(foo, 1, sizeof(foo));
      bl.append(foo, sizeof(foo));
    }
    vector<ObjectStore::Transaction> tls;
    int orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(1, bl, orig_len, new C_SafeCond(&wait_lock, &cond, &done));
    wait();
    j.close();
  }
}

TEST(TestFileJournal, WriteMany) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "false");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);

  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&wait_lock, &cond, &done));

    vector<ObjectStore::Transaction> tls;
    bufferlist bl;
    bl.append("small");
    uint64_t seq = 1;
    for (int i=0; i<100; i++) {
      bl.append("small");
      int orig_len = j.prepare_entry(tls, &bl);
      j.reserve_throttle_and_backoff(bl.length());
      j.submit_entry(seq++, bl, orig_len, gb.new_sub());
    }
    gb.activate();

    wait();

    j.close();
  }
}

TEST(TestFileJournal, WriteManyVecs) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "false");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);

  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&wait_lock, &cond, &done));

    bufferlist first;
    first.append("small");
    vector<ObjectStore::Transaction> tls;
    int orig_len = j.prepare_entry(tls, &first);
    j.reserve_throttle_and_backoff(first.length());
    j.submit_entry(1, first, orig_len, gb.new_sub());

    bufferlist bl;
    for (int i=0; i<IOV_MAX * 2; i++) {
      bufferptr bp = buffer::create_page_aligned(4096);
      memset(bp.c_str(), (char)i, 4096);
      bl.append(bp);
    }
    bufferlist origbl = bl;
    orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(2, bl, orig_len, gb.new_sub());
    gb.activate();
    wait();

    j.close();

    j.open(1);
    bufferlist inbl;
    string v;
    uint64_t seq = 0;
    ASSERT_EQ(true, j.read_entry(inbl, seq));
    ASSERT_EQ(seq, 2ull);
    ASSERT_TRUE(inbl.contents_equal(origbl));
    j.make_writeable();
    j.close();

  }
}

TEST(TestFileJournal, ReplaySmall) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "false");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);

  vector<ObjectStore::Transaction> tls;

  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&wait_lock, &cond, &done));

    bufferlist bl;
    bl.append("small");
    int orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(1, bl, orig_len, gb.new_sub());
    bl.append("small");
    orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(2, bl, orig_len, gb.new_sub());
    bl.append("small");
    orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(3, bl, orig_len, gb.new_sub());
    gb.activate();
    wait();

    j.close();

    j.open(1);

    bufferlist inbl;
    string v;
    uint64_t seq = 0;
    ASSERT_EQ(true, j.read_entry(inbl, seq));
    ASSERT_EQ(seq, 2ull);
    inbl.copy(0, inbl.length(), v);
    ASSERT_EQ("small", v);
    inbl.clear();
    v.clear();

    ASSERT_EQ(true, j.read_entry(inbl, seq));
    ASSERT_EQ(seq, 3ull);
    inbl.copy(0, inbl.length(), v);
    ASSERT_EQ("small", v);
    inbl.clear();
    v.clear();

    ASSERT_TRUE(!j.read_entry(inbl, seq));

    j.make_writeable();
    j.close();
  }
}

TEST(TestFileJournal, ReplayCorrupt) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "true");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);

  vector<ObjectStore::Transaction> tls;
  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&wait_lock, &cond, &done));

    const char *needle =    "i am a needle";
    const char *newneedle = "in a haystack";
    bufferlist bl;
    bl.append(needle);
    int orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(1, bl, orig_len, gb.new_sub());
    bl.append(needle);
    orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(2, bl, orig_len, gb.new_sub());
    bl.append(needle);
    orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(3, bl, orig_len, gb.new_sub());
    bl.append(needle);
    orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(4, bl, orig_len, gb.new_sub());
    gb.activate();
    wait();

    j.close();

    cout << "corrupting journal" << std::endl;
    char buf[1024*128];
    int fd = open(path, O_RDONLY);
    ASSERT_GE(fd, 0);
    int r = safe_read_exact(fd, buf, sizeof(buf));
    ASSERT_EQ(0, r);
    int n = 0;
    for (unsigned o=0; o < sizeof(buf) - strlen(needle); o++) {
      if (memcmp(buf+o, needle, strlen(needle)) == 0) {
        if (n >= 2) {
	  cout << "replacing at offset " << o << std::endl;
	  memcpy(buf+o, newneedle, strlen(newneedle));
        } else {
	  cout << "leaving at offset " << o << std::endl;
        }
        n++;
      }
    }
    ASSERT_EQ(n, 4);
    close(fd);
    fd = open(path, O_WRONLY);
    ASSERT_GE(fd, 0);
    r = safe_write(fd, buf, sizeof(buf));
    ASSERT_EQ(r, 0);
    close(fd);

    j.open(1);

    bufferlist inbl;
    string v;
    uint64_t seq = 0;
    ASSERT_EQ(true, j.read_entry(inbl, seq));
    ASSERT_EQ(seq, 2ull);
    inbl.copy(0, inbl.length(), v);
    ASSERT_EQ(needle, v);
    inbl.clear();
    v.clear();
    bool corrupt;
    ASSERT_FALSE(j.read_entry(inbl, seq, &corrupt));
    ASSERT_TRUE(corrupt);

    j.make_writeable();
    j.close();
  }
}

TEST(TestFileJournal, WriteTrim) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "false");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);

  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    list<C_Sync*> ls;

    bufferlist bl;
    char foo[1024*1024];
    memset(foo, 1, sizeof(foo));

    uint64_t seq = 1, committed = 0;
    vector<ObjectStore::Transaction> tls;

    for (unsigned i=0; i<size_mb*2; i++) {
      bl.clear();
      bl.push_back(buffer::copy(foo, sizeof(foo)));
      bl.zero();
      ls.push_back(new C_Sync);
      int orig_len = j.prepare_entry(tls, &bl);
      j.reserve_throttle_and_backoff(bl.length());
      j.submit_entry(seq++, bl, orig_len, ls.back()->c);

      while (ls.size() > size_mb/2) {
        delete ls.front();
        ls.pop_front();
        committed++;
        j.committed_thru(committed);
      }
    }

    while (ls.size()) {
      delete ls.front();
      ls.pop_front();
      j.committed_thru(++committed);
    }

    ASSERT_TRUE(j.journalq_empty());

    j.close();
  }
}

TEST(TestFileJournal, WriteTrimSmall) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "false");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "0");
  g_ceph_context->_conf->apply_changes(NULL);
  vector<ObjectStore::Transaction> tls;

  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    list<C_Sync*> ls;

    bufferlist bl;
    char foo[1024*1024];
    memset(foo, 1, sizeof(foo));

    uint64_t seq = 1, committed = 0;

    for (unsigned i=0; i<size_mb*2; i++) {
      bl.clear();
      for (int k=0; k<128; k++)
        bl.push_back(buffer::copy(foo, sizeof(foo) / 128));
      bl.zero();
      ls.push_back(new C_Sync);
      int orig_len = j.prepare_entry(tls, &bl);
      j.reserve_throttle_and_backoff(bl.length());
      j.submit_entry(seq++, bl, orig_len, ls.back()->c);

      while (ls.size() > size_mb/2) {
        delete ls.front();
        ls.pop_front();
        committed++;
        j.committed_thru(committed);
      }
    }

    while (ls.size()) {
      delete ls.front();
      ls.pop_front();
      j.committed_thru(committed);
    }

    j.close();
  }
}

TEST(TestFileJournal, ReplayDetectCorruptFooterMagic) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "true");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "1");
  g_ceph_context->_conf->apply_changes(NULL);

  vector<ObjectStore::Transaction> tls;
  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&wait_lock, &cond, &done));

    const char *needle =    "i am a needle";
    for (unsigned i = 1; i <= 4; ++i) {
      bufferlist bl;
      bl.append(needle);
      int orig_len = j.prepare_entry(tls, &bl);
      j.reserve_throttle_and_backoff(bl.length());
      j.submit_entry(i, bl, orig_len, gb.new_sub());
    }
    gb.activate();
    wait();

    bufferlist bl;
    bl.append("needle");
    int orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(5, bl, orig_len, new C_SafeCond(&wait_lock, &cond, &done));
    wait();

    j.close();
    int fd = open(path, O_WRONLY);

    cout << "corrupting journal" << std::endl;
    j.open(0);
    j.corrupt_footer_magic(fd, 2);

    uint64_t seq = 0;
    bl.clear();
    bool corrupt = false;
    bool result = j.read_entry(bl, seq, &corrupt);
    ASSERT_TRUE(result);
    ASSERT_EQ(seq, 1UL);
    ASSERT_FALSE(corrupt);

    result = j.read_entry(bl, seq, &corrupt);
    ASSERT_FALSE(result);
    ASSERT_TRUE(corrupt);

    j.make_writeable();
    j.close();
    ::close(fd);
  }
}

TEST(TestFileJournal, ReplayDetectCorruptPayload) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "true");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "1");
  g_ceph_context->_conf->apply_changes(NULL);

  vector<ObjectStore::Transaction> tls;
  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&wait_lock, &cond, &done));

    const char *needle =    "i am a needle";
    for (unsigned i = 1; i <= 4; ++i) {
      bufferlist bl;
      bl.append(needle);
      int orig_len = j.prepare_entry(tls, &bl);
      j.reserve_throttle_and_backoff(bl.length());
      j.submit_entry(i, bl, orig_len, gb.new_sub());
    }
    gb.activate();
    wait();

    bufferlist bl;
    bl.append("needle");
    int orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(5, bl, orig_len, new C_SafeCond(&wait_lock, &cond, &done));
    wait();

    j.close();
    int fd = open(path, O_WRONLY);

    cout << "corrupting journal" << std::endl;
    j.open(0);
    j.corrupt_payload(fd, 2);

    uint64_t seq = 0;
    bl.clear();
    bool corrupt = false;
    bool result = j.read_entry(bl, seq, &corrupt);
    ASSERT_TRUE(result);
    ASSERT_EQ(seq, 1UL);
    ASSERT_FALSE(corrupt);

    result = j.read_entry(bl, seq, &corrupt);
    ASSERT_FALSE(result);
    ASSERT_TRUE(corrupt);

    j.make_writeable();
    j.close();
    ::close(fd);
  }
}

TEST(TestFileJournal, ReplayDetectCorruptHeader) {
  g_ceph_context->_conf->set_val("journal_ignore_corruption", "true");
  g_ceph_context->_conf->set_val("journal_write_header_frequency", "1");
  g_ceph_context->_conf->apply_changes(NULL);

  vector<ObjectStore::Transaction> tls;
  for (unsigned i = 0 ; i < 3; ++i) {
    SCOPED_TRACE(subtests[i].description);
    fsid.generate_random();
    FileJournal j(fsid, finisher, &sync_cond, path, subtests[i].directio,
		  subtests[i].aio, subtests[i].faio);
    ASSERT_EQ(0, j.create());
    j.make_writeable();

    C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&wait_lock, &cond, &done));

    const char *needle =    "i am a needle";
    for (unsigned i = 1; i <= 4; ++i) {
      bufferlist bl;
      bl.append(needle);
      int orig_len = j.prepare_entry(tls, &bl);
      j.reserve_throttle_and_backoff(bl.length());
      j.submit_entry(i, bl, orig_len, gb.new_sub());
    }
    gb.activate();
    wait();

    bufferlist bl;
    bl.append("needle");
    int orig_len = j.prepare_entry(tls, &bl);
    j.reserve_throttle_and_backoff(bl.length());
    j.submit_entry(5, bl, orig_len, new C_SafeCond(&wait_lock, &cond, &done));
    wait();

    j.close();
    int fd = open(path, O_WRONLY);

    cout << "corrupting journal" << std::endl;
    j.open(0);
    j.corrupt_header_magic(fd, 2);

    uint64_t seq = 0;
    bl.clear();
    bool corrupt = false;
    bool result = j.read_entry(bl, seq, &corrupt);
    ASSERT_TRUE(result);
    ASSERT_EQ(seq, 1UL);
    ASSERT_FALSE(corrupt);

    result = j.read_entry(bl, seq, &corrupt);
    ASSERT_FALSE(result);
    ASSERT_TRUE(corrupt);

    j.make_writeable();
    j.close();
    ::close(fd);
  }
}
