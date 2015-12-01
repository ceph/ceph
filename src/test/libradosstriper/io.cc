#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/radosstriper/libradosstriper.h"
#include "include/radosstriper/libradosstriper.hpp"
#include "test/librados/test.h"
#include "test/libradosstriper/TestCase.h"

#include <fcntl.h>
#include <errno.h>
#include "gtest/gtest.h"

using namespace librados;
using namespace libradosstriper;
using std::string;

TEST_F(StriperTest, SimpleWrite) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "SimpleWrite", buf, sizeof(buf), 0));
}

TEST_F(StriperTestPP, SimpleWritePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("SimpleWritePP", bl, sizeof(buf), 0));
}

TEST_F(StriperTest, SimpleWriteFull) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write_full(striper, "SimpleWrite", buf, sizeof(buf)));
}

TEST_F(StriperTestPP, SimpleWriteFullPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write_full("SimpleWritePP", bl));
}

TEST_F(StriperTest, Stat) {
  uint64_t psize;
  time_t pmtime;
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "Stat", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_striper_stat(striper, "Stat", &psize, &pmtime));
  ASSERT_EQ(psize, sizeof(buf));
  ASSERT_EQ(-ENOENT, rados_striper_stat(striper, "nonexistent", &psize, &pmtime));
}

TEST_F(StriperTestPP, StatPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("Statpp", bl, sizeof(buf), 0));
  uint64_t psize;
  time_t pmtime;
  ASSERT_EQ(0, striper.stat("Statpp", &psize, &pmtime));
  ASSERT_EQ(psize, sizeof(buf));
  ASSERT_EQ(-ENOENT, striper.stat("nonexistent", &psize, &pmtime));
}

TEST_F(StriperTest, RoundTrip) {
  char buf[128];
  char buf2[sizeof(buf)];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "RoundTrip", buf, sizeof(buf), 0));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_striper_read(striper, "RoundTrip", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
}

TEST_F(StriperTestPP, RoundTripPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("RoundTripPP", bl, sizeof(buf), 0));
  bufferlist cl;
  ASSERT_EQ((int)sizeof(buf), striper.read("RoundTripPP", &cl, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(buf, cl.c_str(), sizeof(buf)));
}

TEST_F(StriperTest, OverlappingWriteRoundTrip) {
  char buf[128];
  char buf2[64];
  char buf3[sizeof(buf)];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "OverlappingWriteRoundTrip", buf, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_striper_write(striper, "OverlappingWriteRoundTrip", buf2, sizeof(buf2), 0));
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf3), rados_striper_read(striper, "OverlappingWriteRoundTrip", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf2)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf2), buf, sizeof(buf) - sizeof(buf2)));
}

TEST_F(StriperTestPP, OverlappingWriteRoundTripPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("OverlappingWriteRoundTripPP", bl1, sizeof(buf), 0));
  char buf2[64];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, striper.write("OverlappingWriteRoundTripPP", bl2, sizeof(buf2), 0));
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf), striper.read("OverlappingWriteRoundTripPP", &bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  ASSERT_EQ(0, memcmp(bl3.c_str() + sizeof(buf2), buf, sizeof(buf) - sizeof(buf2)));
}

TEST_F(StriperTest, SparseWriteRoundTrip) {
  char buf[128];
  char buf2[2*sizeof(buf)];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "SparseWriteRoundTrip", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_striper_write(striper, "SparseWriteRoundTrip", buf, sizeof(buf), 1000000000));
  memset(buf2, 0xaa, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_striper_read(striper, "SparseWriteRoundTrip", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(0, memcmp(buf, buf2+sizeof(buf), sizeof(buf)));
  memset(buf2, 0xaa, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf), rados_striper_read(striper, "SparseWriteRoundTrip", buf2, sizeof(buf), 500000000));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
}

TEST_F(StriperTestPP, SparseWriteRoundTripPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("SparseWriteRoundTripPP", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, striper.write("SparseWriteRoundTripPP", bl1, sizeof(buf), 1000000000));
  bufferlist bl2;
  ASSERT_EQ((int)(2*sizeof(buf)), striper.read("SparseWriteRoundTripPP", &bl2, 2*sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl2.c_str(), buf, sizeof(buf)));
  memset(buf, 0, sizeof(buf));
  ASSERT_EQ(0, memcmp(bl2.c_str()+sizeof(buf), buf, sizeof(buf)));
  ASSERT_EQ((int)sizeof(buf), striper.read("SparseWriteRoundTripPP", &bl2, sizeof(buf), 500000000));
  ASSERT_EQ(0, memcmp(bl2.c_str(), buf, sizeof(buf)));
}

TEST_F(StriperTest, WriteFullRoundTrip) {
  char buf[128];
  char buf2[64];
  char buf3[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "WriteFullRoundTrip", buf, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_striper_write_full(striper, "WriteFullRoundTrip", buf2, sizeof(buf2)));
  memset(buf3, 0x00, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf2), rados_striper_read(striper, "WriteFullRoundTrip", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf2, buf3, sizeof(buf2)));
}

TEST_F(StriperTestPP, WriteFullRoundTripPP) {
  char buf[128];
  char buf2[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("WriteFullRoundTripPP", bl1, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, striper.write_full("WriteFullRoundTripPP", bl2));
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf2), striper.read("WriteFullRoundTripPP", &bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
}

TEST_F(StriperTest, AppendRoundTrip) {
  char buf[64];
  char buf2[64];
  char buf3[sizeof(buf) + sizeof(buf2)];
  memset(buf, 0xde, sizeof(buf));
  ASSERT_EQ(0, rados_striper_append(striper, "AppendRoundTrip", buf, sizeof(buf)));
  memset(buf2, 0xad, sizeof(buf2));
  ASSERT_EQ(0, rados_striper_append(striper, "AppendRoundTrip", buf2, sizeof(buf2)));
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf3), rados_striper_read(striper, "AppendRoundTrip", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf), buf2, sizeof(buf2)));
}

TEST_F(StriperTestPP, AppendRoundTripPP) {
  char buf[64];
  char buf2[64];
  memset(buf, 0xde, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.append("AppendRoundTripPP", bl1, sizeof(buf)));
  memset(buf2, 0xad, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, striper.append("AppendRoundTripPP", bl2, sizeof(buf2)));
  bufferlist bl3;
  ASSERT_EQ((int)(sizeof(buf) + sizeof(buf2)),
	    striper.read("AppendRoundTripPP", &bl3, (sizeof(buf) + sizeof(buf2)), 0));
  const char *bl3_str = bl3.c_str();
  ASSERT_EQ(0, memcmp(bl3_str, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(bl3_str + sizeof(buf), buf2, sizeof(buf2)));
}

TEST_F(StriperTest, TruncTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_striper_append(striper, "TruncTest", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_striper_trunc(striper, "TruncTest", sizeof(buf) / 2));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)(sizeof(buf)/2), rados_striper_read(striper, "TruncTest", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)/2));
}

TEST_F(StriperTestPP, TruncTestPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.append("TruncTestPP", bl, sizeof(buf)));
  ASSERT_EQ(0, striper.trunc("TruncTestPP", sizeof(buf) / 2));
  bufferlist bl2;
  ASSERT_EQ((int)(sizeof(buf)/2), striper.read("TruncTestPP", &bl2, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl2.c_str(), buf, sizeof(buf)/2));
}

TEST_F(StriperTest, TruncTestGrow) {
  char buf[128];
  char buf2[sizeof(buf)*2];
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_striper_append(striper, "TruncTestGrow", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_striper_trunc(striper, "TruncTestGrow", sizeof(buf2)));
  memset(buf2, 0xbb, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_striper_read(striper, "TruncTestGrow", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  memset(buf, 0x00, sizeof(buf));
  ASSERT_EQ(0, memcmp(buf, buf2+sizeof(buf), sizeof(buf)));
}

TEST_F(StriperTestPP, TruncTestGrowPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.append("TruncTestGrowPP", bl, sizeof(buf)));
  ASSERT_EQ(0, striper.trunc("TruncTestGrowPP", sizeof(buf) * 2));
  bufferlist bl2;
  ASSERT_EQ(sizeof(buf)*2, (unsigned)striper.read("TruncTestGrowPP", &bl2, sizeof(buf)*2, 0));
  ASSERT_EQ(0, memcmp(bl2.c_str(), buf, sizeof(buf)));
  memset(buf, 0x00, sizeof(buf));
  ASSERT_EQ(0, memcmp(bl2.c_str()+sizeof(buf), buf, sizeof(buf)));
}

TEST_F(StriperTest, RemoveTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "RemoveTest", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_striper_remove(striper, "RemoveTest"));
  ASSERT_EQ(-ENOENT, rados_striper_read(striper, "RemoveTest", buf2, sizeof(buf2), 0));
}

TEST_F(StriperTestPP, RemoveTestPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("RemoveTestPP", bl, sizeof(buf), 0));
  ASSERT_EQ(0, striper.remove("RemoveTestPP"));
  bufferlist bl2;
  ASSERT_EQ(-ENOENT, striper.read("RemoveTestPP", &bl2, sizeof(buf), 0));
}

TEST_F(StriperTest, XattrsRoundTrip) {
  char buf[128];
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "XattrsRoundTrip", buf, sizeof(buf), 0));
  ASSERT_EQ(-ENODATA, rados_striper_getxattr(striper, "XattrsRoundTrip", "attr1", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_striper_setxattr(striper, "XattrsRoundTrip", "attr1", attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ((int)sizeof(attr1_buf), rados_striper_getxattr(striper, "XattrsRoundTrip", "attr1", buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(attr1_buf, buf, sizeof(attr1_buf)));
}

TEST_F(StriperTestPP, XattrsRoundTripPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("XattrsRoundTripPP", bl1, sizeof(buf), 0));
  char attr1_buf[] = "foo bar baz";
  bufferlist bl2;
  ASSERT_EQ(-ENODATA, striper.getxattr("XattrsRoundTripPP", "attr1", bl2));
  bufferlist bl3;
  bl3.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, striper.setxattr("XattrsRoundTripPP", "attr1", bl3));
  bufferlist bl4;
  ASSERT_EQ((int)sizeof(attr1_buf), striper.getxattr("XattrsRoundTripPP", "attr1", bl4));
  ASSERT_EQ(0, memcmp(bl4.c_str(), attr1_buf, sizeof(attr1_buf)));
}

TEST_F(StriperTest, RmXattr) {
  char buf[128];
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "RmXattr", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_striper_setxattr(striper, "RmXattr", "attr1", attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ(0, rados_striper_rmxattr(striper, "RmXattr", "attr1"));
  ASSERT_EQ(-ENODATA, rados_striper_getxattr(striper, "RmXattr", "attr1", buf, sizeof(buf)));
}

TEST_F(StriperTestPP, RmXattrPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("RmXattrPP", bl1, sizeof(buf), 0));
  char attr1_buf[] = "foo bar baz";
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, striper.setxattr("RmXattrPP", "attr1", bl2));
  ASSERT_EQ(0, striper.rmxattr("RmXattrPP", "attr1"));
  bufferlist bl3;
  ASSERT_EQ(-ENODATA, striper.getxattr("RmXattrPP", "attr1", bl3));
}

TEST_F(StriperTest, XattrIter) {
  char buf[128];
  char attr1_buf[] = "foo bar baz";
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_striper_write(striper, "RmXattr", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_striper_setxattr(striper, "RmXattr", "attr1", attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ(0, rados_striper_setxattr(striper, "RmXattr", "attr2", attr2_buf, sizeof(attr2_buf)));
  rados_xattrs_iter_t iter;
  ASSERT_EQ(0, rados_striper_getxattrs(striper, "RmXattr", &iter));
  int num_seen = 0;
  while (true) {
    const char *name;
    const char *val;
    size_t len;
    ASSERT_EQ(0, rados_striper_getxattrs_next(iter, &name, &val, &len));
    if (name == NULL) {
      break;
    }
    ASSERT_LT(num_seen, 2) << "Extra attribute : " << name;
    if ((strcmp(name, "attr1") == 0) && (val != NULL) && (memcmp(val, attr1_buf, len) == 0)) {
      num_seen++;
      continue;
    }
    else if ((strcmp(name, "attr2") == 0) && (val != NULL) && (memcmp(val, attr2_buf, len) == 0)) {
      num_seen++;
      continue;
    }
    else {
      ASSERT_EQ(0, 1) << "Unexpected attribute : " << name;;
    }
  }
  rados_striper_getxattrs_end(iter);
}

TEST_F(StriperTestPP, XattrListPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, striper.write("RmXattrPP", bl1, sizeof(buf), 0));
  char attr1_buf[] = "foo bar baz";
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, striper.setxattr("RmXattrPP", "attr1", bl2));
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  bufferlist bl3;
  bl3.append(attr2_buf, sizeof(attr2_buf));
  ASSERT_EQ(0, striper.setxattr("RmXattrPP", "attr2", bl3));
  std::map<std::string, bufferlist> attrset;
  ASSERT_EQ(0, striper.getxattrs("RmXattrPP", attrset));
  for (std::map<std::string, bufferlist>::iterator i = attrset.begin();
       i != attrset.end(); ++i) {
    if (i->first == string("attr1")) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr1_buf, sizeof(attr1_buf)));
    }
    else if (i->first == string("attr2")) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr2_buf, sizeof(attr2_buf)));
    }
    else {
      ASSERT_EQ(0, 1) << "Unexpected attribute : " << i->first;
    }
  }
}
