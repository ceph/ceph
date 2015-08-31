// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/radosstriper/libradosstriper.h"
#include "include/radosstriper/libradosstriper.hpp"
#include "include/ceph_fs.h"
#include "test/librados/test.h"
#include "test/libradosstriper/TestCase.h"

#include <string>
#include <errno.h>
using namespace librados;
using namespace libradosstriper;

class StriperTestRT : public StriperTestParam {
public:
  StriperTestRT() : StriperTestParam() {}
protected:
  char* getObjName(const std::string& soid, uint64_t nb)
  {
    char name[soid.size()+18];
    sprintf(name, "%s.%016llx", soid.c_str(), (long long unsigned int)nb);
    return strdup(name);
  }
  
  void checkObjectFromRados(const std::string& soid, bufferlist &bl,
                            uint64_t exp_stripe_unit, uint64_t exp_stripe_count,
                            uint64_t exp_object_size, size_t size)
  {
    checkObjectFromRados(soid, bl, exp_stripe_unit, exp_stripe_count, exp_object_size, size, size);
  }
      
  void checkObjectFromRados(const std::string& soid, bufferlist &bl,
                            uint64_t exp_stripe_unit, uint64_t exp_stripe_count,
                            uint64_t exp_object_size, size_t size,
                            size_t actual_size_if_sparse)
  {
    // checking first object's rados xattrs
    bufferlist xattrbl;
    char* firstOid = getObjName(soid, 0);
    ASSERT_LT(0, ioctx.getxattr(firstOid, "striper.layout.stripe_unit", xattrbl));
    std::string s_xattr(xattrbl.c_str(), xattrbl.length()); // adds 0 byte at the end
    uint64_t stripe_unit = strtoll(s_xattr.c_str(), NULL, 10);
    ASSERT_LT((unsigned)0, stripe_unit);
    ASSERT_EQ(stripe_unit, exp_stripe_unit);
    ASSERT_LT(0, ioctx.getxattr(firstOid, "striper.layout.stripe_count", xattrbl));
    s_xattr = std::string(xattrbl.c_str(), xattrbl.length()); // adds 0 byte at the end
    uint64_t stripe_count = strtoll(s_xattr.c_str(), NULL, 10);
    ASSERT_LT(0U, stripe_count);
    ASSERT_EQ(stripe_count, exp_stripe_count);
    ASSERT_LT(0, ioctx.getxattr(firstOid, "striper.layout.object_size", xattrbl));
    s_xattr = std::string(xattrbl.c_str(), xattrbl.length()); // adds 0 byte at the end
    uint64_t object_size = strtoll(s_xattr.c_str(), NULL, 10);
    ASSERT_EQ(object_size, exp_object_size);
    ASSERT_LT(0, ioctx.getxattr(firstOid, "striper.size", xattrbl));
    s_xattr = std::string(xattrbl.c_str(), xattrbl.length()); // adds 0 byte at the end
    uint64_t xa_size = strtoll(s_xattr.c_str(), NULL, 10);
    ASSERT_EQ(xa_size, size);
    // checking object content from rados point of view
    // we will go stripe by stripe, read the content of each of them and
    // check with expectations
    uint64_t stripe_per_object = object_size / stripe_unit;
    uint64_t stripe_per_objectset = stripe_per_object * stripe_count;
    uint64_t nb_stripes_in_object = (size+stripe_unit-1)/stripe_unit;
    for (uint64_t stripe_nb = 0;
         stripe_nb < nb_stripes_in_object;
         stripe_nb++) {
      // find out where this stripe is stored
      uint64_t objectset = stripe_nb / stripe_per_objectset;
      uint64_t stripe_in_object_set = stripe_nb % stripe_per_objectset;
      uint64_t object_in_set = stripe_in_object_set % stripe_count;
      uint64_t stripe_in_object = stripe_in_object_set / stripe_count;
      uint64_t object_nb = objectset * stripe_count + object_in_set;
      uint64_t start = stripe_in_object * stripe_unit;
      uint64_t len = stripe_unit;
      if (stripe_nb == nb_stripes_in_object-1 and size % stripe_unit != 0) {
        len = size % stripe_unit;
      }
      // handle case of sparse object (can only be sparse at the end in our tests)
      if (actual_size_if_sparse < size and
          ((actual_size_if_sparse+stripe_unit-1)/stripe_unit)-1 == stripe_nb) {
        len = actual_size_if_sparse % stripe_unit;
        if (0 == len) len = stripe_unit;
      }
      bufferlist stripe_data;
      // check object content
      char* oid = getObjName(soid, object_nb);
      int rc = ioctx.read(oid, stripe_data, len, start);
      if (actual_size_if_sparse < size and
          (actual_size_if_sparse+stripe_unit-1)/stripe_unit <= stripe_nb) {
        // sparse object case : the stripe does not exist, but the rados object may
        uint64_t object_start = (object_in_set + objectset*stripe_per_objectset) * stripe_unit;
        if (actual_size_if_sparse <= object_start) {
          ASSERT_EQ(rc, -ENOENT);
        } else {
          ASSERT_EQ(rc, 0);
        }
      } else {
        ASSERT_EQ((uint64_t)rc, len);
        bufferlist original_data;
        original_data.substr_of(bl, stripe_nb*stripe_unit, len);
        ASSERT_EQ(0, memcmp(original_data.c_str(), stripe_data.c_str(), len));
      }
      free(oid);
    }
    // checking rados object sizes; we go object by object
    uint64_t nb_full_object_sets = nb_stripes_in_object / stripe_per_objectset;
    uint64_t nb_extra_objects = nb_stripes_in_object % stripe_per_objectset;
    if (nb_extra_objects > stripe_count) nb_extra_objects = stripe_count;
    uint64_t nb_objects = nb_full_object_sets * stripe_count + nb_extra_objects;
    for (uint64_t object_nb = 0; object_nb < nb_objects; object_nb++) {
      uint64_t rados_size;
      time_t mtime;
      char* oid = getObjName(soid, object_nb);
      uint64_t nb_full_object_set = object_nb / stripe_count;
      uint64_t object_index_in_set = object_nb % stripe_count;
      uint64_t object_start_stripe = nb_full_object_set * stripe_per_objectset + object_index_in_set;
      uint64_t object_start_off = object_start_stripe * stripe_unit;
      if (actual_size_if_sparse < size and actual_size_if_sparse <= object_start_off) {
        ASSERT_EQ(-ENOENT, ioctx.stat(oid, &rados_size, &mtime));
      } else {
        ASSERT_EQ(0, ioctx.stat(oid, &rados_size, &mtime));
        uint64_t offset;
        uint64_t stripe_size = stripe_count * stripe_unit;
        uint64_t set_size = stripe_count * object_size;
        uint64_t len = 0;
        for (offset = object_start_off;
             (offset < (object_start_off) + set_size) && (offset < actual_size_if_sparse);
             offset += stripe_size) {
          if (offset + stripe_unit > actual_size_if_sparse) {
            len += actual_size_if_sparse-offset;
          } else {
            len += stripe_unit;
          }
        }
        ASSERT_EQ(len, rados_size);
      }
      free(oid);
    }
    // check we do not have an extra object behind
    uint64_t rados_size;
    time_t mtime;
    char* oid = getObjName(soid, nb_objects);
    ASSERT_EQ(-ENOENT, ioctx.stat(oid, &rados_size, &mtime));
    free(oid);
    free(firstOid);
  }
};
  
TEST_P(StriperTestRT, StripedRoundtrip) {
  // get striping parameters and apply them
  TestData testData = GetParam();
  ASSERT_EQ(0, striper.set_object_layout_stripe_unit(testData.stripe_unit));
  ASSERT_EQ(0, striper.set_object_layout_stripe_count(testData.stripe_count));
  ASSERT_EQ(0, striper.set_object_layout_object_size(testData.object_size));
  std::ostringstream oss;
  oss << "StripedRoundtrip_" << testData.stripe_unit << "_"
      << testData.stripe_count << "_" << testData.object_size
      << "_" << testData.size;
  std::string soid = oss.str();
  // writing striped data
  bufferlist bl1;
  {
    SCOPED_TRACE("Writing initial object"); 
    char buf[testData.size];
    for (unsigned int i = 0; i < testData.size; i++) buf[i] = 13*((unsigned char)i);
    bl1.append(buf, testData.size);
    ASSERT_EQ(0, striper.write(soid, bl1, testData.size, 0));
    // checking object state from Rados point of view
    ASSERT_NO_FATAL_FAILURE(checkObjectFromRados(soid, bl1, testData.stripe_unit,
                                                 testData.stripe_count, testData.object_size,
                                                 testData.size));
  }
  // adding more data to object and checking again
  bufferlist bl2;
  {
    SCOPED_TRACE("Testing append");
    char buf2[testData.size];
    for (unsigned int i = 0; i < testData.size; i++) buf2[i] = 17*((unsigned char)i);
    bl2.append(buf2, testData.size);
    ASSERT_EQ(0, striper.append(soid, bl2, testData.size));
    bl1.append(buf2, testData.size);
    ASSERT_NO_FATAL_FAILURE(checkObjectFromRados(soid, bl1, testData.stripe_unit,
                                                 testData.stripe_count, testData.object_size,
                                                 testData.size*2));
  }
  // truncating to half original size and checking again
  {
    SCOPED_TRACE("Testing trunc to truncate object");
    ASSERT_EQ(0, striper.trunc(soid, testData.size/2));
    ASSERT_NO_FATAL_FAILURE(checkObjectFromRados(soid, bl1, testData.stripe_unit,
                                                 testData.stripe_count, testData.object_size,
                                                 testData.size/2));
  }
  // truncating back to original size and checking again (especially for 0s)
  {
    SCOPED_TRACE("Testing trunc to extend object with 0s");
    ASSERT_EQ(0, striper.trunc(soid, testData.size));
    bufferlist bl3;
    bl3.substr_of(bl1, 0, testData.size/2);
    bl3.append_zero(testData.size - testData.size/2);
    ASSERT_NO_FATAL_FAILURE(checkObjectFromRados(soid, bl3, testData.stripe_unit,
                                                 testData.stripe_count, testData.object_size,
                                                 testData.size, testData.size/2));
  }
  {
    SCOPED_TRACE("Testing write_full");
    // using write_full and checking again
    ASSERT_EQ(0, striper.write_full(soid, bl2));
    checkObjectFromRados(soid, bl2, testData.stripe_unit,
                         testData.stripe_count, testData.object_size,
                         testData.size);
  }
  {
    SCOPED_TRACE("Testing standard remove");
    // call remove
    ASSERT_EQ(0, striper.remove(soid));
    // check that the removal was successful
    uint64_t size;
    time_t mtime;   
    for (uint64_t object_nb = 0;
         object_nb < testData.size*2/testData.object_size + testData.stripe_count;
         object_nb++) {
      ASSERT_EQ(-ENOENT, ioctx.stat(getObjName(soid, object_nb), &size, &mtime));
    }
  }
  {
    SCOPED_TRACE("Testing remove when no object size");
    // recreate object
    ASSERT_EQ(0, striper.write(soid, bl1, testData.size*2, 0));
    // remove the object size attribute from the striped object
    char* firstOid = getObjName(soid, 0);
    ASSERT_EQ(0, ioctx.rmxattr(firstOid, "striper.size"));
    // check that stat fails
    uint64_t size;
    time_t mtime;   
    ASSERT_EQ(-ENODATA, striper.stat(soid, &size, &mtime));
    // call remove
    ASSERT_EQ(0, striper.remove(soid));
    // check that the removal was successful
    for (uint64_t object_nb = 0;
         object_nb < testData.size*2/testData.object_size + testData.stripe_count;
         object_nb++) {
      ASSERT_EQ(-ENOENT, ioctx.stat(getObjName(soid, object_nb), &size, &mtime));
    }
    free(firstOid);
  }
}

const TestData simple_stripe_schemes[] = {
  // stripe_unit,        stripe_count, object_size,            size
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   2},
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 5,            CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 5,            3*CEPH_MIN_STRIPE_UNIT, 2*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 5,            3*CEPH_MIN_STRIPE_UNIT, 8*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 5,            3*CEPH_MIN_STRIPE_UNIT, 12*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 5,            3*CEPH_MIN_STRIPE_UNIT, 15*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 5,            3*CEPH_MIN_STRIPE_UNIT, 25*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 5,            3*CEPH_MIN_STRIPE_UNIT, 45*CEPH_MIN_STRIPE_UNIT+100},
  {262144,               5,            262144,                 2},
  {262144,               5,            262144,                 262144},
  {262144,               5,            262144,                 262144-1},
  {262144,               5,            262144,                 2*262144},
  {262144,               5,            262144,                 12*262144},
  {262144,               5,            262144,                 2*262144-1},
  {262144,               5,            262144,                 12*262144-1},
  {262144,               5,            262144,                 2*262144+100},
  {262144,               5,            262144,                 12*262144+100},
  {262144,               5,            3*262144,               2*262144+100},
  {262144,               5,            3*262144,               8*262144+100},
  {262144,               5,            3*262144,               12*262144+100},
  {262144,               5,            3*262144,               15*262144+100},
  {262144,               5,            3*262144,               25*262144+100},
  {262144,               5,            3*262144,               45*262144+100},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   2},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 1,            CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 1,            3*CEPH_MIN_STRIPE_UNIT, 2*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 1,            3*CEPH_MIN_STRIPE_UNIT, 8*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 1,            3*CEPH_MIN_STRIPE_UNIT, 12*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 1,            3*CEPH_MIN_STRIPE_UNIT, 15*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 1,            3*CEPH_MIN_STRIPE_UNIT, 25*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 1,            3*CEPH_MIN_STRIPE_UNIT, 45*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   2},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT-1},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   2*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 50,           CEPH_MIN_STRIPE_UNIT,   12*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 50,           3*CEPH_MIN_STRIPE_UNIT, 2*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 50,           3*CEPH_MIN_STRIPE_UNIT, 8*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 50,           3*CEPH_MIN_STRIPE_UNIT, 12*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 50,           3*CEPH_MIN_STRIPE_UNIT, 15*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 50,           3*CEPH_MIN_STRIPE_UNIT, 25*CEPH_MIN_STRIPE_UNIT+100},
  {CEPH_MIN_STRIPE_UNIT, 50,           3*CEPH_MIN_STRIPE_UNIT, 45*CEPH_MIN_STRIPE_UNIT+100}
};

INSTANTIATE_TEST_CASE_P(SimpleStriping,
                        StriperTestRT,
                        ::testing::ValuesIn(simple_stripe_schemes));
