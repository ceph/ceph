#ifndef TEST_CEPH_TIME_H
#define TEST_CEPH_TIME_H

#include <list>

#include "include/encoding.h"
#include "common/ceph_time.h"
#include "common/Formatter.h"

// wrapper for ceph::real_time that implements the dencoder interface
class real_time_wrapper {
  ceph::real_time t;
 public:
  real_time_wrapper() = default;
  explicit real_time_wrapper(const ceph::real_time& t) : t(t) {}

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(t, bl);
  }
  void decode(bufferlist::iterator &p) {
    using ceph::decode;
    decode(t, p);
  }
  void dump(Formatter* f) {
    auto epoch_time = ceph::real_clock::to_time_t(t);
    f->dump_string("time", std::ctime(&epoch_time));
  }
  static void generate_test_instances(std::list<real_time_wrapper*>& ls) {
    constexpr time_t t{455500800}; // Ghostbusters release date
    ls.push_back(new real_time_wrapper(ceph::real_clock::from_time_t(t)));
  }
};
WRITE_CLASS_ENCODER(real_time_wrapper)

// wrapper for ceph::timespan that implements the dencoder interface
class timespan_wrapper {
  ceph::timespan d;
 public:
  timespan_wrapper() = default;
  explicit timespan_wrapper(const ceph::timespan& d) : d(d) {}

  void encode(bufferlist& bl) const {
    using ceph::encode;
    encode(d, bl);
  }
  void decode(bufferlist::iterator &p) {
    using ceph::decode;
    decode(d, p);
  }
  void dump(Formatter* f) {
    f->dump_int("timespan", d.count());
  }
  static void generate_test_instances(std::list<timespan_wrapper*>& ls) {
    constexpr std::chrono::seconds d{7377}; // marathon world record (2:02:57)
    ls.push_back(new timespan_wrapper(d));
  }
};
WRITE_CLASS_ENCODER(timespan_wrapper)

#endif
