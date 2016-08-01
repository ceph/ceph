#ifndef _CEPH_UUID_H
#define _CEPH_UUID_H

/*
 * Thin C++ wrapper around libuuid.
 */

#include "encoding.h"
#include <ostream>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/random/random_device.hpp>

struct uuid_d {
  boost::uuids::uuid uuid;

  uuid_d() {
    boost::uuids::nil_generator gen;
    uuid = gen();
  }

  bool is_zero() const {
    return uuid.is_nil();
  }

  void generate_random() {
    boost::random::random_device rng("/dev/urandom");
    boost::uuids::basic_random_generator<boost::random::random_device> gen(&rng);
    uuid = gen();
  }
  
  bool parse(const char *s) {
    try {
      boost::uuids::string_generator gen;
      uuid = gen(s);
      return true;
    } catch (std::runtime_error& e) {
      return false;
    }
  }
  void print(char *s) const {
    memcpy(s, boost::uuids::to_string(uuid).c_str(), 37);
  }

 std::string to_string() const {
    return boost::uuids::to_string(uuid);
  }

  char *bytes() const {
    return (char*)uuid.data;
  }

  void encode(bufferlist& bl) const {
    ::encode_raw(uuid, bl);
  }

  void decode(bufferlist::iterator& p) const {
    ::decode_raw(uuid, p);
  }
};
WRITE_CLASS_ENCODER(uuid_d)

inline std::ostream& operator<<(std::ostream& out, const uuid_d& u) {
  char b[37];
  u.print(b);
  return out << b;
}

inline bool operator==(const uuid_d& l, const uuid_d& r) {
  return l.uuid == r.uuid;
}
inline bool operator!=(const uuid_d& l, const uuid_d& r) {
  return l.uuid != r.uuid;
}


#endif
