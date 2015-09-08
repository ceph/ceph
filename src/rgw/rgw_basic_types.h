#ifndef CEPH_RGW_BASIC_TYPES_H
#define CEPH_RGW_BASIC_TYPES_H

#include <string>

#include "include/types.h"


class rgw_bucket_namespace {
  std::string bns_id;

public:
  rgw_bucket_namespace()
    : bns_id(std::string()) {
  }

  rgw_bucket_namespace(const std::string b)
    : bns_id(b) {
  }

  std::string get_id() const {
    return bns_id;
  }

  bool operator!=(const rgw_bucket_namespace& rhs) const {
    return (bns_id != rhs.bns_id);
  }
  bool operator==(const rgw_bucket_namespace& rhs) const {
    return (bns_id == rhs.bns_id);
  }
  bool operator<(const rgw_bucket_namespace& rhs) const {
    return (bns_id < rhs.bns_id);
  }
};


struct rgw_user {
  std::string id;
  bool has_own_bns;

  rgw_user()
    : id(std::string()),
      has_own_bns(false) {
  }

  rgw_user(const std::string& s, const bool h = false)
    : has_own_bns(h) {
    from_str(s);
  }

  std::string get_id() const {
    return id;
  }

  /* Answer whether a given stored bucket entry points in his own
   * namespace or not. */
  rgw_bucket_namespace get_bns() const {
    if (has_own_bns) {
      return rgw_bucket_namespace(get_id());
    } else {
      return rgw_bucket_namespace();
    }
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 3, bl);
    ::encode(id, bl);
    ::encode(has_own_bns, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(3, bl);
    ::decode(id, bl);
    ::decode(has_own_bns, bl);
    DECODE_FINISH(bl);
  }

  void to_str(std::string& str) const {
    str = id;
  }

  void clear() {
    id.clear();
  }

  bool empty() {
    return id.empty();
  }

  string to_str() const {
    string s;
    to_str(s);
    return s;
  }

  void from_str(const std::string& str) {
    id = str;
  }

  rgw_user& operator=(const string& str) {
    from_str(str);
    return *this;
  }

  int compare(const rgw_user& u) const {
    return id.compare(u.id);
  }
  int compare(const string& str) const {
    rgw_user u(str);
    return compare(u);
  }

  bool operator!=(const rgw_user& rhs) const {
    return (compare(rhs) != 0);
  }
  bool operator==(const rgw_user& rhs) const {
    return (compare(rhs) == 0);
  }
  bool operator<(const rgw_user& rhs) const {
    return (id < rhs.id);
  }
};
WRITE_CLASS_ENCODER(rgw_user)


class JSONObj;

void decode_json_obj(rgw_user& val, JSONObj *obj);
void encode_json(const char *name, const rgw_user& val, Formatter *f);

inline ostream& operator<<(ostream& out, const rgw_user &u) {
  string s;
  u.to_str(s);
  return out << s;
}


#endif
