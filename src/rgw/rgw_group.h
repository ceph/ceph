#ifndef CEPH_RGW_GROUP_H
#define CEPH_RGW_GROUP_H

#include <string>
#include <vector>

#include "include/encoding.h"

class CephContext;

class RGWRados;

class RGWGroup
{
  static const std::string group_name_oid_prefix;
  static const std::string group_oid_prefix;
  static const std::string group_path_oid_prefix;
  static constexpr int MAX_GROUP_NAME_LEN = 128;
  static constexpr int MAX_PATH_NAME_LEN = 512;

  CephContext *cct;
  RGWRados *store;
  std::string id;
  std::string name;
  std::string path;
  std::string creation_date;
  std::vector<std::string> users;
  std::string tenant;

  int store_info(bool exclusive);
  int store_name(bool exclusive);
  int store_path(bool exclusive);
  int read_id(const std::string& role_name, const std::string& tenant, std::string& role_id);
  int read_name();
  int read_info();
  void set_id(const std::string& id) { this->id = id; }
  bool validate_name(const std::string& name);
  bool validate_path(const std::string& path);
  bool validate_input();
  void extract_name_tenant(const std::string& str, std::string& tenant, std::string& name);

public:
  RGWGroup(CephContext *cct,
            RGWRados *store,
            std::string name,
            std::string path,
            std::string tenant)
  : cct(cct),
    store(store),
    name(std::move(name)),
    path(std::move(path)),
    tenant(std::move(tenant)) {
    if (this->path.empty())
      this->path = "/";
    extract_name_tenant(this->name, this->tenant, this->name);
  }

  RGWGroup(CephContext *cct,
            RGWRados *store,
            std::string name,
            std::string tenant)
  : cct(cct),
    store(store),
    name(std::move(name)),
    tenant(std::move(tenant)) {
    extract_name_tenant(this->name, this->tenant, this->name);
  }

  RGWGroup(CephContext *cct,
            RGWRados *store,
            std::string id)
  : cct(cct),
    store(store),
    id(std::move(id)) {}

  RGWGroup(CephContext *cct,
            RGWRados *store)
  : cct(cct),
    store(store) {}

  RGWGroup() {}

  ~RGWGroup() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(id, bl);
    encode(name, bl);
    encode(path, bl);
    encode(creation_date, bl);
    encode(users, bl);
    encode(tenant, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    decode(id, bl);
    decode(name, bl);
    decode(path, bl);
    decode(creation_date, bl);
    decode(users, bl);
    decode(tenant, bl);
    DECODE_FINISH(bl);
  }

  const std::string& get_id() const { return id; }
  const std::string& get_name() const { return name; }
  const std::string& get_path() const { return path; }
  const std::string& get_create_date() const { return creation_date; }
  const std::string& get_tenant() const { return tenant; }

  int create(bool exclusive);
  int delete_obj();
  int get();
  int get_by_id();
  int update();
  int update_name(const std::string& name);
  int update_path(const std::string& path);
  bool add_user(const std::string& user);
  bool remove_user(const std::string& user);
  void get_users(std::vector<std::string>& users);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static const std::string& get_names_oid_prefix();
  static const std::string& get_info_oid_prefix();
  static const std::string& get_path_oid_prefix();
  static int get_groups_by_path_prefix(RGWRados *store,
                                      CephContext *cct,
                                      const std::string& path_prefix,
                                      const std::string& tenant,
                                      std::vector<RGWGroup>& groups);
};
WRITE_CLASS_ENCODER(RGWGroup)
#endif /* CEPH_RGW_GROUP_H */

