#ifndef CEPH_RGW_GROUP_H
#define CEPH_RGW_GROUP_H

class RGWGroup
{
  static const string group_name_oid_prefix;
  static const string group_oid_prefix;
  static const string group_path_oid_prefix;
  static const string group_arn_prefix;
  static constexpr int MAX_GROUP_NAME_LEN = 128;
  static constexpr int MAX_PATH_NAME_LEN = 512;

  CephContext *cct;
  RGWRados *store;
  string id;
  string name;
  string path;
  string arn;
  string creation_date;
  vector<string> users;
  string tenant;

  int store_info(bool exclusive);
  int store_name(bool exclusive);
  int store_path(bool exclusive);
  int read_id(const string& role_name, const string& tenant, string& role_id);
  int read_name();
  int read_info();
  void set_id(const string& id) { this->id = id; }
  bool validate_name(const string& name);
  bool validate_path(const string& path);
  bool validate_input();
  void extract_name_tenant(const string& str, string& tenant, string& name);

public:
  RGWGroup(CephContext *cct,
            RGWRados *store,
            string name,
            string path,
            string tenant)
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
            string name,
            string tenant)
  : cct(cct),
    store(store),
    name(std::move(name)),
    tenant(std::move(tenant)) {
    extract_name_tenant(this->name, this->tenant, this->name);
  }

  RGWGroup(CephContext *cct,
            RGWRados *store,
            string id)
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
    ::encode(id, bl);
    ::encode(name, bl);
    ::encode(path, bl);
    ::encode(arn, bl);
    ::encode(creation_date, bl);
    ::encode(users, bl);
    ::encode(tenant, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(id, bl);
    ::decode(name, bl);
    ::decode(path, bl);
    ::decode(arn, bl);
    ::decode(creation_date, bl);
    ::decode(users, bl);
    ::decode(tenant, bl);
    DECODE_FINISH(bl);
  }

  const string& get_id() const { return id; }
  const string& get_name() const { return name; }
  const string& get_path() const { return path; }
  const string& get_create_date() const { return creation_date; }
  const string& get_tenant() const { return tenant; }

  int create(bool exclusive);
  int delete_obj();
  int get();
  int get_by_id();
  int update();
  int update_name(const string& name);
  int update_path(const string& path);
  bool add_user(string& user);
  bool remove_user(string& user);
  void get_users(vector<string>& users);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static const string& get_names_oid_prefix();
  static const string& get_info_oid_prefix();
  static const string& get_path_oid_prefix();
  static int get_groups_by_path_prefix(RGWRados *store,
                                      CephContext *cct,
                                      const string& path_prefix,
                                      const string& tenant,
                                      vector<RGWGroup>& groups);
};
WRITE_CLASS_ENCODER(RGWGroup)
#endif /* CEPH_RGW_GROUP_H */

