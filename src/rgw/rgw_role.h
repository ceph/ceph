#ifndef CEPH_RGW_ROLE_H
#define CEPH_RGW_ROLE_H

class RGWRole
{
  static const string role_name_oid_prefix;
  static const string role_oid_prefix;
  static const string role_path_oid_prefix;
  static const string role_arn_prefix;

  CephContext *cct;
  RGWRados *store;
  string id;
  string name;
  string path;
  string arn;
  string creation_date;
  string trust_policy;
  map<string, string> perm_policy_map;
  string uid;

  int store_info(bool exclusive);
  int store_name(bool exclusive);
  int store_path(bool exclusive);
  int read_id(const string& role_name, string& role_id);
  int read_name();
  int read_info();
  void set_id(const string& id) { this->id = id; }

public:
  RGWRole(CephContext *cct,
          RGWRados *store,
          string name,
          string path,
          string trust_policy,
          string uid)
  : cct(cct),
    store(store),
    name(std::move(name)),
    path(std::move(path)),
    trust_policy(std::move(trust_policy)),
    uid(std::move(uid)) {
    if (this->path.empty())
      this->path = "/";
  }

  RGWRole(CephContext *cct,
          RGWRados *store,
          string name)
  : cct(cct),
    store(store),
    name(std::move(name)) {}

  RGWRole(CephContext *cct,
          RGWRados *store)
  : cct(cct),
    store(store) {}

  RGWRole() {}

  ~RGWRole() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(id, bl);
    ::encode(name, bl);
    ::encode(path, bl);
    ::encode(arn, bl);
    ::encode(creation_date, bl);
    ::encode(trust_policy, bl);
    ::encode(perm_policy_map, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(id, bl);
    ::decode(name, bl);
    ::decode(path, bl);
    ::decode(arn, bl);
    ::decode(creation_date, bl);
    ::decode(trust_policy, bl);
    ::decode(perm_policy_map, bl);
    DECODE_FINISH(bl);
  }

  const string& get_id() const { return id; }
  const string& get_name() const { return name; }
  const string& get_path() const { return path; }
  const string& get_create_date() const { return creation_date; }

  int create(bool exclusive);
  int delete_obj();
  int get();
  int update();
  void update_trust_policy(string& trust_policy);
  void set_perm_policy(const string& policy_name, const string& perm_policy);
  vector<string> get_role_policy_names();
  int get_role_policy(const string& policy_name, string& perm_policy);
  int delete_policy(const string& policy_name);
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);

  static const string& get_names_oid_prefix();
  static const string& get_info_oid_prefix();
  static const string& get_path_oid_prefix();
  static int get_roles_by_path_prefix(RGWRados *store, CephContext *cct, const string& path_prefix, vector<RGWRole>& roles);
};
WRITE_CLASS_ENCODER(RGWRole)
#endif /* CEPH_RGW_ROLE_H */

