#ifndef CEPH_RGW_ROLE_H
#define CEPH_RGW_ROLE_H

class RGWRole
{
  CephContext *cct;
  RGWRados *store;
  string id;
  string name;
  string path;
  string creation_date;
  //TODO: Add trust policy here

  int store_info(bool exclusive);
  int store_name(bool exclusive);
  int read_id(const string& role_name, string& role_id);
  int read_name();
  int read_info();

public:
  RGWRole(CephContext *cct,
          RGWRados *store,
          string name,
          string path)
  : cct(cct),
    store(store),
    name(name),
    path(path) {
    if (this->path.empty())
      this->path = "/";
  }

  RGWRole(CephContext *cct,
          RGWRados *store,
          string name)
  : cct(cct),
    store(store),
    name(name) {}

  RGWRole() {}

  ~RGWRole() = default;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(id, bl);
    ::encode(name, bl);
    ::encode(path, bl);
    ::encode(creation_date, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(id, bl);
    ::decode(name, bl);
    ::decode(path, bl);
    ::decode(creation_date, bl);
    DECODE_FINISH(bl);
  }

  const string& get_id() { return id; }
  const string& get_name() { return name; }
  const string& get_path() { return path; }
  const string& get_create_date() { return creation_date; }

  int create(bool exclusive);
  int delete_obj();
  int list();
  void dump(Formatter *f) const;
  void decode_json(JSONObj *obj);
};
WRITE_CLASS_ENCODER(RGWRole)
#endif /* CEPH_RGW_ROLE_H */

