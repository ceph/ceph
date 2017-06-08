#ifndef CEPH_CLIENT_USERGROUPS_H
#define CEPH_CLIENT_USERGROUPS_H

class UserGroups {
public:
  virtual bool is_in(gid_t gid) = 0;
  virtual gid_t get_gid() = 0;
  virtual int get_gids(const gid_t **gids) = 0;
  virtual ~UserGroups() {};
};

#endif
