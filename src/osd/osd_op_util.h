// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>
#include <string>

#include "osd/OSDMap.h"

#include "messages/MOSDOp.h"

class OpInfo {
public:
  struct ClassInfo {
    ClassInfo(std::string&& class_name, std::string&& method_name,
              bool read, bool write, bool allowed) :
      class_name(std::move(class_name)), method_name(std::move(method_name)),
      read(read), write(write), allowed(allowed)
    {}
    const std::string class_name;
    const std::string method_name;
    const bool read, write, allowed;
  };

private:
  uint64_t rmw_flags = 0;
  std::vector<ClassInfo> classes;

  void set_rmw_flags(int flags);

  void add_class(std::string&& class_name, std::string&& method_name,
                 bool read, bool write, bool allowed) {
    classes.emplace_back(std::move(class_name), std::move(method_name),
                          read, write, allowed);
  }

public:

  void clear() {
    rmw_flags = 0;
  }

  uint64_t get_flags() const {
    return rmw_flags;
  }

  bool check_rmw(int flag) const ;
  bool may_read() const;
  bool may_read_data() const;
  bool may_write() const;
  bool may_cache() const;
  bool rwordered_forced() const;
  bool rwordered() const;
  bool includes_pg_op() const;
  bool need_read_cap() const;
  bool need_write_cap() const;
  bool need_promote() const;
  bool need_skip_handle_cache() const;
  bool need_skip_promote() const;
  bool allows_returnvec() const;

  void set_read();
  void set_write();
  void set_cache();
  void set_class_read();
  void set_class_write();
  void set_pg_op();
  void set_promote();
  void set_skip_handle_cache();
  void set_skip_promote();
  void set_force_rwordered();
  void set_returnvec();
  void set_read_data();

  int set_from_op(
    const MOSDOp *m,
    const OSDMap &osdmap);
  int set_from_op(
    const std::vector<OSDOp> &ops,
    const pg_t &pg,
    const OSDMap &osdmap);

  std::vector<ClassInfo> get_classes() const {
    return classes;
  }
};

std::ostream& operator<<(std::ostream& out, const OpInfo::ClassInfo& i);
