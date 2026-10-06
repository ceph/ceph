// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_COMMON_CMDPARSE_FWD_H
#define CEPH_COMMON_CMDPARSE_FWD_H

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>
#include <boost/variant/variant_fwd.hpp>

typedef boost::variant<std::string,
		       bool,
		       int64_t,
		       double,
		       std::vector<std::string>,
		       std::vector<int64_t>,
		       std::vector<double>>  cmd_vartype;
typedef std::map<std::string, cmd_vartype, std::less<>> cmdmap_t;

#endif
