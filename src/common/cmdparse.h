// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_COMMON_CMDPARSE_H
#define CEPH_COMMON_CMDPARSE_H

#include <string>
#include <sstream>
#include <map>
#include <boost/variant.hpp>
#include <vector>
#include <stdexcept>
#include "common/Formatter.h"
#include "common/BackTrace.h"

class CephContext;

/* this is handy; can't believe it's not standard */
#define ARRAY_SIZE(a)	(sizeof(a) / sizeof(*a))

typedef boost::variant<std::string, bool, int64_t, double, std::vector<std::string> > cmd_vartype;
typedef std::map<std::string, cmd_vartype> cmdmap_t;

void dump_cmd_to_json(ceph::Formatter *f, const std::string& cmd);
void dump_cmd_and_help_to_json(ceph::Formatter *f,
			       const std::string& secname,
			       const std::string& cmd,
			       const std::string& helptext);
void dump_cmddesc_to_json(ceph::Formatter *jf,
		          const std::string& secname,
		          const std::string& cmdsig,
		          const std::string& helptext,
		          const std::string& module,
		          const std::string& perm,
		          const std::string& avail);
bool cmdmap_from_json(std::vector<std::string> cmd, cmdmap_t *mapp,
		      std::stringstream &ss);
void handle_bad_get(CephContext *cct, std::string k, const char *name);

std::string cmd_vartype_stringify(const cmd_vartype& v);

template <typename T>
bool
cmd_getval(CephContext *cct, const cmdmap_t& cmdmap, std::string k, T& val)
{
  if (cmdmap.count(k)) {
    try {
      val = boost::get<T>(cmdmap.find(k)->second);
      return true;
    } catch (boost::bad_get) {
      handle_bad_get(cct, k, typeid(T).name());
    }
  }
  return false;
}

// with default

template <typename T>
void
cmd_getval(CephContext *cct, cmdmap_t& cmdmap, std::string k, T& val, T defval)
{
  if (!cmd_getval(cct, cmdmap, k, val))
    val = defval;
}

template <typename T>
void
cmd_putval(CephContext *cct, cmdmap_t& cmdmap, std::string k, T val)
{
  cmdmap[k] = val;
}
#endif
