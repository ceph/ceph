// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_COMMON_CMDPARSE_H
#define CEPH_COMMON_CMDPARSE_H

#include <vector>
#include <stdexcept>
#include <optional>
#include <ostream>
#include <boost/variant.hpp>
#include "include/ceph_assert.h"	// boost clobbers this
#include "include/common_fwd.h"
#include "common/Formatter.h"
#include "common/BackTrace.h"

typedef boost::variant<std::string,
		       bool,
		       int64_t,
		       double,
		       std::vector<std::string>,
		       std::vector<int64_t>,
		       std::vector<double>>  cmd_vartype;
typedef std::map<std::string, cmd_vartype, std::less<>> cmdmap_t;

namespace TOPNSPC::common {
std::string cmddesc_get_prefix(const std::string_view &cmddesc);
std::string cmddesc_get_prenautilus_compat(const std::string &cmddesc);
void dump_cmd_to_json(ceph::Formatter *f, uint64_t features,
                      const std::string& cmd);
void dump_cmd_and_help_to_json(ceph::Formatter *f,
			       uint64_t features,
			       const std::string& secname,
			       const std::string& cmd,
			       const std::string& helptext);
void dump_cmddesc_to_json(ceph::Formatter *jf,
		          uint64_t features,
		          const std::string& secname,
		          const std::string& cmdsig,
		          const std::string& helptext,
		          const std::string& module,
		          const std::string& perm,
		          uint64_t flags);
bool cmdmap_from_json(const std::vector<std::string>& cmd, cmdmap_t *mapp,
		      std::ostream& ss);
void cmdmap_dump(const cmdmap_t &cmdmap, ceph::Formatter *f);
void handle_bad_get(CephContext *cct, const std::string& k, const char *name);

std::string cmd_vartype_stringify(const cmd_vartype& v);

struct bad_cmd_get : public std::exception {
  std::string desc;
  bad_cmd_get(std::string_view f, const cmdmap_t& cmdmap) {
    desc += "bad or missing field '";
    desc += f;
    desc += "'";
  }
  const char *what() const throw() override {
    return desc.c_str();
  }
};

bool cmd_getval(const cmdmap_t& cmdmap,
		std::string_view k, bool& val);

bool cmd_getval_compat_cephbool(
  const cmdmap_t& cmdmap,
  const std::string& k, bool& val);

template <typename T>
bool cmd_getval(const cmdmap_t& cmdmap,
		std::string_view k, T& val)
{
  auto found = cmdmap.find(k);
  if (found == cmdmap.end()) {
    return false;
  }
  try {
    val = boost::get<T>(found->second);
    return true;
  } catch (boost::bad_get&) {
    throw bad_cmd_get(k, cmdmap);
  }
}

template <typename T>
std::optional<T> cmd_getval(const cmdmap_t& cmdmap,
			    std::string_view k)
{
  T ret;
  if (const bool found = cmd_getval(cmdmap, k, ret); found) {
    return std::make_optional(std::move(ret));
  } else {
    return std::nullopt;
  }
}

// with default

template <typename T, typename V>
T cmd_getval_or(const cmdmap_t& cmdmap, std::string_view k,
		const V& defval)
{
  auto found = cmdmap.find(k);
  if (found == cmdmap.end()) {
    return T(defval);
  }
  try {
    return boost::get<T>(cmdmap.find(k)->second);
  } catch (boost::bad_get&) {
    throw bad_cmd_get(k, cmdmap);
  }
}

template <typename T>
void
cmd_putval(CephContext *cct, cmdmap_t& cmdmap, std::string_view k, const T& val)
{
  cmdmap.insert_or_assign(std::string{k}, val);
}

bool validate_cmd(CephContext* cct,
		  const std::string& desc,
		  const cmdmap_t& cmdmap,
		  std::ostream& os);
extern int parse_osd_id(const char *s, std::ostream *pss);
extern long parse_pos_long(const char *s, std::ostream *pss = NULL);

}
#endif
