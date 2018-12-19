// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_COMMON_CMDPARSE_H
#define CEPH_COMMON_CMDPARSE_H

#include <vector>
#include <stdexcept>
#include <ostream>
#include <boost/variant.hpp>
#include "include/ceph_assert.h"	// boost clobbers this
#include "common/Formatter.h"
#include "common/BackTrace.h"

class CephContext;

typedef boost::variant<std::string,
		       bool,
		       int64_t,
		       double,
		       std::vector<std::string>,
		       std::vector<int64_t>,
		       std::vector<double>>  cmd_vartype;
typedef std::map<std::string, cmd_vartype, std::less<>> cmdmap_t;

std::string cmddesc_get_prefix(const std::string &cmddesc);
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
bool cmdmap_from_json(std::vector<std::string> cmd, cmdmap_t *mapp,
		      std::stringstream &ss);
void cmdmap_dump(const cmdmap_t &cmdmap, ceph::Formatter *f);
void handle_bad_get(CephContext *cct, const std::string& k, const char *name);

std::string cmd_vartype_stringify(const cmd_vartype& v);

struct bad_cmd_get : public std::exception {
  std::string desc;
  bad_cmd_get(const std::string& f, const cmdmap_t& cmdmap) {
    desc = "bad or missing field '" + f + "'";
  }
  const char *what() const throw() override {
    return desc.c_str();
  }
};

bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
		const std::string& k, bool& val);

template <typename T>
bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
		const std::string& k, T& val)
{
  if (cmdmap.count(k)) {
    try {
      val = boost::get<T>(cmdmap.find(k)->second);
      return true;
    } catch (boost::bad_get&) {
      throw bad_cmd_get(k, cmdmap);
    }
  }
  return false;
}

// with default

template <typename T>
bool cmd_getval(
  CephContext *cct, const cmdmap_t& cmdmap, const std::string& k,
  T& val, const T& defval)
{
  if (cmdmap.count(k)) {
    try {
      val = boost::get<T>(cmdmap.find(k)->second);
      return true;
    } catch (boost::bad_get&) {
      throw bad_cmd_get(k, cmdmap);
    }
  } else {
    val = defval;
    return true;
  }
}

template <typename T>
void
cmd_putval(CephContext *cct, cmdmap_t& cmdmap, const std::string& k, const T& val)
{
  cmdmap[k] = val;
}

bool validate_cmd(CephContext* cct,
		  const std::string& desc,
		  const cmdmap_t& cmdmap,
		  std::ostream& os);
extern int parse_osd_id(const char *s, std::ostream *pss);
extern long parse_pos_long(const char *s, std::ostream *pss = NULL);

#endif
