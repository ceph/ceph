// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/cmdparse.h"
#include "common/Formatter.h"
#include "common/debug.h"
#include "common/strtol.h"
#include "json_spirit/json_spirit.h"

/**
 * Given a cmddesc like "foo baz name=bar,type=CephString",
 * return the prefix "foo baz".
 */
std::string cmddesc_get_prefix(const std::string &cmddesc)
{
  stringstream ss(cmddesc);
  std::string word;
  std::ostringstream result;
  bool first = true;
  while (std::getline(ss, word, ' ')) {
    if (word.find_first_of(",=") != string::npos) {
      break;
    }

    if (!first) {
      result << " ";
    }
    result << word;
    first = false;
  }

  return result.str();
}

using arg_desc_t = std::map<std::string_view, std::string_view>;

// Snarf up all the key=val,key=val pairs, put 'em in a dict.
template<class String>
arg_desc_t cmddesc_get_args(const String& cmddesc)
{
  arg_desc_t arg_desc;
  for_each_substr(cmddesc, ",", [&](auto kv) {
      // key=value; key by itself implies value is bool true
      // name="name" means arg dict will be titled 'name'
      auto equal = kv.find('=');
      if (equal == kv.npos) {
	// it should be the command
	return;
      }
      auto key = kv.substr(0, equal);
      auto val = kv.substr(equal + 1);
      arg_desc[key] = val;
    });
  return arg_desc;
}

std::string cmddesc_get_prenautilus_compat(const std::string &cmddesc)
{
  std::vector<std::string> out;
  stringstream ss(cmddesc);
  std::string word;
  bool changed = false;
  while (std::getline(ss, word, ' ')) {
    // if no , or =, must be a plain word to put out
    if (word.find_first_of(",=") == string::npos) {
      out.push_back(word);
      continue;
    }
    auto desckv = cmddesc_get_args(word);
    auto j = desckv.find("type");
    if (j != desckv.end() && j->second == "CephBool") {
      // Instruct legacy clients or mons to send --foo-bar string in place
      // of a 'true'/'false' value
      std::ostringstream oss;
      oss << std::string("--") << desckv["name"];
      std::string val = oss.str();
      std::replace(val.begin(), val.end(), '_', '-');
      desckv["type"] = "CephChoices";
      desckv["strings"] = val;
      std::ostringstream fss;
      for (auto k = desckv.begin(); k != desckv.end(); ++k) {
	if (k != desckv.begin()) {
	  fss << ",";
	}
	fss << k->first << "=" << k->second;
      }
      out.push_back(fss.str());
      changed = true;
    } else {
      out.push_back(word);
    }
  }
  if (!changed) {
    return cmddesc;
  }
  std::string o;
  for (auto i = out.begin(); i != out.end(); ++i) {
    if (i != out.begin()) {
      o += " ";
    }
    o += *i;
  }
  return o;
}

/**
 * Read a command description list out of cmd, and dump it to f.
 * A signature description is a set of space-separated words;
 * see MonCommands.h for more info.
 */

void
dump_cmd_to_json(Formatter *f, uint64_t features, const string& cmd)
{
  // put whole command signature in an already-opened container
  // elements are: "name", meaning "the typeless name that means a literal"
  // an object {} with key:value pairs representing an argument

  stringstream ss(cmd);
  std::string word;

  while (std::getline(ss, word, ' ')) {
    // if no , or =, must be a plain word to put out
    if (word.find_first_of(",=") == string::npos) {
      f->dump_string("arg", word);
      continue;
    }
    // accumulate descriptor keywords in desckv
    auto desckv = cmddesc_get_args(word);
    // name the individual desc object based on the name key
    f->open_object_section(string(desckv["name"]).c_str());

    // Compatibility for pre-nautilus clients that don't know about CephBool
    std::string val;
    if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
      auto i = desckv.find("type");
      if (i != desckv.end() && i->second == "CephBool") {
        // Instruct legacy clients to send --foo-bar string in place
        // of a 'true'/'false' value
        std::ostringstream oss;
        oss << std::string("--") << desckv["name"];
        val = oss.str();
        std::replace(val.begin(), val.end(), '_', '-');

        desckv["type"] = "CephChoices";
        desckv["strings"] = val;
      }
    }

    // dump all the keys including name into the array
    for (auto [key, value] : desckv) {
      f->dump_string(string(key).c_str(), string(value));
    }
    f->close_section(); // attribute object for individual desc
  }
}

void
dump_cmd_and_help_to_json(Formatter *jf,
			  uint64_t features,
			  const string& secname,
			  const string& cmdsig,
			  const string& helptext)
{
      jf->open_object_section(secname.c_str());
      jf->open_array_section("sig");
      dump_cmd_to_json(jf, features, cmdsig);
      jf->close_section(); // sig array
      jf->dump_string("help", helptext.c_str());
      jf->close_section(); // cmd
}

void
dump_cmddesc_to_json(Formatter *jf,
		     uint64_t features,
		     const string& secname,
		     const string& cmdsig,
		     const string& helptext,
		     const string& module,
		     const string& perm,
		     uint64_t flags)
{
      jf->open_object_section(secname.c_str());
      jf->open_array_section("sig");
      dump_cmd_to_json(jf, features, cmdsig);
      jf->close_section(); // sig array
      jf->dump_string("help", helptext.c_str());
      jf->dump_string("module", module.c_str());
      jf->dump_string("perm", perm.c_str());
      jf->dump_int("flags", flags);
      jf->close_section(); // cmd
}

void cmdmap_dump(const cmdmap_t &cmdmap, Formatter *f)
{
  ceph_assert(f != nullptr);

  class dump_visitor : public boost::static_visitor<void>
  {
    Formatter *f;
    std::string const &key;
    public:
    dump_visitor(Formatter *f_, std::string const &key_)
      : f(f_), key(key_)
    {
    }

    void operator()(const std::string &operand) const
    {
      f->dump_string(key.c_str(), operand);
    }

    void operator()(const bool &operand) const
    {
      f->dump_bool(key.c_str(), operand);
    }

    void operator()(const int64_t &operand) const
    {
      f->dump_int(key.c_str(), operand);
    }

    void operator()(const double &operand) const
    {
      f->dump_float(key.c_str(), operand);
    }

    void operator()(const std::vector<std::string> &operand) const
    {
      f->open_array_section(key.c_str());
      for (const auto i : operand) {
        f->dump_string("item", i);
      }
      f->close_section();
    }

    void operator()(const std::vector<int64_t> &operand) const
    {
      f->open_array_section(key.c_str());
      for (const auto i : operand) {
        f->dump_int("item", i);
      }
      f->close_section();
    }

    void operator()(const std::vector<double> &operand) const
    {
      f->open_array_section(key.c_str());
      for (const auto i : operand) {
        f->dump_float("item", i);
      }
      f->close_section();
    }
  };

  //f->open_object_section("cmdmap");
  for (const auto &i : cmdmap) {
    boost::apply_visitor(dump_visitor(f, i.first), i.second);
  }
  //f->close_section();
}


/** Parse JSON in vector cmd into a map from field to map of values
 * (use mValue/mObject)
 * 'cmd' should not disappear over lifetime of map
 * 'mapp' points to the caller's map
 * 'ss' captures any errors during JSON parsing; if function returns
 * false, ss is valid */

bool
cmdmap_from_json(vector<string> cmd, cmdmap_t *mapp, stringstream &ss)
{
  json_spirit::mValue v;

  string fullcmd;
  // First, join all cmd strings
  for (vector<string>::iterator it = cmd.begin();
       it != cmd.end(); ++it)
    fullcmd += *it;

  try {
    if (!json_spirit::read(fullcmd, v))
      throw runtime_error("unparseable JSON " + fullcmd);
    if (v.type() != json_spirit::obj_type)
      throw(runtime_error("not JSON object " + fullcmd));

    // allocate new mObject (map) to return
    // make sure all contents are simple types (not arrays or objects)
    json_spirit::mObject o = v.get_obj();
    for (map<string, json_spirit::mValue>::iterator it = o.begin();
	 it != o.end(); ++it) {

      // ok, marshal it into our string->cmd_vartype map, or throw an
      // exception if it's not a simple datatype.  This is kind of
      // annoying, since json_spirit has a boost::variant inside it
      // already, but it's not public.  Oh well.

      switch (it->second.type()) {

      case json_spirit::obj_type:
      default:
	throw(runtime_error("JSON array/object not allowed " + fullcmd));
        break;

      case json_spirit::array_type:
	{
	  // array is a vector of values.  Unpack it to a vector
	  // of strings, doubles, or int64_t, the only types we handle.
	  const vector<json_spirit::mValue>& spvals = it->second.get_array();
	  if (spvals.empty()) {
	    // if an empty array is acceptable, the caller should always check for
	    // vector<string> if the expected value of "vector<int64_t>" in the
	    // cmdmap is missing.
	    (*mapp)[it->first] = vector<string>();
	  } else if (spvals.front().type() == json_spirit::str_type) {
	    vector<string> outv;
	    for (const auto& sv : spvals) {
	      if (sv.type() != json_spirit::str_type) {
		throw(runtime_error("Can't handle arrays of multiple types"));
	      }
	      outv.push_back(sv.get_str());
	    }
	    (*mapp)[it->first] = std::move(outv);
	  } else if (spvals.front().type() == json_spirit::int_type) {
	    vector<int64_t> outv;
	    for (const auto& sv : spvals) {
	      if (spvals.front().type() != json_spirit::int_type) {
		throw(runtime_error("Can't handle arrays of multiple types"));
	      }
	      outv.push_back(sv.get_int64());
	    }
	    (*mapp)[it->first] = std::move(outv);
	  } else if (spvals.front().type() == json_spirit::real_type) {
	    vector<double> outv;
	    for (const auto& sv : spvals) {
	      if (spvals.front().type() != json_spirit::real_type) {
		throw(runtime_error("Can't handle arrays of multiple types"));
	      }
	      outv.push_back(sv.get_real());
	    }
	    (*mapp)[it->first] = std::move(outv);
	  } else {
	    throw(runtime_error("Can't handle arrays of types other than "
				"int, string, or double"));
	  }
	}
	break;
      case json_spirit::str_type:
	(*mapp)[it->first] = it->second.get_str();
	break;

      case json_spirit::bool_type:
	(*mapp)[it->first] = it->second.get_bool();
	break;

      case json_spirit::int_type:
	(*mapp)[it->first] = it->second.get_int64();
	break;

      case json_spirit::real_type:
	(*mapp)[it->first] = it->second.get_real();
	break;
      }
    }
    return true;
  } catch (runtime_error &e) {
    ss << e.what();
    return false;
  }
}

class stringify_visitor : public boost::static_visitor<string>
{
  public:
    template <typename T>
    string operator()(T &operand) const
      {
	ostringstream oss;
	oss << operand;
	return oss.str();
      }
};

string 
cmd_vartype_stringify(const cmd_vartype &v)
{
  return boost::apply_visitor(stringify_visitor(), v);
}


void
handle_bad_get(CephContext *cct, const string& k, const char *tname)
{
  ostringstream errstr;
  int status;
  const char *typestr = abi::__cxa_demangle(tname, 0, 0, &status);
  if (status != 0) 
    typestr = tname;
  errstr << "bad boost::get: key " << k << " is not type " << typestr;
  lderr(cct) << errstr.str() << dendl;

  ostringstream oss;
  oss << BackTrace(1);
  lderr(cct) << oss.str() << dendl;

  if (status == 0)
    free((char *)typestr);
}

long parse_pos_long(const char *s, std::ostream *pss)
{
  if (*s == '-' || *s == '+') {
    if (pss)
      *pss << "expected numerical value, got: " << s;
    return -EINVAL;
  }

  string err;
  long r = strict_strtol(s, 10, &err);
  if ((r == 0) && !err.empty()) {
    if (pss)
      *pss << err;
    return -1;
  }
  if (r < 0) {
    if (pss)
      *pss << "unable to parse positive integer '" << s << "'";
    return -1;
  }
  return r;
}

int parse_osd_id(const char *s, std::ostream *pss)
{
  // osd.NNN?
  if (strncmp(s, "osd.", 4) == 0) {
    s += 4;
  }

  // NNN?
  ostringstream ss;
  long id = parse_pos_long(s, &ss);
  if (id < 0) {
    *pss << ss.str();
    return id;
  }
  if (id > 0xffff) {
    *pss << "osd id " << id << " is too large";
    return -ERANGE;
  }
  return id;
}

namespace {
template <typename Func>
bool find_first_in(std::string_view s, const char *delims, Func&& f)
{
  auto pos = s.find_first_not_of(delims);
  while (pos != s.npos) {
    s.remove_prefix(pos);
    auto end = s.find_first_of(delims);
    if (f(s.substr(0, end))) {
      return true;
    }
    pos = s.find_first_not_of(delims, end);
  }
  return false;
}

template<typename T>
T str_to_num(const std::string& s)
{
  if constexpr (is_same_v<T, int>) {
    return std::stoi(s);
  } else if constexpr (is_same_v<T, long>) {
    return std::stol(s);
  } else if constexpr (is_same_v<T, long long>) {
    return std::stoll(s);
  } else if constexpr (is_same_v<T, double>) {
    return std::stod(s);
  }
}

template<typename T>
bool arg_in_range(T value, const arg_desc_t& desc, std::ostream& os) {
  auto range = desc.find("range");
  if (range == desc.end()) {
    return true;
  }
  auto min_max = get_str_list(string(range->second), "|");
  auto min = str_to_num<T>(min_max.front());
  auto max = numeric_limits<T>::max();
  if (min_max.size() > 1) {
    max = str_to_num<T>(min_max.back());
  }
  if (value < min || value > max) {
    os << "'" << value << "' out of range: " << min_max;
    return false;
  }
  return true;
}

bool validate_str_arg(std::string_view value,
		      std::string_view type,
		      const arg_desc_t& desc,
		      std::ostream& os)
{
  if (type == "CephIPAddr") {
    entity_addr_t addr;
    if (addr.parse(string(value).c_str())) {
      return true;
    } else {
      os << "failed to parse addr '" << value << "', should be ip:[port]";
      return false;
    }
  } else if (type == "CephChoices") {
    auto choices = desc.find("strings");
    ceph_assert(choices != end(desc));
    auto strings = choices->second;
    if (find_first_in(strings, "|", [=](auto choice) {
	  return (value == choice);
	})) {
      return true;
    } else {
      os << "'" << value << "' not belong to '" << strings << "'";
      return false;
    }
  } else {
    // CephString or other types like CephPgid
    return true;
  }
}

template<bool is_vector,
	 typename T,
	 typename Value = conditional_t<is_vector,
					vector<T>,
					T>>
bool validate_arg(CephContext* cct,
		  const cmdmap_t& cmdmap,
		  const arg_desc_t& desc,
		  const std::string_view name,
		  const std::string_view type,
		  std::ostream& os)
{
  Value v;
  try {
    if (!cmd_getval(cct, cmdmap, string(name), v)) {
      if constexpr (is_vector) {
	  // an empty list is acceptable.
	  return true;
	} else {
	if (auto req = desc.find("req");
	    req != end(desc) && req->second == "false") {
	  return true;
	} else {
	  os << "missing required parameter: '" << name << "'";
	  return false;
	}
      }
    }
  } catch (const bad_cmd_get& e) {
    return false;
  }
  auto validate = [&](const T& value) {
    if constexpr (is_same_v<std::string, T>) {
      return validate_str_arg(value, type, desc, os);
    } else if constexpr (is_same_v<int64_t, T> ||
			 is_same_v<double, T>) {
      return arg_in_range(value, desc, os);
    }
  };
  if constexpr(is_vector) {
    return find_if_not(begin(v), end(v), validate) == end(v);
  } else {
    return validate(v);
  }
}
} // anonymous namespace

bool validate_cmd(CephContext* cct,
		  const std::string& desc,
		  const cmdmap_t& cmdmap,
		  std::ostream& os)
{
  return !find_first_in(desc, " ", [&](auto desc) {
    auto arg_desc = cmddesc_get_args(desc);
    if (arg_desc.empty()) {
      return false;
    }
    ceph_assert(arg_desc.count("name"));
    ceph_assert(arg_desc.count("type"));
    auto name = arg_desc["name"];
    auto type = arg_desc["type"];
    if (arg_desc.count("n")) {
      if (type == "CephInt") {
	return !validate_arg<true, int64_t>(cct, cmdmap, arg_desc,
					    name, type, os);
      } else if (type == "CephFloat") {
	return !validate_arg<true, double>(cct, cmdmap, arg_desc,
					    name, type, os);
      } else {
	return !validate_arg<true, string>(cct, cmdmap, arg_desc,
					   name, type, os);
      }
    } else {
      if (type == "CephInt") {
	return !validate_arg<false, int64_t>(cct, cmdmap, arg_desc,
					    name, type, os);
      } else if (type == "CephFloat") {
	return !validate_arg<false, double>(cct, cmdmap, arg_desc,
					    name, type, os);
      } else {
	return !validate_arg<false, string>(cct, cmdmap, arg_desc,
					    name, type, os);
      }
    }
  });
}

bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
		const std::string& k, bool& val)
{
  /*
   * Specialized getval for booleans.  CephBool didn't exist before Nautilus,
   * so earlier clients are sent a CephChoices argdesc instead, and will
   * send us a "--foo-bar" value string for boolean arguments.
   */
  if (cmdmap.count(k)) {
    try {
      val = boost::get<bool>(cmdmap.find(k)->second);
      return true;
    } catch (boost::bad_get&) {
      try {
        std::string expected = "--" + k;
        std::replace(expected.begin(), expected.end(), '_', '-');

        std::string v_str = boost::get<std::string>(cmdmap.find(k)->second);
        if (v_str == expected) {
          val = true;
          return true;
        } else {
          throw bad_cmd_get(k, cmdmap);
        }
      } catch (boost::bad_get&) {
        throw bad_cmd_get(k, cmdmap);
      }
    }
  }
  return false;
}


