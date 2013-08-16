#ifndef CEPH_STRLIST_H
#define CEPH_STRLIST_H

#include <list>
#include <set>
#include <sstream>
#include <string>
#include <vector>

extern void get_str_list(const std::string& str,
			 std::list<std::string>& str_list);
extern void get_str_list(const std::string& str,
                         const char *delims,
			 std::list<std::string>& str_list);
extern void get_str_vec(const std::string& str,
			 std::vector<std::string>& str_vec);
extern void get_str_vec(const std::string& str,
                         const char *delims,
			 std::vector<std::string>& str_vec);
extern void get_str_set(const std::string& str,
			std::set<std::string>& str_list);
extern void get_str_set(const std::string& str,
                        const char *delims,
			std::set<std::string>& str_list);

inline std::string str_join(const std::vector<std::string>& v, std::string sep)
{
  if (v.empty())
    return std::string();
  std::vector<std::string>::const_iterator i = v.begin();
  std::string r = *i;
  for (++i; i != v.end(); ++i) {
    r += sep;
    r += *i;
  }
  return r;
}

#endif
