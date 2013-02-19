#ifndef CEPH_STRLIST_H
#define CEPH_STRLIST_H

#include <string>
#include <list>
#include <set>

extern void get_str_list(const std::string& str,
			 std::list<std::string>& str_list);
extern void get_str_list(const std::string& str,
                         const char *delims,
			 std::list<std::string>& str_list);
extern void get_str_set(const std::string& str,
			std::set<std::string>& str_list);


#endif
