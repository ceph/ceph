#ifndef CEPH_STRLIST_H
#define CEPH_STRLIST_H

#include <string>
#include <list>
#include <set>

extern bool get_str_list(const std::string& str,
			 std::list<std::string>& str_list);
extern bool get_str_set(const std::string& str,
			std::set<std::string>& str_list);


#endif
