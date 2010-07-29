#ifndef CEPH_STRLIST_H
#define CEPH_STRLIST_H

#include <string>
#include <list>
#include <set>

extern bool get_str_list(std::string& str, std::list<std::string>& str_list);
extern bool get_str_set(std::string& str, std::set<std::string>& str_list);


#endif
