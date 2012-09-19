#ifndef CEPH_RGW_USAGE_H
#define CEPH_RGW_USAGE_H

#include <string>
#include <map>

#include "common/Formatter.h"

class RGWRados;


class RGWUsage
{
public:
  static int show(RGWRados *store, std::string& uid, uint64_t start_epoch,
	          uint64_t end_epoch, bool show_log_entries, bool show_log_sum,
		  std::map<std::string, bool> *categories,
	          ceph::Formatter *formatter);

  static int trim(RGWRados *store, std::string& uid, uint64_t start_epoch,
	          uint64_t end_epoch);
};


#endif
