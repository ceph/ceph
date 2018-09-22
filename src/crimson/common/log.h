#include <seastar/util/log.hh>
#include "common/subsys_types.h"

namespace ceph {
seastar::logger& get_logger(int subsys);
}
