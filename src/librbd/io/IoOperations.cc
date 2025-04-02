// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include "librbd/io/Types.h"
#include "librbd/io/IoOperations.h"

#include <map>
#include <vector>

namespace librbd {
namespace io {

#define RBD_IO_OPERATION_NAME_READ                 "read"
#define RBD_IO_OPERATION_NAME_WRITE                "write"
#define RBD_IO_OPERATION_NAME_DISCARD              "discard"
#define RBD_IO_OPERATION_NAME_WRITE_SAME           "write_same"
#define RBD_IO_OPERATION_NAME_COMPARE_AND_WRITE    "compare_and_write"

static const std::map<std::string, uint64_t> RBD_IO_OPERATION_MAP = {
  {RBD_IO_OPERATION_NAME_READ, RBD_IO_OPERATION_READ},
  {RBD_IO_OPERATION_NAME_WRITE, RBD_IO_OPERATION_WRITE},
  {RBD_IO_OPERATION_NAME_DISCARD, RBD_IO_OPERATION_DISCARD},
  {RBD_IO_OPERATION_NAME_WRITE_SAME, RBD_IO_OPERATION_WRITE_SAME},
  {RBD_IO_OPERATION_NAME_COMPARE_AND_WRITE, RBD_IO_OPERATION_COMPARE_AND_WRITE},
};
static_assert((RBD_IO_OPERATION_COMPARE_AND_WRITE << 1) > RBD_IO_OPERATIONS_ALL,
		          "new RBD io operation added");

std::string rbd_io_operations_to_string(uint64_t operations,
                                        std::ostream *err)
{
  std::string r;
  for (auto& i : RBD_IO_OPERATION_MAP) {
    if (operations & i.second) {
      if (!r.empty()) {
      r += ",";
      }
      r += i.first;
      operations &= ~i.second;
    }
  }
  if (err && operations) {
    *err << "ignoring unknown io operation mask 0x"
	 << std::hex << operations << std::dec;
  }
  return r;
}

uint64_t rbd_io_operations_from_string(const std::string& orig_value,
		                       std::ostream *err)
{
  uint64_t operations = 0;
  std::string value = orig_value;
  boost::trim(value);

  // empty string means default operations
  if (!value.size()) {
    return RBD_IO_OPERATIONS_DEFAULT;
  }

  try {
    // numeric?
    operations = boost::lexical_cast<uint64_t>(value);

    // drop unrecognized bits
    uint64_t unsupported_operations = (operations & ~RBD_IO_OPERATIONS_ALL);
    if (unsupported_operations != 0ull) {
      operations &= RBD_IO_OPERATIONS_ALL;
      if (err) {
	*err << "ignoring unknown operation mask 0x"
             << std::hex << unsupported_operations << std::dec;
      }
    }
  } catch (boost::bad_lexical_cast&) {
    // operation name list?
    bool errors = false;
    std::vector<std::string> operation_names;
    boost::split(operation_names, value, boost::is_any_of(","));
    for (auto operation_name: operation_names) {
      boost::trim(operation_name);
      auto operation_it = RBD_IO_OPERATION_MAP.find(operation_name);
      if (operation_it != RBD_IO_OPERATION_MAP.end()) {
	operations += operation_it->second;
      } else if (err) {
	if (errors) {
	  *err << ", ";
	} else {
	  errors = true;
	}
	*err << "ignoring unknown operation " << operation_name;
      }
    }
  }
  return operations;
}

} // namespace io
} // namespace librbd
