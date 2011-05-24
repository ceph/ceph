#include "common/DoutStreambuf.h"
#include "common/code_environment.h"
#include "common/config.h"
#include "common/version.h"
#include "debug.h"

#include <iostream>
#include <sstream>

void output_ceph_version()
{
  char buf[1024];
  snprintf(buf, sizeof(buf), "ceph version %s.commit: %s. process: %s. "
	    "pid: %d", ceph_version_to_str(), git_version_to_str(),
	    get_process_name_cpp().c_str(), getpid());
  generic_dout(0) << buf << dendl;
}
