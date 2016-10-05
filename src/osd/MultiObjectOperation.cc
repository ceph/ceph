#include "boost/tuple/tuple.hpp"
#include "boost/bind.hpp"
#include "PG.h"
#include "ReplicatedPG.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/config.h"
#include "include/compat.h"

#include "osdc/Objecter.h"

#include "include/assert.h"
#include "include/rados/rados_types.hpp"

#ifdef WITH_LTTNG
#include "tracing/osd.h"
#else
#define tracepoint(...)
#endif

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
template <typename T>
static ostream& _prefix(std::ostream *_dout, T *pg) {
  return *_dout << pg->gen_prefix();
}


#include <sstream>
#include <utility>

#include <errno.h>

void ReplicatedPG::MultiObjectWriteOpContext::encode(bufferlist &bl)
{
  ENCODE_START(1, 1, bl);

  __u8 m = is_master ? 1 : 0;
  ::encode(m, bl);

  ::encode(master, bl);
  ::encode(locator, bl);
  ::encode(snapc, bl);
  utime_t t;
  t = ceph::real_clock::to_ceph_timespec(mtime);
  ::encode(t, bl);
  ::encode(master_reqid, bl);

  if (is_master) {
    int obj_nums = sub_objects.size();
    ::encode(obj_nums, bl);
    for (auto &p : sub_objects)
      ::encode(p.first, bl);
  } else {
    ::encode(slave_data_bl, bl);
  }

  ENCODE_FINISH(bl);
}

void ReplicatedPG::MultiObjectWriteOpContext::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);

  __u8 m;
  ::decode(m, bl);
  if (m)
    is_master = true;

  ::decode(master, bl);
  ::decode(locator, bl);
  ::decode(snapc, bl);
  utime_t t;
  ::decode(t, bl);
  mtime = ceph::real_clock::from_ceph_timespec(t);
  ::decode(master_reqid, bl);

  if (is_master) {
    int obj_nums;
    ::decode(obj_nums, bl);
    assert(obj_nums > 0);
    for (int i = 0; i < obj_nums; ++i) {
      object_t obj;
      ::decode(obj, bl);
      sub_objects.insert(make_pair(obj, make_pair(0, 0)));
    }
  } else {
    ::decode(slave_data_bl, bl);
  }

  DECODE_FINISH(bl);
}
