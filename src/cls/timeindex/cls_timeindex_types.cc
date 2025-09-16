#include "cls_timeindex_types.h"
#include "common/Formatter.h"

void cls_timeindex_entry::dump(Formatter *f) const
{
  f->dump_stream("key_ts") << key_ts;
  f->dump_string("key_ext", key_ext);
  f->dump_string("value", value.to_str());
}

std::list<cls_timeindex_entry> cls_timeindex_entry::generate_test_instances()
{
  std::list<cls_timeindex_entry> o;
  cls_timeindex_entry i;
  i.key_ts = utime_t(0,0);
  i.key_ext = "foo";
  bufferlist bl;
  bl.append("bar");
  i.value = bl;
  o.push_back(std::move(i));
  o.emplace_back();
  return o;
}
