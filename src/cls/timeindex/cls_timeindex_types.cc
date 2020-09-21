#include "cls_timeindex_types.h"
#include "common/Formatter.h"

void cls_timeindex_entry::dump(Formatter *f) const
{
  f->dump_stream("key_ts") << key_ts;
  f->dump_string("key_ext", key_ext);
  f->dump_string("value", value.to_str());
}

void cls_timeindex_entry::generate_test_instances(list<cls_timeindex_entry*>& o)
{
  cls_timeindex_entry *i = new cls_timeindex_entry;
  i->key_ts = utime_t(0,0);
  i->key_ext = "foo";
  bufferlist bl;
  bl.append("bar");
  i->value = bl;
  o.push_back(i);
  o.push_back(new cls_timeindex_entry);
}
