// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr/ServiceMap.h"

#include "common/Formatter.h"

using ceph::bufferlist;
using ceph::Formatter;

// Daemon

void ServiceMap::Daemon::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(gid, bl);
  encode(addr, bl, features);
  encode(start_epoch, bl);
  encode(start_stamp, bl);
  encode(metadata, bl);
  ENCODE_FINISH(bl);
}

void ServiceMap::Daemon::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(gid, p);
  decode(addr, p);
  decode(start_epoch, p);
  decode(start_stamp, p);
  decode(metadata, p);
  DECODE_FINISH(p);
}

void ServiceMap::Daemon::dump(Formatter *f) const
{
  f->dump_unsigned("start_epoch", start_epoch);
  f->dump_stream("start_stamp") << start_stamp;
  f->dump_unsigned("gid", gid);
  f->dump_string("addr", addr.get_legacy_str());
  f->open_object_section("metadata");
  for (auto& p : metadata) {
    f->dump_string(p.first.c_str(), p.second);
  }
  f->close_section();
}

void ServiceMap::Daemon::generate_test_instances(std::list<Daemon*>& ls)
{
  ls.push_back(new Daemon);
  ls.push_back(new Daemon);
  ls.back()->gid = 222;
  ls.back()->metadata["this"] = "that";
}

// Service

void ServiceMap::Service::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(daemons, bl, features);
  encode(summary, bl);
  ENCODE_FINISH(bl);
}

void ServiceMap::Service::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(daemons, p);
  decode(summary, p);
  DECODE_FINISH(p);
}

void ServiceMap::Service::dump(Formatter *f) const
{
  f->open_object_section("daemons");
  f->dump_string("summary", summary);
  for (auto& p : daemons) {
    f->dump_object(p.first.c_str(), p.second);
  }
  f->close_section();
}

void ServiceMap::Service::generate_test_instances(std::list<Service*>& ls)
{
  ls.push_back(new Service);
  ls.push_back(new Service);
  ls.back()->daemons["one"].gid = 1;
  ls.back()->daemons["two"].gid = 2;
}

// ServiceMap

void ServiceMap::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(1, 1, bl);
  encode(epoch, bl);
  encode(modified, bl);
  encode(services, bl, features);
  ENCODE_FINISH(bl);
}

void ServiceMap::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(epoch, p);
  decode(modified, p);
  decode(services, p);
  DECODE_FINISH(p);
}

void ServiceMap::dump(Formatter *f) const
{
  f->dump_unsigned("epoch", epoch);
  f->dump_stream("modified") << modified;
  f->open_object_section("services");
  for (auto& p : services) {
    f->dump_object(p.first.c_str(), p.second);
  }
  f->close_section();
}

void ServiceMap::generate_test_instances(std::list<ServiceMap*>& ls)
{
  ls.push_back(new ServiceMap);
  ls.push_back(new ServiceMap);
  ls.back()->epoch = 123;
  ls.back()->services["rgw"].daemons["one"].gid = 123;
  ls.back()->services["rgw"].daemons["two"].gid = 344;
  ls.back()->services["iscsi"].daemons["foo"].gid = 3222;
}
