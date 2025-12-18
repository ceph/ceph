// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/ostream_temp.h"

OstreamTemp::OstreamTemp(clog_type type_, OstreamTempSink *parent_)
  : type(type_), parent(parent_)
{
}

OstreamTemp::~OstreamTemp()
{
  if (ss.peek() != EOF && parent)
    parent->do_log(type, ss);
}
