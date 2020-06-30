// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#pragma once
#include <string>
#include <vector>

namespace rgw::notify {
  enum EventType {
    ObjectCreated                        = 0xF,
    ObjectCreatedPut                     = 0x1,
    ObjectCreatedPost                    = 0x2,
    ObjectCreatedCopy                    = 0x4,
    ObjectCreatedCompleteMultipartUpload = 0x8,
    ObjectRemoved                        = 0xF0,
    ObjectRemovedDelete                  = 0x10,
    ObjectRemovedDeleteMarkerCreated     = 0x20,
    UnknownEvent                         = 0x100
  };

  using EventTypeList = std::vector<EventType>;

  // two event types are considered equal if their bits intersect
  bool operator==(EventType lhs, EventType rhs);

  std::string to_string(EventType t);

  std::string to_ceph_string(EventType t);

  EventType from_string(const std::string& s);
 
  // create a vector of event types from comma separated list of event types
  void from_string_list(const std::string& string_list, EventTypeList& event_list);
}

