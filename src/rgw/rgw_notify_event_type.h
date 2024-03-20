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
    // lifecycle events (RGW extension)
    ObjectLifecycle                       = 0xFF00,
    ObjectExpiration                     = 0xF00,
    ObjectExpirationCurrent              = 0x100,
    ObjectExpirationNoncurrent           = 0x200,
    ObjectExpirationDeleteMarker         = 0x400,
    ObjectExpirationAbortMPU             = 0x800,
    ObjectTransition                     = 0xF000,
    ObjectTransitionCurrent              = 0x1000,
    ObjectTransitionNoncurrent           = 0x2000,
    ObjectSynced                         = 0xF0000,
    ObjectSyncedCreate                   = 0x10000,
    ObjectSyncedDelete                   = 0x20000,
    ObjectSyncedDeletionMarkerCreated    = 0x40000,
    LifecycleExpiration                    = 0xF00000,
    LifecycleExpirationDelete              = 0x100000,
    LifecycleExpirationDeleteMarkerCreated = 0x200000,
    LifecycleTransition                    = 0xF000000,
    Replication                            = 0xF0000000,
    ReplicationCreate                      = 0x10000000,
    ReplicationDelete                      = 0x20000000,
    ReplicationDeletionMarkerCreated       = 0x40000000,
    UnknownEvent                           = 0x100000000
};

  using EventTypeList = std::vector<EventType>;

  // two event types are considered equal if their bits intersect
  bool operator==(EventType lhs, EventType rhs);

  std::string to_string(EventType t);

  std::string to_event_string(EventType t);

  EventType from_string(const std::string& s);
 
  // create a vector of event types from comma separated list of event types
  void from_string_list(const std::string& string_list, EventTypeList& event_list);
}

