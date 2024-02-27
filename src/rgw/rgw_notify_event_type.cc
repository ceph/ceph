// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "rgw_notify_event_type.h"
#include "include/str_list.h"

namespace rgw::notify {

  std::string to_string(EventType t) {
    switch (t) {
    case ObjectCreated:
      return "s3:ObjectCreated:*";
    case ObjectCreatedPut:
      return "s3:ObjectCreated:Put";
    case ObjectCreatedPost:
      return "s3:ObjectCreated:Post";
    case ObjectCreatedCopy:
      return "s3:ObjectCreated:Copy";
    case ObjectCreatedCompleteMultipartUpload:
      return "s3:ObjectCreated:CompleteMultipartUpload";
    case ObjectRemoved:
      return "s3:ObjectRemoved:*";
    case ObjectRemovedDelete:
      return "s3:ObjectRemoved:Delete";
    case ObjectRemovedDeleteMarkerCreated:
      return "s3:ObjectRemoved:DeleteMarkerCreated";
    case ObjectLifecycle:
      return "s3:ObjectLifecycle:*";
    case ObjectExpiration:
      return "s3:ObjectLifecycle:Expiration:*";
    case ObjectExpirationCurrent:
      return "s3:ObjectLifecycle:Expiration:Current";
    case ObjectExpirationNoncurrent:
      return "s3:ObjectLifecycle:Expiration:Noncurrent";
    case ObjectExpirationDeleteMarker:
      return "s3:ObjectLifecycle:Expiration:DeleteMarker";
    case ObjectExpirationAbortMPU:
      return "s3:ObjectLifecycle:Expiration:AbortMPU";
    case ObjectTransition:
      return "s3:ObjectLifecycle:Transition:*";
    case ObjectTransitionCurrent:
      return "s3:ObjectLifecycle:Transition:Current";
    case ObjectTransitionNoncurrent:
      return "s3:ObjectLifecycle:Transition:Noncurrent";
    case ObjectSynced:
      return "s3:ObjectSynced:*";
    case ObjectSyncedCreate:
      return "s3:ObjectSynced:Create";
    case ObjectSyncedDelete:
      return "s3:ObjectSynced:Delete";
    case ObjectSyncedDeletionMarkerCreated:
      return "s3:ObjectSynced:DeletionMarkerCreated";
    case LifecycleExpiration:
      return "s3:LifecycleExpiration:*";
    case LifecycleExpirationDelete:
      return "s3:LifecycleExpiration:Delete";
    case LifecycleExpirationDeleteMarkerCreated:
      return "s3:LifecycleExpiration:DeleteMarkerCreated";
    case LifecycleTransition:
      return "s3:LifecycleTransition";
    case UnknownEvent:
        return "s3:UnknownEvent";
    }
    return "s3:UnknownEvent";
  }

  std::string to_event_string(EventType t) {
    return to_string(t).substr(3);
  }

  EventType from_string(const std::string& s) {
    if (s == "s3:ObjectCreated:*")
        return ObjectCreated;
    if (s == "s3:ObjectCreated:Put")
        return ObjectCreatedPut;
    if (s == "s3:ObjectCreated:Post")
        return ObjectCreatedPost;
    if (s == "s3:ObjectCreated:Copy")
        return ObjectCreatedCopy;
    if (s == "s3:ObjectCreated:CompleteMultipartUpload")
        return ObjectCreatedCompleteMultipartUpload;
    if (s == "s3:ObjectRemoved:*")
        return ObjectRemoved;
    if (s == "s3:ObjectRemoved:Delete")
        return ObjectRemovedDelete;
    if (s == "s3:ObjectRemoved:DeleteMarkerCreated")
        return ObjectRemovedDeleteMarkerCreated;
    if (s == "s3:ObjectLifecycle:*")
        return ObjectLifecycle;
    if (s == "s3:ObjectLifecycle:Expiration:*")
        return ObjectExpiration;
    if (s == "s3:ObjectLifecycle:Expiration:Current")
        return ObjectExpirationCurrent;
    if (s == "s3:ObjectLifecycle:Expiration:Noncurrent")
        return ObjectExpirationNoncurrent;
    if (s == "s3:ObjectLifecycle:Expiration:DeleteMarker")
        return ObjectExpirationDeleteMarker;
    if (s == "s3:ObjectLifecycle:Expiration:AbortMultipartUpload")
        return ObjectExpirationAbortMPU;
    if (s == "s3:ObjectLifecycle:Transition:*")
        return ObjectTransition;
    if (s == "s3:ObjectLifecycle:Transition:Current")
        return ObjectTransitionCurrent;
    if (s == "s3:ObjectLifecycle:Transition:Noncurrent")
        return ObjectTransitionNoncurrent;
    if (s == "s3:ObjectSynced:*")
        return ObjectSynced;
    if (s == "s3:ObjectSynced:Create")
        return ObjectSyncedCreate;
    if (s == "s3:ObjectSynced:Delete")
        return ObjectSyncedDelete;
    if (s == "s3:ObjectSynced:DeletionMarkerCreated")
        return ObjectSyncedDeletionMarkerCreated;
    if (s == "s3:LifecycleExpiration:*")
      return LifecycleExpiration;
    if (s == "s3:LifecycleExpiration:Delete")
      return LifecycleExpirationDelete;
    if (s == "s3:LifecycleExpiration:DeleteMarkerCreated")
      return LifecycleExpirationDeleteMarkerCreated;
    if (s == "s3:LifecycleTransition")
      return LifecycleTransition;
    return UnknownEvent;
  }

bool operator==(EventType lhs, EventType rhs) {
  return lhs & rhs;
}

void from_string_list(const std::string& string_list, EventTypeList& event_list) {
  event_list.clear();
  ceph::for_each_substr(string_list, ",", [&event_list] (auto token) {
    event_list.push_back(rgw::notify::from_string(std::string(token.begin(), token.end())));
  });
}
}
