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
      case UnknownEvent:
        return "s3:UnknownEvet";
    }
    return "s3:UnknownEvent";
  }

  std::string to_ceph_string(EventType t) {
    switch (t) {
      case ObjectCreated:
      case ObjectCreatedPut:
      case ObjectCreatedPost:
      case ObjectCreatedCopy:
      case ObjectCreatedCompleteMultipartUpload:
        return "OBJECT_CREATE";
      case ObjectRemovedDelete:
        return "OBJECT_DELETE";
      case ObjectRemovedDeleteMarkerCreated:
        return "DELETE_MARKER_CREATE";
      case ObjectRemoved:
      case UnknownEvent:
        return "UNKNOWN_EVENT";
    }
    return "UNKNOWN_EVENT";
  }

  EventType from_string(const std::string& s) {
    if (s == "s3:ObjectCreated:*" || s == "OBJECT_CREATE")
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
    if (s == "s3:ObjectRemoved:Delete" || s == "OBJECT_DELETE")
        return ObjectRemovedDelete;
    if (s == "s3:ObjectRemoved:DeleteMarkerCreated" || s == "DELETE_MARKER_CREATE")
        return ObjectRemovedDeleteMarkerCreated;
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
