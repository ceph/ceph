#pragma once

#include "msg/Message.h"

/*
 * MMDSOp is used to ensure that all incoming MDS-MDS
 * messages in MDSRankDispatcher are versioned. Therefore
 * all MDS-MDS messages must be of type MMDSOp.
 */
class MMDSOp: public SafeMessage {
public:
  template<typename... Types>
  MMDSOp(Types&&... args)
    : SafeMessage(std::forward<Types>(args)...) {}
};
