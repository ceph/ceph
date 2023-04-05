#ifndef CEPH_RGWD4NPOLICY_H
#define CEPH_RGWD4NPOLICY_H

#include "rgw_common.h"
#include <string>
#include <iostream>

class RGWD4NPolicy {
  public:
    RGWD4NPolicy() {}

    bool should_cache(int objSize, int minSize); /* In bytes */
    bool should_cache(std::string uploadType); 
};

#endif
