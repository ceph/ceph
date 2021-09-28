#ifndef CEPH_REPLICA_TYPES_H
#define CEPH_REPLICA_TYPES_H

#include <inttypes.h>
#include "include/ceph_assert.h"
#include "include/encoding.h"
#include "include/types.h"

namespace librbd::cache::pwl::rwl::replica {

#define MSG_SIZE 4096

const int RWL_REPLICA_INIT_REQUEST        = 0x10;
const int RWL_REPLICA_INIT_SUCCESSED      = 0x11;
const int RWL_REPLICA_INIT_FAILED         = 0x12;
const int RWL_REPLICA_FINISHED_REQUEST    = 0x20;
const int RWL_REPLICA_FINISHED_SUCCCESSED = 0x21;
const int RWL_REPLICA_FINISHED_FAILED     = 0x22;

struct RwlCacheInfo{
    epoch_t cache_id;
    uint64_t cache_size;
    std::string pool_name;
    std::string image_name;
    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &it);
};
WRITE_CLASS_ENCODER(RwlCacheInfo)

struct RpmaConfigDescriptor{
    uint32_t mr_desc_size;
    uint32_t pcfg_desc_size;
    std::string descriptors;
    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &it);
};
WRITE_CLASS_ENCODER(RpmaConfigDescriptor)

class RwlReplicaRequest {
public:
    int type;
    RwlReplicaRequest(int type) : type(type) {}
    RwlReplicaRequest() {}
    virtual ~RwlReplicaRequest() {}
    virtual void encode(ceph::buffer::list &bl) const;
    virtual void decode(ceph::buffer::list::const_iterator &it);
};
WRITE_CLASS_ENCODER(RwlReplicaRequest)

class RwlReplicaInitRequest : public RwlReplicaRequest{
public:
    RwlReplicaInitRequest(int type) : RwlReplicaRequest(type) {}
    RwlReplicaInitRequest() {}
    virtual ~RwlReplicaInitRequest() {}
    struct RwlCacheInfo info;
    virtual void encode(ceph::buffer::list &bl) const override;
    virtual void decode(ceph::buffer::list::const_iterator &it) override;
};
WRITE_CLASS_ENCODER(RwlReplicaInitRequest)

class RwlReplicaInitRequestReply : public RwlReplicaRequest{
public:
    RwlReplicaInitRequestReply(int type) : RwlReplicaRequest(type) {}
    RwlReplicaInitRequestReply() {}
    virtual ~RwlReplicaInitRequestReply() {}
    struct RpmaConfigDescriptor desc;
    virtual void encode(ceph::buffer::list &bl) const override;
    virtual void decode(ceph::buffer::list::const_iterator &it) override;
};
WRITE_CLASS_ENCODER(RwlReplicaInitRequestReply)

class RwlReplicaFinishedRequest : public RwlReplicaRequest {
public:
    RwlReplicaFinishedRequest(int type) : RwlReplicaRequest(type) {}
    RwlReplicaFinishedRequest() {}
    virtual ~RwlReplicaFinishedRequest() {}
};
WRITE_CLASS_ENCODER(RwlReplicaFinishedRequest)

class RwlReplicaFinishedRequestReply : public RwlReplicaRequest {
public:
    RwlReplicaFinishedRequestReply(int type) : RwlReplicaRequest(type) {}
    RwlReplicaFinishedRequestReply() {}
    virtual ~RwlReplicaFinishedRequestReply() {}
};
WRITE_CLASS_ENCODER(RwlReplicaFinishedRequestReply)

} //namespace ceph::librbd::cache::pwl::rwl::replica
#endif //CEPH_REPLICA_TYPES_H
