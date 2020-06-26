// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// // vim: ts=8 sw=2 smarttab ft=cpp
//
#ifndef RGW_CACHEREQUEST_H
#define RGW_CACHEREQUEST_H

#include "rgw_rest_conn.h"
#include <aio.h>
#include "rgw_aio.h"
#include "rgw_rest_client.h"

class RGWRESTConn;
struct get_obj_data;
struct AioResult;
class Aio;
class RGWRESTStreamRWRequest;
//class RGWRadosGetObj;

class CacheRequest {
  public:
    ceph::mutex lock = ceph::make_mutex("CacheRequest");
//    struct get_obj_data *op_data;
    int sequence;
    int stat;
    bufferlist *bl;
    uint64_t ofs;
    uint64_t read_ofs;
    uint64_t read_len;   
    rgw::AioResult* r= nullptr;
    std::string key;
    rgw::Aio* aio = nullptr;
    CacheRequest() :  sequence(0), stat(-1), bl(NULL), ofs(0),  read_ofs(0), read_len(0){};
    virtual ~CacheRequest(){};
    virtual void release()=0;
    virtual void cancel_io()=0;
    virtual int status()=0;
    virtual void finish()=0;
};

struct LocalRequest : public CacheRequest{
  struct aiocb *paiocb;
  LocalRequest() :  CacheRequest(), paiocb(NULL) {}
  ~LocalRequest(){}

  int prepare_op(std::string key,  bufferlist *bl, int read_len, int ofs, int read_ofs, void (*f)(sigval_t), rgw::Aio* aio, rgw::AioResult* r) {
    this->r = r;	
    this->aio = aio;
    this->bl = bl;
    this->ofs = ofs;
    this->key = key;
    this->read_len = read_len;
    this->stat = EINPROGRESS;	
    std::string loc = "/tmp/" + key;
    struct aiocb *cb = new struct aiocb;
    memset(cb, 0, sizeof(struct aiocb));
    cb->aio_fildes = ::open(loc.c_str(), O_RDONLY);
    if (cb->aio_fildes < 0) {
      return -1;
    }
    cb->aio_buf = malloc(read_len);
    cb->aio_nbytes = read_len;
    cb->aio_offset = read_ofs;
    cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    cb->aio_sigevent.sigev_notify_function = f ;
    cb->aio_sigevent.sigev_notify_attributes = NULL;
    cb->aio_sigevent.sigev_value.sival_ptr = (void*)this;
    this->paiocb = cb;
    return 0;
  }

  void release (){
    lock.lock();
    free((void *)paiocb->aio_buf);
    paiocb->aio_buf = NULL;
    ::close(paiocb->aio_fildes);
    free(paiocb);
    lock.unlock();
    delete this;
  }  

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  int status(){
    lock.lock();
    if (stat != EINPROGRESS) {
      lock.unlock();
      if (stat == ECANCELED){
	release();
	return ECANCELED;
      }}
    stat = aio_error(paiocb);
    lock.unlock();
    return stat;
  }

  void finish(){
    bl->append((char*)paiocb->aio_buf, paiocb->aio_nbytes);
    release();
  }
};

struct RemoteRequest : public CacheRequest{
  string dest;
  int stat;
  void *tp;
  RGWRESTConn *conn;
  RGWRESTStreamRWRequest *in_stream_req;
  cache_obj *c_obj;
  RGWRados *store;
  CephContext *cct;
  rgw_obj obj;
  RemoteRequest(rgw_obj& _obj, cache_obj* _c_obj, RGWRados* _store, CephContext* _cct) :  CacheRequest() , stat(-1), obj(_obj), c_obj(_c_obj), store(_store), cct(_cct){
    }

  ~RemoteRequest(){}
  int prepare_op(std::string key,  bufferlist *bl, int read_len, int ofs, int read_ofs, string dest, rgw::Aio* aio, rgw::AioResult* r);
  int submit_op();
  
  void release (){
    lock.lock();
    lock.unlock();
  }

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  void finish(){
    release();
  }
  int status(){
    return 0;
  }

};  

#endif 
