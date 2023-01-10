#ifndef RGW_S3_REQUEST_H
#define RGW_S3_REQUEST_H
#include "rgw_rest_conn.h"
#include <aio.h>
#include "rgw_aio.h"
#include "rgw_rest_client.h"
#include <boost/algorithm/string/replace.hpp>

#include <string>
#include <map>
#include <unordered_map>
#include "include/types.h"
#include "include/utime.h"
#include "include/ceph_assert.h"
#include "common/ceph_mutex.h"

#include "cls/version/cls_version_types.h"
#include "rgw_common.h"


/*datacache*/
#include <errno.h>
#include <unistd.h> 
#include <signal.h> 
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include "include/Context.h"
#include "rgw_threadpool.h"
#include <curl/curl.h>
#include "include/lru.h"

// #include <mutex> 
// #include "common/RWLock.h"
struct cache_obj;
class CopyRemoteS3Object;
class CacheThreadPool;
struct RemoteRequest;
struct ObjectDataInfo;
class RemoteS3Request;
class RGWGetDataCB;
struct DataCache;
/*datacache*/





class RGWRESTConn;
struct get_obj_data;
struct AioResult;
class Aio;
class RGWRESTStreamRWRequest;

class CacheRequest {
  public:
    ceph::mutex lock = ceph::make_mutex("CacheRequest");
    int sequence;
    int stat;
    bufferlist *bl=nullptr;
    off_t ofs;
    off_t read_ofs;
    off_t read_len;   
    rgw::AioResult* r = nullptr;
    std::string key;
    rgw::Aio* aio = nullptr;
    librados::AioCompletion *lc;
    Context *onack;
    CacheRequest() :  sequence(0), stat(-1), bl(nullptr), ofs(0),  read_ofs(0), read_len(0), lc(nullptr){};
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

  int prepare_op(std::string key_orig,  bufferlist *bl, off_t read_len, off_t ofs, off_t read_ofs, void(*f)(sigval_t), rgw::Aio* aio, rgw::AioResult* r, string& location) {
    this->r = r;	
    this->aio = aio;
//    this->bl = bl;
    this->ofs = ofs;
    string tmp = key_orig;
	const char x = '/';
	const char y = '_';
	std::replace(tmp.begin(), tmp.end(), x, y);
	this->key = tmp;
    this->read_len = read_len;
    this->stat = EINPROGRESS;
	std::string loc = location+ "/" + this->key;
	//cout << "prepare_op  " << loc << "\n";
    struct aiocb *cb = new struct aiocb;
    memset(cb, 0, sizeof(struct aiocb));
    cb->aio_fildes = ::open(loc.c_str(), O_CLOEXEC|O_RDONLY);
    if (cb->aio_fildes < 0) {
      return -1;
    }
    cb->aio_buf = malloc(read_len);
    cb->aio_nbytes = read_len;
    cb->aio_offset = read_ofs;
    cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    cb->aio_sigevent.sigev_notify_function = f ;
    cb->aio_sigevent.sigev_notify_attributes = NULL;
    cb->aio_sigevent.sigev_value.sival_ptr = this;
    this->paiocb = cb;
    return 0;
  }

   int submit_op(){
    int ret = 0;
    if((ret = ::aio_read(this->paiocb)) != 0) {
          return ret;
         }
    return ret;
  }

  void release (){
    lock.lock();
    free((void *)paiocb->aio_buf);
    paiocb->aio_buf = nullptr;
    ::close(paiocb->aio_fildes);
    delete(paiocb);
    lock.unlock();
//    delete this;
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
    	//release();
	  return ECANCELED;
      }}
    stat = aio_error(paiocb);
    lock.unlock();
    return stat;
  }

  void finish(){
    bl->append((char*)paiocb->aio_buf, paiocb->aio_nbytes);
    onack->complete(0);
    release();
  }
};


typedef   void (*f)( RemoteRequest* func );
struct RemoteRequest : public CacheRequest{
  string dest;
  void *tp;
  RGWRESTConn *conn;
  string path;
  string ak;
  string sk;
  bool req_type; 
  //bufferlist pbl;// =nullptr;
  std::string s;
  size_t sizeleft;
  const char *readptr;
  f func; 
  cache_block *c_block;
  RemoteRequest() :  CacheRequest(), c_block(nullptr) , req_type(0){}


  ~RemoteRequest(){}
  int prepare_op(std::string key,  bufferlist *bl, off_t read_len, off_t ofs, off_t read_ofs, string dest, rgw::Aio* aio, rgw::AioResult* r, cache_block *c_block, string path, void(*f)(RemoteRequest*));

  void release (){
//    lock.lock();
//    lock.unlock();
  }

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  void finish(){
    lock.lock();
    bl->append(s.c_str(), s.size());
    s.clear();
    onack->complete(0);
    lock.unlock();
    delete this;
  }

  int status(){
    return 0;
  }

};  



class RemoteS3Request : public Task {
  public:
    RemoteS3Request(RemoteRequest *_req, CephContext *_cct) : Task(), req(_req), cct(_cct) {
      pthread_mutex_init(&qmtx,0);
      pthread_cond_init(&wcond, 0);
    }
    ~RemoteS3Request() {
      pthread_mutex_destroy(&qmtx);
      pthread_cond_destroy(&wcond);
    }
    virtual void run();
    virtual void set_handler(void *handle) {
      curl_handle = (CURL *)handle;
    }
    string sign_s3_request(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId);
    string get_date();
	string sign_s3_request2(string HTTP_Verb, string uri, string date, string YourSecretAccessKeyID, string AWSAccessKeyId);
  private:
    int submit_http_get_request_s3();
	int submit_http_put_request_s3();
  private:
    pthread_mutex_t qmtx;
    pthread_cond_t wcond;
    RemoteRequest *req;
    CephContext *cct;
    CURL *curl_handle;

};


#endif 
