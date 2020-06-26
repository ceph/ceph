

#include "rgw_cacherequest.h"
#include "rgw_rest_client.h"
#define COPY_BUF_SIZE (4 * 1024 * 1024)
#include <errno.h>
#define dout_subsys ceph_subsys_rgw

class RGWRadosGetObj2 : public RGWHTTPStreamRWRequest::ReceiveCB
{
  CephContext* cct;
  bufferlist extra_data_bl;
  bufferlist chunk_buffer;
  string key;
  uint64_t chunk_size = COPY_BUF_SIZE;
  uint64_t extra_data_left{0};
  RGWRados *store;
  public:
  bufferlist *pbl;
  rgw::AioResult* r;
  rgw::Aio* aio ;
  RGWRadosGetObj2(CephContext* cct, RGWRados* store) :  cct(cct) , store(store) {}
  
  int handle_data(bufferlist& bl, bool *pause) override {
    uint64_t size = bl.length();
    chunk_buffer.append(bl);
    ldout(cct, 0) << "RGWRadosGetObj1 length()" << bl.length() <<"id "<< r->id <<dendl;
    if (chunk_buffer.length() >= chunk_size){
      bufferlist tmp;
      //chunk_buffer.splice(0, chunk_size, &tmp);
      chunk_buffer.splice(0, chunk_size, pbl);
//      r->data = pbl;
      r->result = 0;
      //req->r->result = 0;
      aio->put(*(r));
  //req->aio->put(*(req->r));
      ldout(cct, 0) << "RGWRadosGetObj2 length()" << pbl->length()  <<dendl;
      ldout(cct, 0) << "RGWRadosGetObj2 length()" << r->data.length() << "id "<< r->id <<dendl;
      //int ret = store->put_data(key, tmp , tmp.length());
    }
    return 0;
  }
  bufferlist& get_extra_data() { return extra_data_bl; }
  void set_extra_data_len(uint64_t len) override {
    extra_data_left = len;
    ldout(cct, 0) << "RGWRadosGetObj2 set_extra_data_len" << len  <<dendl;
    RGWHTTPStreamRWRequest::ReceiveCB::set_extra_data_len(len);
  }
};



int RemoteRequest::submit_op(){
	
  ldout(cct, 0) << __func__ << "RemoteRequest::submit_op " << ofs<< dendl;
  RGWRadosGetObj2 *cb2 = new RGWRadosGetObj2(cct, store);
  cb2->r = r;
  cb2->aio = aio;
  cb2->pbl = bl; 
  list<string> endpoints;
  //endpoints.push_back(c_obj->destination);
  endpoints.push_back(c_obj->host); //FIXME: Ugur should it be host?
  RGWRESTConn *conn = new RGWRESTConn(this->cct, nullptr, "", endpoints, c_obj->accesskey);
  real_time set_mtime;
  uint64_t expected_size = 0;
  bool prepend_metadata = true;
  bool rgwx_stat = false;
  bool skip_decrypt =false;
  bool get_op = true;
  bool sync_manifest =false;
  bool send = true;
  int  ret = conn->get_obj(c_obj->user, ofs, read_len, obj, prepend_metadata,
      get_op, rgwx_stat, sync_manifest, skip_decrypt, send, cb2, &in_stream_req);
  if (ret < 0 )
    return ret;
  ret = conn->complete_request(in_stream_req, nullptr, &set_mtime, &expected_size, nullptr, nullptr);
  if (ret < 0 )
    return ret;
  ldout(cct, 0) << __func__ << "girdi completed sonrasi " << ofs<< dendl;
//  ldout(cct, 0) << __func__ << "girdi completed sonrasi pl.length()) " << pl.length()<< dendl;
/*  bl->append(pl);
  */
 //  sleep(3);
//   r->result = 0;
//   aio->put(*(r));
  return 0;
}
int RemoteRequest::prepare_op(std::string key,  bufferlist *bl, int read_len, int ofs, int read_ofs, string dest, rgw::Aio* aio, rgw::AioResult* r){
  this->r = r;
  this->aio = aio;
  this->bl = bl;
  this->ofs = ofs;
  this->key = key;
  this->read_len = read_len;
  this->dest = dest;
  return 0;
}

