// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/intrusive/list.hpp>
#include <atomic>
#include <mutex>
#include <string.h>

#include "include/compat.h"
#include "common/errno.h"
#include "rgw_common.h"
#include "rgw_kmip_client.h"
#include "rgw_kmip_client_impl.h"

#include <openssl/err.h>
#include <openssl/ssl.h>
extern "C" {
#include "kmip.h"
#include "kmip_bio.h"
#include "kmip_memset.h"
};

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

static enum kmip_version protocol_version = KMIP_1_0;

struct RGWKmipHandle {
  int uses;
  mono_time lastuse;
  SSL_CTX *ctx;
  SSL *ssl;
  BIO *bio;
  KMIP kmip_ctx[1];
  TextString textstrings[2];
  UsernamePasswordCredential upc[1];
  Credential credential[1];
  int need_to_free_kmip;
  size_t buffer_blocks, buffer_block_size, buffer_total_size;
  uint8 *encoding;

  explicit RGWKmipHandle() :
    uses(0), ctx(0), ssl(0), bio(0),
    need_to_free_kmip(0),
    encoding(0) {
      memset(kmip_ctx, 0, sizeof kmip_ctx);
      memset(textstrings, 0, sizeof textstrings);
      memset(upc, 0, sizeof upc);
      memset(credential, 0, sizeof credential);
  };
};

struct RGWKmipWorker: public Thread {
  RGWKMIPManagerImpl &m;
  RGWKmipWorker(RGWKMIPManagerImpl& m) : m(m) {}
  void *entry() override;
  void signal() {
    std::lock_guard l{m.lock};
    m.cond.notify_all();
  }
};

static void
kmip_free_handle_stuff(RGWKmipHandle *kmip)
{
  if (kmip->encoding) {
    kmip_free_buffer(kmip->kmip_ctx,
      kmip->encoding,
      kmip->buffer_total_size);
    kmip_set_buffer(kmip->kmip_ctx, NULL, 0);
  }
  if (kmip->need_to_free_kmip)
    kmip_destroy(kmip->kmip_ctx);
  if (kmip->bio)
    BIO_free_all(kmip->bio);
  if (kmip->ctx)
    SSL_CTX_free(kmip->ctx);
}

class RGWKmipHandleBuilder {
private:
  CephContext *cct;
  const char *clientcert = 0;
  const char *clientkey = 0;
  const char *capath = 0;
  const char *host = 0;
  const char *portstring = 0;
  const char *username = 0;
  const char *password = 0;
public:
  RGWKmipHandleBuilder(CephContext *cct) : cct(cct) {};
  RGWKmipHandleBuilder& set_clientcert(const std::string &v) {
    const char *s = v.c_str();
    if (*s) {
      clientcert = s;
    }
    return *this;
  }
  RGWKmipHandleBuilder& set_clientkey(const std::string &v) {
    const char *s = v.c_str();
    if (*s) {
      clientkey = s;
    }
    return *this;
  }
  RGWKmipHandleBuilder& set_capath(const std::string &v) {
    const char *s = v.c_str();
    if (*s) {
      capath = s;
    }
    return *this;
  }
  RGWKmipHandleBuilder& set_host(const char *v) {
    host = v;
    return *this;
  }
  RGWKmipHandleBuilder& set_portstring(const char *v) {
    portstring = v;
    return *this;
  }
  RGWKmipHandleBuilder& set_username(const std::string &v) {
    const char *s = v.c_str();
    if (*s) {
      username = s;
    }
    return *this;
  }
  RGWKmipHandleBuilder& set_password(const std::string& v) {
    const char *s = v.c_str();
    if (*s) {
      password = s;
    }
    return *this;
  }
  RGWKmipHandle *build() const;
};

static int
kmip_write_an_error_helper(const char *s, size_t l, void *u) {
  CephContext *cct = (CephContext *)u;
  std::string_view es(s, l);
  lderr(cct) << es << dendl;
  return l;
}

void
ERR_print_errors_ceph(CephContext *cct)
{
  ERR_print_errors_cb(kmip_write_an_error_helper, cct);
}

RGWKmipHandle *
RGWKmipHandleBuilder::build() const
{
  int failed = 1;
  RGWKmipHandle *r = new RGWKmipHandle();
  TextString *up = 0;
	size_t ns;

  r->ctx = SSL_CTX_new(TLS_client_method());

  if (!clientcert)
    ;
  else if (SSL_CTX_use_certificate_file(r->ctx, clientcert, SSL_FILETYPE_PEM) != 1) {
    lderr(cct) << "ERROR: can't load client cert from "
      << clientcert << dendl;
    ERR_print_errors_ceph(cct);
    goto Done;
  }

  if (!clientkey)
    ;
  else if (SSL_CTX_use_PrivateKey_file(r->ctx, clientkey,
      SSL_FILETYPE_PEM) != 1) {
    lderr(cct) << "ERROR: can't load client key from "
      << clientkey << dendl;
    ERR_print_errors_ceph(cct);
    goto Done;
  }

  if (!capath)
    ;
  else if (SSL_CTX_load_verify_locations(r->ctx, capath, NULL) != 1) {
    lderr(cct) << "ERROR: can't load cacert from "
      << capath << dendl;
    ERR_print_errors_ceph(cct);
    goto Done;
  }
  r->bio = BIO_new_ssl_connect(r->ctx);
  if (!r->bio) {
    lderr(cct) << "BIO_new_ssl_connect failed" << dendl;
    goto Done;
  }
  BIO_get_ssl(r->bio, &r->ssl);
  SSL_set_mode(r->ssl, SSL_MODE_AUTO_RETRY);

  BIO_set_conn_hostname(r->bio, host);
  BIO_set_conn_port(r->bio, portstring);
  if (BIO_do_connect(r->bio) != 1) {
    lderr(cct) << "BIO_do_connect failed to " << host
      << ":" << portstring << dendl;
    ERR_print_errors_ceph(cct);
    goto Done;
  }

  // setup kmip

  kmip_init(r->kmip_ctx, NULL, 0, protocol_version);
	r->need_to_free_kmip = 1;
	r->buffer_blocks = 1;
	r->buffer_block_size = 1024;
	r->encoding = static_cast<uint8*>(r->kmip_ctx->calloc_func(
    r->kmip_ctx->state, r->buffer_blocks, r->buffer_block_size));
	if (!r->encoding) {
		lderr(cct) << "kmip buffer alloc failed: "
      << r->buffer_blocks <<
      " * " << r->buffer_block_size << dendl;
		goto Done;
	}
	ns = r->buffer_blocks * r->buffer_block_size;
	kmip_set_buffer(r->kmip_ctx, r->encoding, ns);
	r->buffer_total_size = ns;

  up = r->textstrings;
  if (username) {
    memset(r->upc, 0, sizeof *r->upc);
    up->value = (char *) username;
    up->size = strlen(username);
    r->upc->username = up++;
    if (password) {
      up->value = (char *) password;
      up->size = strlen(password);
      r->upc->password = up++;
    }
    r->credential->credential_type = KMIP_CRED_USERNAME_AND_PASSWORD;
    r->credential->credential_value = r->upc;
    int i = kmip_add_credential(r->kmip_ctx, r->credential);
    if (i != KMIP_OK) {
      fprintf(stderr,"failed to add credential to kmip\n");
      goto Done;
    }
  }

  failed = 0;
Done:
  if(failed && r) {
    kmip_free_handle_stuff(r);
    delete r;
    r = 0;
  }
  return r;
}

struct RGWKmipHandles : public Thread {
  CephContext *cct;
  ceph::mutex cleaner_lock = ceph::make_mutex("RGWKmipHandles::cleaner_lock");
  std::vector<RGWKmipHandle*> saved_kmip;
  int cleaner_shutdown;
  bool cleaner_active = false;
  ceph::condition_variable cleaner_cond;
  RGWKmipHandles(CephContext *cct) :
    cct(cct), cleaner_shutdown{0} {
  }
  RGWKmipHandle* get_kmip_handle();
  void release_kmip_handle_now(RGWKmipHandle* kmip);
  void release_kmip_handle(RGWKmipHandle* kmip);
  void flush_kmip_handles();
  int do_one_entry(RGWKMIPTransceiver &element);
  void* entry();
  void start();
  void stop();
};

RGWKmipHandle*
RGWKmipHandles::get_kmip_handle()
{
  RGWKmipHandle* kmip = 0;
  const char *hostaddr = cct->_conf->rgw_crypt_kmip_addr.c_str();
  {
    std::lock_guard lock{cleaner_lock};
    if (!saved_kmip.empty()) {
      kmip = *saved_kmip.begin();
      saved_kmip.erase(saved_kmip.begin());
    }
  }
  if (!kmip && hostaddr) {
    char *hosttemp = strdup(hostaddr);
    char *port = strchr(hosttemp, ':');
    if (port)
      *port++ = 0;
    kmip = RGWKmipHandleBuilder{cct}
      .set_clientcert(cct->_conf->rgw_crypt_kmip_client_cert)
      .set_clientkey(cct->_conf->rgw_crypt_kmip_client_key)
      .set_capath(cct->_conf->rgw_crypt_kmip_ca_path)
      .set_host(hosttemp)
      .set_portstring(port ? port : "5696")
      .set_username(cct->_conf->rgw_crypt_kmip_username)
      .set_password(cct->_conf->rgw_crypt_kmip_password)
      .build();
    free(hosttemp);
  }
  return kmip;
}

void
RGWKmipHandles::release_kmip_handle_now(RGWKmipHandle* kmip)
{
  kmip_free_handle_stuff(kmip);
  delete kmip;
}

#define MAXIDLE 5
void
RGWKmipHandles::release_kmip_handle(RGWKmipHandle* kmip)
{
  if (cleaner_shutdown) {
    release_kmip_handle_now(kmip);
  } else {
    std::lock_guard lock{cleaner_lock};
    kmip->lastuse = mono_clock::now();
    saved_kmip.insert(saved_kmip.begin(), 1, kmip);
  }
}

void*
RGWKmipHandles::entry()
{
  RGWKmipHandle* kmip;
  std::unique_lock lock{cleaner_lock};

  for (;;) {
    if (cleaner_shutdown) {
      if (saved_kmip.empty())
	break;
    } else {
      cleaner_cond.wait_for(lock, std::chrono::seconds(MAXIDLE));
    }
    mono_time now = mono_clock::now();
    while (!saved_kmip.empty()) {
      auto cend = saved_kmip.end();
      --cend;
      kmip = *cend;
      if (!cleaner_shutdown && now - kmip->lastuse
	  < std::chrono::seconds(MAXIDLE))
	break;
      saved_kmip.erase(cend);
      release_kmip_handle_now(kmip);
    }
  }
  return nullptr;
}

void
RGWKmipHandles::start()
{
  std::lock_guard lock{cleaner_lock};
  if (!cleaner_active) {
    cleaner_active = true;
    this->create("KMIPcleaner");  // len<16!!!
  }
}

void
RGWKmipHandles::stop()
{
  std::unique_lock lock{cleaner_lock};
  cleaner_shutdown = 1;
  cleaner_cond.notify_all();
  if (cleaner_active) {
    lock.unlock();
    this->join();
    cleaner_active = false;
  }
}

void
RGWKmipHandles::flush_kmip_handles()
{
  stop();
  join();
  if (!saved_kmip.empty()) {
    ldout(cct, 0) << "ERROR: " << __func__ << " failed final cleanup" << dendl;
  }
  saved_kmip.shrink_to_fit();
}

int
RGWKMIPManagerImpl::start()
{
  if (worker) {
    lderr(cct) << "kmip worker already started" << dendl;
    return -1;
  }
  worker = new RGWKmipWorker(*this);
  worker->create("kmip worker");
  return 0;
}

void
RGWKMIPManagerImpl::stop()
{
  going_down = true;
  if (worker) {
    worker->signal();
    worker->join();
    delete worker;
    worker = 0;
  }
}

int
RGWKMIPManagerImpl::add_request(RGWKMIPTransceiver *req)
{
  std::unique_lock l{lock};
  if (going_down)
    return -ECANCELED;
  // requests is a boost::intrusive::list, which manages pointers and does not copy the instance
  // coverity[leaked_storage:SUPPRESS]
  // coverity[uninit_use_in_call:SUPPRESS]
  requests.push_back(*new Request{*req});
  l.unlock();
  if (worker)
    worker->signal();
  return 0;
}

int
RGWKmipHandles::do_one_entry(RGWKMIPTransceiver &element)
{
  auto h = get_kmip_handle();
  std::unique_lock l{element.lock};
  Attribute a[8], *ap;
  TextString nvalue[1], uvalue[1];
  Name nattr[1];
  enum cryptographic_algorithm alg = KMIP_CRYPTOALG_AES;
  int32 length = 256;
  int32 mask = KMIP_CRYPTOMASK_ENCRYPT | KMIP_CRYPTOMASK_DECRYPT;
  size_t ns;
  ProtocolVersion pv[1];
  RequestHeader rh[1];
  RequestMessage rm[1];
  Authentication auth[1];
  ResponseMessage resp_m[1];
  int i;
  union {
    CreateRequestPayload create_req[1];
    LocateRequestPayload locate_req[1];
    GetRequestPayload get_req[1];
    GetAttributeListRequestPayload lsattrs_req[1];
    GetAttributesRequestPayload getattrs_req[1];
  } u[1];
  RequestBatchItem rbi[1];
  TemplateAttribute ta[1];
  const char *what = "?";
  int need_to_free_response = 0;
  char *response = NULL;
  int response_size = 0;
  enum result_status rs;
  ResponseBatchItem *req;

  if (!h) {
    element.ret = -ERR_SERVICE_UNAVAILABLE;
    return element.ret;
  }
  memset(a, 0, sizeof *a);
  for (i = 0; i < (int)(sizeof a/sizeof *a); ++i)
    kmip_init_attribute(a+i);
  ap = a;
  switch(element.operation) {
  case RGWKMIPTransceiver::CREATE:
    ap->type = KMIP_ATTR_CRYPTOGRAPHIC_ALGORITHM;
    ap->value = &alg;
    ++ap;
    ap->type = KMIP_ATTR_CRYPTOGRAPHIC_LENGTH;
    ap->value = &length;
    ++ap;
    ap->type = KMIP_ATTR_CRYPTOGRAPHIC_USAGE_MASK;
    ap->value = &mask;
    ++ap;
    break;
  default:
    break;
  }
  if (element.name) {
    memset(nvalue, 0, sizeof *nvalue);
    nvalue->value = element.name;
    nvalue->size = strlen(element.name);
    memset(nattr, 0, sizeof *nattr);
    nattr->value = nvalue;
    nattr->type = KMIP_NAME_UNINTERPRETED_TEXT_STRING;
    ap->type = KMIP_ATTR_NAME;
    ap->value = nattr;
    ++ap;
  }
  if (element.unique_id) {
    memset(uvalue, 0, sizeof *uvalue);
    uvalue->value = element.unique_id;
    uvalue->size = strlen(element.unique_id);
  }
  memset(pv, 0, sizeof *pv);
  memset(rh, 0, sizeof *rh);
  memset(rm, 0, sizeof *rm);
  memset(auth, 0, sizeof *auth);
  memset(resp_m, 0, sizeof *resp_m);
  kmip_init_protocol_version(pv, h->kmip_ctx->version);
  kmip_init_request_header(rh);
  rh->protocol_version = pv;
  rh->maximum_response_size = h->kmip_ctx->max_message_size;
  rh->time_stamp = time(NULL);
  rh->batch_count = 1;
  memset(rbi, 0, sizeof *rbi);
  kmip_init_request_batch_item(rbi);
  memset(u, 0, sizeof *u);
  rbi->request_payload = u;
  switch(element.operation) {
  case RGWKMIPTransceiver::CREATE:
    memset(ta, 0, sizeof *ta);
    ta->attributes = a;
    ta->attribute_count = ap-a;
    u->create_req->object_type = KMIP_OBJTYPE_SYMMETRIC_KEY;
    u->create_req->template_attribute = ta;
    rbi->operation = KMIP_OP_CREATE;
    what = "create";
    break;
  case RGWKMIPTransceiver::GET:
    if (element.unique_id)
      u->get_req->unique_identifier = uvalue;
    rbi->operation = KMIP_OP_GET;
    what = "get";
    break;
  case RGWKMIPTransceiver::LOCATE:
    if (ap > a) {
      u->locate_req->attributes = a;
      u->locate_req->attribute_count = ap - a;
    }
    rbi->operation = KMIP_OP_LOCATE;
    what = "locate";
    break;
  case RGWKMIPTransceiver::GET_ATTRIBUTES:
  case RGWKMIPTransceiver::GET_ATTRIBUTE_LIST:
  case RGWKMIPTransceiver::DESTROY:
  default:
    lderr(cct) << "Missing operation logic op=" << element.operation << dendl;
    element.ret = -EINVAL;
    goto Done;
  }
  rm->request_header = rh;
  rm->batch_items = rbi;
  rm->batch_count = 1;
  if (h->kmip_ctx->credential_list) {
    LinkedListItem *item = h->kmip_ctx->credential_list->head;
    if (item) {
      auth->credential = (Credential *)item->data;
      rh->authentication = auth;
    }
  }
  for (;;) {
    i = kmip_encode_request_message(h->kmip_ctx, rm);
    if (i != KMIP_ERROR_BUFFER_FULL) break;
    h->kmip_ctx->free_func(h->kmip_ctx->state, h->encoding);
    h->encoding = 0;
    ++h->buffer_blocks;
    h->encoding = static_cast<uint8*>(h->kmip_ctx->calloc_func(h->kmip_ctx->state, h->buffer_blocks, h->buffer_block_size));
    if (!h->encoding) {
      lderr(cct) << "kmip buffer alloc failed: "
	<< h->buffer_blocks
	<< " * " << h->buffer_block_size << dendl;
      element.ret = -ENOMEM;
      goto Done;
    }
    ns = h->buffer_blocks * h->buffer_block_size;
    kmip_set_buffer(h->kmip_ctx, h->encoding, ns);
    h->buffer_total_size = ns;
  }
  if (i != KMIP_OK) {
    lderr(cct) << " Failed to encode " << what
      << " request; err=" << i
      << " ctx error message " << h->kmip_ctx->error_message
      << dendl;
    element.ret = -EINVAL;
    goto Done;
  }
  i = kmip_bio_send_request_encoding(h->kmip_ctx, h->bio,
    (char*)h->encoding,
    h->kmip_ctx->index - h->kmip_ctx->buffer,
    &response, &response_size);
  if (i < 0) {
    lderr(cct) << "Problem sending request to " << what << " " << i << " context error message " << h->kmip_ctx->error_message << dendl;
    element.ret = -EINVAL;
    goto Done;
  }
  kmip_free_buffer(h->kmip_ctx, h->encoding,
    h->buffer_total_size);
  h->encoding = 0;
  kmip_set_buffer(h->kmip_ctx, response, response_size);
  need_to_free_response = 1;
  i = kmip_decode_response_message(h->kmip_ctx, resp_m);
  if (i != KMIP_OK) {
    lderr(cct) << "Failed to decode " << what << " " << i << " context error message " << h->kmip_ctx->error_message << dendl;
    element.ret = -EINVAL;
    goto Done;
  }
  if (resp_m->batch_count != 1) {
    lderr(cct) << "Failed; weird response count doing " << what << " " << resp_m->batch_count << dendl;
    element.ret = -EINVAL;
    goto Done;
  }
  req = resp_m->batch_items;
  rs = req->result_status;
  if (rs != KMIP_STATUS_SUCCESS) {
    lderr(cct) << "Failed; result status not success " << rs << dendl;
    element.ret = -EINVAL;
    goto Done;
  }
  if (req->operation != rbi->operation) {
    lderr(cct) << "Failed; response operation mismatch, got " << req->operation << " expected " << rbi->operation << dendl;
    element.ret = -EINVAL;
    goto Done;
  }
  switch(req->operation)
  {
  case KMIP_OP_CREATE: {
      CreateResponsePayload *pld = (CreateResponsePayload *)req->response_payload;
      element.out = static_cast<char *>(malloc(pld->unique_identifier->size+1));
      memcpy(element.out, pld->unique_identifier->value, pld->unique_identifier->size);
      element.out[pld->unique_identifier->size] = 0;
    } break;
  case KMIP_OP_LOCATE: {
      LocateResponsePayload *pld = (LocateResponsePayload *)req->response_payload;
      char **list = static_cast<char **>(malloc(sizeof (char*) * (1 + pld->unique_identifiers_count)));
      for (i = 0; i < pld->unique_identifiers_count; ++i) {
	list[i] = static_cast<char *>(malloc(pld->unique_identifiers[i].size+1));
	memcpy(list[i], pld->unique_identifiers[i].value, pld->unique_identifiers[i].size);
	list[i][pld->unique_identifiers[i].size] = 0;
      }
      list[i] = 0;
      element.outlist->strings = list;
      element.outlist->string_count = pld->unique_identifiers_count;
    } break;
  case KMIP_OP_GET: {
      GetResponsePayload *pld = (GetResponsePayload *)req->response_payload;
      element.out = static_cast<char *>(malloc(pld->unique_identifier->size+1));
      memcpy(element.out, pld->unique_identifier->value, pld->unique_identifier->size);
      element.out[pld->unique_identifier->size] = 0;
      if (pld->object_type != KMIP_OBJTYPE_SYMMETRIC_KEY) {
	lderr(cct) << "get: expected symmetric key got " << pld->object_type << dendl;
	element.ret = -EINVAL;
	goto Done;
      }
      KeyBlock *kp = static_cast<SymmetricKey *>(pld->object)->key_block;
      ByteString *bp;
      if (kp->key_format_type != KMIP_KEYFORMAT_RAW) {
	lderr(cct) << "get: expected raw key format got  " << kp->key_format_type << dendl;
	element.ret = -EINVAL;
	goto Done;
      }
      KeyValue *kv = static_cast<KeyValue *>(kp->key_value);
      bp  = static_cast<ByteString*>(kv->key_material);
      element.outkey->data = static_cast<unsigned char *>(malloc(bp->size));
      element.outkey->keylen = bp->size;
      memcpy(element.outkey->data, bp->value, bp->size);
    } break;
  case KMIP_OP_GET_ATTRIBUTES: {
      GetAttributesResponsePayload *pld = (GetAttributesResponsePayload *)req->response_payload;
      element.out = static_cast<char *>(malloc(pld->unique_identifier->size+1));
      memcpy(element.out, pld->unique_identifier->value, pld->unique_identifier->size);
      element.out[pld->unique_identifier->size] = 0;
    } break;
  case KMIP_OP_GET_ATTRIBUTE_LIST: {
      GetAttributeListResponsePayload *pld = (GetAttributeListResponsePayload *)req->response_payload;
      element.out = static_cast<char *>(malloc(pld->unique_identifier->size+1));
      memcpy(element.out, pld->unique_identifier->value, pld->unique_identifier->size);
      element.out[pld->unique_identifier->size] = 0;
    } break;
  case KMIP_OP_DESTROY: {
      DestroyResponsePayload *pld = (DestroyResponsePayload *)req->response_payload;
      element.out = static_cast<char *>(malloc(pld->unique_identifier->size+1));
      memcpy(element.out, pld->unique_identifier->value, pld->unique_identifier->size);
      element.out[pld->unique_identifier->size] = 0;
    } break;
  default:
    lderr(cct) << "Missing response logic op=" << element.operation << dendl;
    element.ret = -EINVAL;
    goto Done;
  }
  element.ret = 0;
Done:
  if (need_to_free_response)
    kmip_free_response_message(h->kmip_ctx, resp_m);
  element.done = true;
  element.cond.notify_all();
  release_kmip_handle(h);
  return element.ret;
}

void *
RGWKmipWorker::entry()
{
  std::unique_lock entry_lock{m.lock};
  ldout(m.cct, 10) << __func__ << " start" << dendl;
  RGWKmipHandles handles{m.cct};
  handles.start();
  while (!m.going_down) {
    if (m.requests.empty()) {
      m.cond.wait_for(entry_lock, std::chrono::seconds(MAXIDLE));
      continue;
    }
    auto iter = m.requests.begin();
    auto element = *iter;
    m.requests.erase(iter);
    entry_lock.unlock();
    (void) handles.do_one_entry(element.details);
    entry_lock.lock();
  }
  for (;;) {
    if (m.requests.empty()) break;
    auto iter = m.requests.begin();
    auto element = std::move(*iter);
    m.requests.erase(iter);
    element.details.ret = -666;
    element.details.done = true;
    element.details.cond.notify_all();
  }
  handles.stop();
  ldout(m.cct, 10) << __func__ << " finish" << dendl;
  return nullptr;
}
