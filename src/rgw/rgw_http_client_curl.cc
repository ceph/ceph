// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_http_client_curl.h"
#include <mutex>
#include <vector>
#include <curl/curl.h>

#include "rgw_common.h"
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#ifdef WITH_CURL_OPENSSL
#include <openssl/crypto.h>
#endif

#if defined(WITH_CURL_OPENSSL) && OPENSSL_API_COMPAT < 0x10100000L
namespace openssl {

class RGWSSLSetup
{
  std::vector <std::mutex> locks;
public:
  explicit RGWSSLSetup(int n) : locks (n){}

  void set_lock(int id){
    try {
      locks.at(id).lock();
    } catch (std::out_of_range& e) {
      dout(0) << __func__ << " failed to set locks" << dendl;
    }
  }

  void clear_lock(int id){
    try {
      locks.at(id).unlock();
    } catch (std::out_of_range& e) {
      dout(0) << __func__ << " failed to unlock" << dendl;
    }
  }
};


void rgw_ssl_locking_callback(int mode, int id, const char *file, int line)
{
  static RGWSSLSetup locks(CRYPTO_num_locks());
  if (mode & CRYPTO_LOCK)
    locks.set_lock(id);
  else
    locks.clear_lock(id);
}

unsigned long rgw_ssl_thread_id_callback(){
  return (unsigned long)pthread_self();
}

void init_ssl(){
  CRYPTO_set_id_callback((unsigned long (*) ()) rgw_ssl_thread_id_callback);
  CRYPTO_set_locking_callback(rgw_ssl_locking_callback);
}

} /* namespace openssl */
#endif // WITH_CURL_OPENSSL


namespace rgw {
namespace curl {

#if defined(WITH_CURL_OPENSSL) && OPENSSL_API_COMPAT < 0x10100000L
void init_ssl() {
  ::openssl::init_ssl();
}

bool fe_inits_ssl(boost::optional <const fe_map_t&> m, long& curl_global_flags){
  if (m) {
    for (const auto& kv: *m){
      if (kv.first == "beast"){
        std::string cert;
        kv.second->get_val("ssl_certificate","", &cert);
        if (!cert.empty()){
         /* TODO this flag is no op for curl > 7.57 */
          curl_global_flags &= ~CURL_GLOBAL_SSL;
          return true;
        }
      }
    }
  }
  return false;
}
#endif // WITH_CURL_OPENSSL

std::once_flag curl_init_flag;

void setup_curl(boost::optional<const fe_map_t&> m) {
  long curl_global_flags = CURL_GLOBAL_ALL;

  #if defined(WITH_CURL_OPENSSL) && OPENSSL_API_COMPAT < 0x10100000L
  if (!fe_inits_ssl(m, curl_global_flags))
    init_ssl();
  #endif

  std::call_once(curl_init_flag, curl_global_init, curl_global_flags);
  rgw_setup_saved_curl_handles();
}

void cleanup_curl() {
  rgw_release_all_curl_handles();
  curl_global_cleanup();
}

} /* namespace curl */
} /* namespace rgw */
