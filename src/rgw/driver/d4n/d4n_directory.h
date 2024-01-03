#pragma once

#include "rgw_common.h"

#include <boost/lexical_cast.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

namespace rgw { namespace d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  std::string creationTime; /* Creation time of the S3 Object */
  bool dirty;
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t blockID;
  std::string version;
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0; /* LFUDA policy variable */
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of block locations */
};

class Directory {
  public:
    CephContext* cct;

    Directory() {}
};

class ObjectDirectory: public Directory {
  public:
    ObjectDirectory(net::io_context& io_context) {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
    }
    ~ObjectDirectory() {
      shutdown();
    }

    int init(CephContext* cct, const DoutPrefixProvider* dpp) {
      this->cct = cct;

      config cfg;
      cfg.addr.host = cct->_conf->rgw_d4n_host;
      cfg.addr.port = std::to_string(cct->_conf->rgw_d4n_port);
      cfg.clientname = "D4N.ObjectDir";

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	ldpp_dout(dpp, 10) << "ObjectDirectory::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
	return -EDESTADDRREQ;
      }
      
      conn->async_run(cfg, {}, net::consign(net::detached, conn)); 

      return 0;
    }
    int exist_key(CacheObj* object, optional_yield y);
    void shutdown();

    int set(CacheObj* object, optional_yield y);
    int get(CacheObj* object, optional_yield y);
    int copy(CacheObj* object, std::string copyName, std::string copyBucketName, optional_yield y);
    int del(CacheObj* object, optional_yield y);
    int update_field(CacheObj* object, std::string field, std::string value, optional_yield y);

  private:
    std::shared_ptr<connection> conn;

    std::string build_index(CacheObj* object);
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(net::io_context& io_context) {
      conn = std::make_shared<connection>(boost::asio::make_strand(io_context));
    }
    ~BlockDirectory() {
      shutdown();
    }
    
    int init(CephContext* cct, const DoutPrefixProvider* dpp) {
      this->cct = cct;

      config cfg;
      cfg.addr.host = cct->_conf->rgw_d4n_host;
      cfg.addr.port = std::to_string(cct->_conf->rgw_d4n_port);
      cfg.clientname = "D4N.BlockDir";

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	ldpp_dout(dpp, 10) << "BlockDirectory::" << __func__ << "(): Endpoint was not configured correctly." << dendl;
	return -EDESTADDRREQ;
      }

      conn->async_run(cfg, {}, net::consign(net::detached, conn)); 

      return 0;
    }
    int exist_key(CacheBlock* block, optional_yield y);
    void shutdown();

    int set(CacheBlock* block, optional_yield y);
    int get(CacheBlock* block, optional_yield y);
    int copy(CacheBlock* block, std::string copyName, std::string copyBucketName, optional_yield y);
    int del(CacheBlock* block, optional_yield y);
    int update_field(CacheBlock* block, std::string field, std::string value, optional_yield y);
    int remove_host(CacheBlock* block, std::string value, optional_yield y);

  private:
    std::shared_ptr<connection> conn;

    std::string build_index(CacheBlock* block);
};

} } // namespace rgw::d4n
