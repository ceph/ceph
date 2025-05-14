#pragma once

#include "MDSNotificationMessage.h"
#include "MDSRank.h"
#include <boost/asio.hpp>

class MDSUDPEndpoint;

struct MDSUDPConnection {
  std::string ip;
  int port;
  MDSUDPConnection() = default;
  MDSUDPConnection(const std::string &ip, int port);
  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &iter);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<MDSUDPConnection *> &o);
};
WRITE_CLASS_ENCODER(MDSUDPConnection)

class MDSUDPManager {
public:
  MDSUDPManager(MDSRank *mds);
  int init();
  int send(const std::shared_ptr<MDSNotificationMessage> &message);
  int add_endpoint(const std::string &name, const MDSUDPConnection &connection,
                   bool write_into_disk);
  int remove_endpoint(const std::string &name, bool write_into_disk);

private:
  int load_data(std::map<std::string, bufferlist> &mp);
  int add_endpoint_into_disk(const std::string &name,
                             const MDSUDPConnection &connection);
  int remove_endpoint_from_disk(const std::string &name);
  int update_omap(const std::map<std::string, bufferlist> &mp);
  int remove_keys(const std::set<std::string> &st);
  void activate();
  void pause();
  CephContext *cct;
  std::shared_mutex endpoint_mutex;
  std::unordered_map<std::string, std::shared_ptr<MDSUDPEndpoint>> endpoints;
  static const size_t MAX_CONNECTIONS_DEFAULT = 256;
  MDSRank *mds;
  std::string object_name;
  std::atomic<bool> paused;
};

class MDSUDPEndpoint {
public:
  MDSUDPEndpoint() = delete;
  MDSUDPEndpoint(CephContext *cct, const std::string &name,
                 const MDSUDPConnection &connection);
  int publish_internal(std::vector<boost::asio::const_buffer> &buf,
                       uint64_t seq_id);
  static std::shared_ptr<MDSUDPEndpoint>
  create(CephContext *cct, const std::string &name,
         const MDSUDPConnection &connection);
  friend class MDSUDPManager;

private:
  std::string name;
  MDSUDPConnection connection;
  boost::asio::io_context io_context;
  boost::asio::ip::udp::socket socket;
  boost::asio::ip::udp::endpoint endpoint;
  CephContext *cct;
};