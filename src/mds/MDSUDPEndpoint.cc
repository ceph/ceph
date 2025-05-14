#include "MDSUDPEndpoint.h"
#include "include/fs_types.h"

#define dout_subsys ceph_subsys_mds

MDSUDPConnection::MDSUDPConnection(const std::string &ip, int port)
    : ip(ip), port(port) {}

void MDSUDPConnection::encode(ceph::buffer::list &bl) const {
  ENCODE_START(1, 1, bl);
  encode(ip, bl);
  encode(port, bl);
  ENCODE_FINISH(bl);
}

void MDSUDPConnection::dump(ceph::Formatter *f) const {
  f->dump_string("ip", ip);
  f->dump_bool("port", port);
}

void MDSUDPConnection::generate_test_instances(
    std::list<MDSUDPConnection *> &o) {
  o.push_back(new MDSUDPConnection);
}

void MDSUDPConnection::decode(ceph::buffer::list::const_iterator &iter) {
  DECODE_START(1, iter);
  decode(ip, iter);
  decode(port, iter);
  DECODE_FINISH(iter);
}

int MDSUDPManager::load_data(std::map<std::string, bufferlist> &mp) {
  int r = update_omap(std::map<std::string, bufferlist>());
  if (r < 0) {
    return r;
  }
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_get_vals("", "", UINT_MAX, &mp, NULL, NULL);
  mds->objecter->read(object_t(object_name),
                      object_locator_t(mds->get_metadata_pool()), op,
                      CEPH_NOSNAP, NULL, 0, &sync_finisher);
  r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error reading omap values from object '" << object_name
                    << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSUDPManager::update_omap(const std::map<std::string, bufferlist> &mp) {
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_set(mp);
  mds->objecter->mutate(
      object_t(object_name), object_locator_t(mds->get_metadata_pool()), op,
      SnapContext(), ceph::real_clock::now(), 0, &sync_finisher);
  int r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error updating omap of object '" << object_name
                    << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSUDPManager::remove_keys(const std::set<std::string> &st) {
  C_SaferCond sync_finisher;
  ObjectOperation op;
  op.omap_rm_keys(st);
  mds->objecter->mutate(
      object_t(object_name), object_locator_t(mds->get_metadata_pool()), op,
      SnapContext(), ceph::real_clock::now(), 0, &sync_finisher);
  int r = sync_finisher.wait();
  if (r < 0) {
    lderr(mds->cct) << "Error removing keys from omap of object '"
                    << object_name << "':" << cpp_strerror(r) << dendl;
  }
  return r;
}

int MDSUDPManager::add_endpoint_into_disk(const std::string &name,
                                          const MDSUDPConnection &connection) {
  std::map<std::string, bufferlist> mp;
  bufferlist bl;
  encode(connection, bl);
  mp[name] = std::move(bl);
  int r = update_omap(mp);
  return r;
}

int MDSUDPManager::remove_endpoint_from_disk(const std::string &name) {
  std::set<std::string> st;
  st.insert(name);
  int r = remove_keys(st);
  return r;
}

MDSUDPManager::MDSUDPManager(MDSRank *mds)
    : mds(mds), cct(mds->cct), object_name("mds_udp_endpoints"), paused(true) {}

int MDSUDPManager::init() {
  std::map<std::string, bufferlist> mp;
  int r = load_data(mp);
  if (r < 0) {
    lderr(cct) << "Error occurred while initilizing UDP endpoints" << dendl;
    return r;
  }
  for (auto &[key, val] : mp) {
    try {
      MDSUDPConnection connection;
      auto iter = val.cbegin();
      decode(connection, iter);
      add_endpoint(key, connection, false);
    } catch (const ceph::buffer::error &e) {
      ldout(cct, 1)
          << "No value exist in the omap of object 'mds_udp_endpoints' "
             "for udp entity name '"
          << key << "'" << dendl;
    }
  }
  return r;
}

int MDSUDPManager::send(
    const std::shared_ptr<MDSNotificationMessage> &message) {
  if (paused) {
    return -CEPHFS_ECANCELED;
  }
  std::shared_lock<std::shared_mutex> lock(endpoint_mutex);
  std::vector<boost::asio::const_buffer> buf(2);
  for (auto &[key, endpoint] : endpoints) {
    uint64_t len = message->message.length();
    buf[0] = boost::asio::buffer(&len, sizeof(len));
    buf[1] = boost::asio::buffer(message->message.c_str(),
                                 message->message.length());
    endpoint->publish_internal(buf, message->seq_id);
  }
  return 0;
}

int MDSUDPManager::add_endpoint(const std::string &name,
                                const MDSUDPConnection &connection,
                                bool write_into_disk) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  std::shared_ptr<MDSUDPEndpoint> new_endpoint;
  auto it = endpoints.find(name);
  int r = 0;
  if (it == endpoints.end() && endpoints.size() >= MAX_CONNECTIONS_DEFAULT) {
    ldout(cct, 1) << "UDP connect: max connections exceeded" << dendl;
    r = -CEPHFS_ENOMEM;
    goto error_occurred;
  }
  new_endpoint = MDSUDPEndpoint::create(cct, name, connection);
  if (!new_endpoint) {
    ldout(cct, 1) << "UDP connect: udp endpoint creation failed" << dendl;
    r = -CEPHFS_ECANCELED;
    goto error_occurred;
  }
  endpoints[name] = new_endpoint;
  if (write_into_disk) {
    r = add_endpoint_into_disk(name, connection);
    if (r < 0) {
      goto error_occurred;
    }
  }
  ldout(cct, 1) << "UDP endpoint with entity name '" << name
                << "' is added successfully" << dendl;
  activate();
  return r;
error_occurred:
  lderr(cct) << "UDP endpoint with entity name '" << name
             << "' can not be added, failed with an error:" << cpp_strerror(r)
             << dendl;
  return r;
}

void MDSUDPManager::activate() {
  paused = false;
}

int MDSUDPManager::remove_endpoint(const std::string &name,
                                   bool write_into_disk) {
  std::unique_lock<std::shared_mutex> lock(endpoint_mutex);
  int r = 0;
  auto it = endpoints.find(name);
  if (it != endpoints.end()) {
    endpoints.erase(it);
    if (write_into_disk) {
      r = remove_endpoint_from_disk(name);
    }
    if (r == 0) {
      ldout(cct, 1) << "UDP endpoint with entity name '" << name
                    << "' is removed successfully" << dendl;
    } else {
      lderr(cct) << "UDP endpoint '" << name
                 << "' can not be removed, failed with an error:"
                 << cpp_strerror(r) << dendl;
    }
    if (endpoints.empty()) {
      pause();
    }
    return r;
  }
  ldout(cct, 1) << "No UDP endpoint exist with entity name '" << name << "'"
                << dendl;
  return -CEPHFS_EINVAL;
}

void MDSUDPManager::pause() {
  paused = true;
}

MDSUDPEndpoint::MDSUDPEndpoint(CephContext *cct, const std::string &name,
                               const MDSUDPConnection &connection)
    : cct(cct), name(name), socket(io_context), connection(connection),
      endpoint(boost::asio::ip::address::from_string(connection.ip),
               connection.port) {
  try {
    boost::system::error_code ec;
    socket.open(boost::asio::ip::udp::v4(), ec);
    if (ec) {
      throw std::runtime_error(ec.message());
    }
  } catch (const std::exception &e) {
    lderr(cct) << "Error occurred while opening UDP socket with error:"
               << e.what() << dendl;
    throw;
  }
}

std::shared_ptr<MDSUDPEndpoint>
MDSUDPEndpoint::create(CephContext *cct, const std::string &name,
                       const MDSUDPConnection &connection) {
  try {
    std::shared_ptr<MDSUDPEndpoint> endpoint =
        std::make_shared<MDSUDPEndpoint>(cct, name, connection);
    return endpoint;
  } catch (...) {
  }
  return nullptr;
}

int MDSUDPEndpoint::publish_internal(
    std::vector<boost::asio::const_buffer> &buf, uint64_t seq_id) {
  boost::system::error_code ec;
  socket.send_to(buf, endpoint, 0, ec);
  if (ec) {
    ldout(cct, 1) << "Error occurred while sending notification having seq_id="
                  << seq_id << ":" << ec.message() << dendl;
    return -ec.value();
  } else {
    ldout(cct, 20) << "Notification having seq_id=" << seq_id << " delivered"
                   << dendl;
  }
  return 0;
}