#include "EventHandler.h"
#include "EventOp.h"
#include "MemoryManager.h"
#include "RpmaOp.h"
#include "Types.h"

#include <librpma.h>
#include <string>
#include <errno.h>
#include <memory>
#include <thread>
#include <chrono>
#include <algorithm>

#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::" << this->name() << ": " << this << " " \
                           << __func__ << ": "

namespace librbd::cache::pwl::rwl::replica {

/*
 * common_peer_via_address -- create a new RPMA peer based on ibv_context
 * received by the provided address
 */
static int common_peer_via_address(const char *addr,
                                   enum rpma_util_ibv_context_type type,
                                   struct rpma_peer **peer_ptr)
{
	struct ibv_context *dev = NULL;

	int ret = rpma_utils_get_ibv_context(addr, type, &dev);
	if (ret)
		return ret;

	// create a new peer object
	return rpma_peer_new(dev, peer_ptr);
}

void RpmaPeerDeleter::operator() (struct rpma_peer *peer_ptr) {
  rpma_peer_delete(&peer_ptr);
}

void RpmaEpDeleter::operator() (struct rpma_ep *ep_ptr) {
  rpma_ep_shutdown(&ep_ptr);
}

void RpmaMRDeleter::operator() (struct rpma_mr_local *mr_ptr) {
  rpma_mr_dereg(&mr_ptr);
}

void RpmaRemoteMRDeleter::operator() (struct rpma_mr_remote *mr_ptr) {
  rpma_mr_remote_delete(&mr_ptr);
}

void RpmaConnCfgDeleter::operator() (struct rpma_conn_cfg *cfg_ptr) {
  rpma_conn_cfg_delete(&cfg_ptr);
}

RpmaConn::~RpmaConn() {
  if (conn == nullptr) {
    return ;
  }
  if (!disconnected.load()) {
    rpma_conn_disconnect(conn);
  }
  rpma_conn_delete(&conn);
  conn = nullptr;
}

void RpmaConn::reset(struct rpma_conn *conn) {
  this->conn = conn;
  disconnected = false;
}

struct rpma_conn* RpmaConn::get() const {
  return conn;
}

int RpmaConn::disconnect() {
  if (!disconnected.load()) {
    disconnected.store(true);
    return rpma_conn_disconnect(conn);
  }
  return 0;
}

AcceptorHandler::AcceptorHandler(CephContext *cct,
                                 const std::string& addr,
                                 const std::string& port,
                                 const std::weak_ptr<Reactor> reactor_manager)
  : EventHandlerInterface(cct, reactor_manager), _address(addr), _port(port)
 {
  int ret = 0;
  struct rpma_peer *peer = nullptr;
  ret = common_peer_via_address(addr.c_str(), RPMA_UTIL_IBV_CONTEXT_LOCAL, &peer);
  if (ret) {
    throw std::runtime_error("lookup an ibv_context via the address and create a new peer using it failed");
  }
  _peer.reset(peer, RpmaPeerDeleter());

  struct rpma_ep *ep = nullptr;
  ret = rpma_ep_listen(peer, addr.c_str(), port.c_str(), &ep);
  if (ret) {
    throw std::runtime_error("listening endpoint at addr:port failed");
  }
  _ep.reset(ep);

  ret = rpma_ep_get_fd(ep, &_fd);
  if (ret) {
    throw std::runtime_error("get the endpoint's event file descriptor failed");
  }
}

AcceptorHandler::~AcceptorHandler() {
  ldout(_cct, 20) << dendl;
}

int AcceptorHandler::register_self() {
  if (auto reactor = _reactor_manager.lock()) {
    return reactor->register_handler(shared_from_this(), ACCEPT_EVENT);
  }
  return -1;
}

int AcceptorHandler::remove_self() {
  if (auto reactor = _reactor_manager.lock()) {
    return reactor->remove_handler(shared_from_this(), ACCEPT_EVENT);
  }
  return -1;
}

Handle AcceptorHandler::get_handle(EventType et) const {
  return _fd;
}

int AcceptorHandler::handle(EventType et) {
  // Can only be called for an ACCEPT event.
  ceph_assert(et == ACCEPT_EVENT);
  int ret = 0;
  try {
    std::shared_ptr<ServerHandler> server_handler = std::make_shared<ServerHandler>(_cct, _peer, _ep.get(), _reactor_manager);
    server_handler->set_error_handler_context(new LambdaContext([this, cct = _cct](int r){
      ldout(cct, 5) << "some error occur here!!!" << dendl;
    }));
    ret = server_handler->register_self();
  } catch (std::runtime_error &e) {
    lderr(_cct) << "Runtime error: " << e.what() << dendl;
    return -1;
  }
  return ret;
}

ConnectionHandler::ConnectionHandler(CephContext *cct, const std::weak_ptr<Reactor> reactor_manager)
  : EventHandlerInterface(cct, reactor_manager) {}

ConnectionHandler::~ConnectionHandler() {
  if (_error_handler_context != nullptr) {
    delete _error_handler_context;
  }
  ldout(_cct, 20) << dendl;
}

Handle ConnectionHandler::get_handle(EventType et) const {
  if (et == CONNECTION_EVENT) {
    return _conn_fd;
  }
  if (et == COMPLETION_EVENT) {
    return _comp_fd;
  }
  return -1;
}

int ConnectionHandler::handle(EventType et) {
  if (et == CONNECTION_EVENT) {
    return handle_connection_event();
  }
  if (et == COMPLETION_EVENT) {
    return handle_completion();
  }
  return -1;
}

// Notice: call this function after _peer is initialized.
void ConnectionHandler::init_send_recv_buffer() {
  int ret = 0;
  rpma_mr_local *mr{nullptr};

  recv_bl.append(bufferptr(MSG_SIZE));
  recv_bl.rebuild_page_aligned();
  ret = rpma_mr_reg(_peer.get(), recv_bl.c_str(), MSG_SIZE, RPMA_MR_USAGE_RECV, &mr);
  if (ret) {
    throw std::runtime_error("recv memory region registers failed.");
  }
  recv_mr.reset(mr);

  mr = nullptr;
  send_bl.append(bufferptr(MSG_SIZE));
  send_bl.rebuild_page_aligned();
  ret = rpma_mr_reg(_peer.get(), send_bl.c_str(), MSG_SIZE, RPMA_MR_USAGE_SEND, &mr);
  if (ret) {
    throw std::runtime_error("send memory region registers failed.");
  }
  send_mr.reset(mr);
  return ;
}

// Notice: call this function after _conn is initialized.
void ConnectionHandler::init_conn_fd() {
  rpma_conn_get_event_fd(_conn.get(), &_conn_fd);
  rpma_cq_get_fd(cq, &_comp_fd);
}

int ConnectionHandler::handle_connection_event() {
  ldout(_cct, 10) << dendl;
  int ret = 0;
  // get next connection's event
  enum rpma_conn_event event;
  ret = rpma_conn_next_event(_conn.get(), &event);
  if (ret) {
    if (ret == RPMA_E_NO_EVENT) {
      return 0;
    }

    // another error occured - disconnect
    lderr(_cct) << "rpma_conn_next_event fails: " << rpma_err_2str(ret) << dendl;
    _conn.disconnect();
    error_handle(ret);
    return ret;
  }

  // proceed to the callback specific to the received event
  if (event == RPMA_CONN_ESTABLISHED) {
    ldout(_cct, 10) << "RPMA_CONN_ESTABLISHED" << dendl;
    {
      std::lock_guard locker(connect_lock);
      connected.store(true);
    }
    connect_cond_var.notify_all();
    return 0;
  } else if (event == RPMA_CONN_CLOSED) {
    ldout(_cct, 10) << "RPMA_CONN_CLOSED" << dendl;
  } else if (event == RPMA_CONN_LOST) {
    ldout(_cct, 10) << "RPMA_CONN_LOST" << dendl;
  } else {
    ldout(_cct, 10) << "RPMA_CONN_UNDEFINED" << dendl;
  }
  // remove self from reactor, and auto disconnect
  ret = remove_self();
  {
    std::lock_guard locker(connect_lock);
    connected.store(false);
  }
  connect_cond_var.notify_all();
  return ret;
}

int ConnectionHandler::handle_completion() {
  int ret = 0;

  // prepare detected completions for processing
  ret = rpma_cq_wait(cq);
  if (ret) {
    // no completion is ready - continue
    if (ret == RPMA_E_NO_COMPLETION) {
      return 0;
    }

    // another error occured - disconnect
    lderr(_cct) << "rpma_conn_completion_wait fails: " << rpma_err_2str(ret) << dendl;
    _conn.disconnect();
    error_handle(ret);
    return ret;
  }

  // get next completion
  struct ibv_wc wc[50];
  int cmpl_count = 0;
  ret = rpma_cq_get_wc(cq, 50, wc, &cmpl_count);
  if (ret) {
    // no completion is ready - continue
    if (ret == RPMA_E_NO_COMPLETION) {
      return 0;
    }

    // another error occured - disconnect
    lderr(_cct) << "rpma_conn_completion_get fails: " << rpma_err_2str(ret) << dendl;
    _conn.disconnect();
    error_handle(ret);
    return ret;
  }

  for (int i = 0; i < cmpl_count; i++) {
    // validate received completion
    if (wc[i].status != IBV_WC_SUCCESS) {
      ldout(_cct, 1) << ibv_wc_status_str(wc[i].status)
                     << dendl;
      _conn.disconnect();
      return -1;
    }
    ceph_assert(wc[i].status == IBV_WC_SUCCESS);

    if (wc[i].wr_id != 0) {
      auto op_func = std::unique_ptr<RpmaOp>{static_cast<RpmaOp*>(reinterpret_cast<void *>(wc[i].wr_id))};
      op_func->do_callback();
    }
  }
  return ret;
}

bool ConnectionHandler::wait_established() {
  ldout(_cct, 20) << dendl;
  std::unique_lock locker(connect_lock);
  using namespace std::chrono_literals;
  connect_cond_var.wait_for(locker, 3s, [this]{return this->connected.load();});
  return this->connected.load();
}

bool ConnectionHandler::wait_disconnected() {
  ldout(_cct, 20) << dendl;
  std::unique_lock locker(connect_lock);
  using namespace std::chrono_literals;
  connect_cond_var.wait_for(locker, 3s, [this]{return !this->connected.load();});
  return !this->connected.load();
}

int ConnectionHandler::send(std::function<void()> callback) {
  int ret = 0;
  std::unique_ptr<RpmaSend> usend = std::make_unique<RpmaSend>(callback);
  ret = (*usend)(_conn.get(), send_mr.get(), 0, MSG_SIZE, RPMA_F_COMPLETION_ALWAYS, usend.get());
  if (ret == 0) {
    usend.release();
  }
  return ret;
}

int ConnectionHandler::recv(std::function<void()> callback) {
  int ret = 0;
  std::unique_ptr<RpmaRecv> rec = std::make_unique<RpmaRecv>(callback);
  ret = (*rec)(_conn.get(), recv_mr.get(), 0, MSG_SIZE, rec.get());
  if (ret == 0) {
    rec.release();
  }
  return ret;
}

void ConnectionHandler::set_error_handler_context(Context *error_callback) {
  ldout(_cct, 20) << dendl;
  ceph_assert(error_callback);
  _error_handler_context = error_callback;
}

void ConnectionHandler::error_handle(int r) {
  ldout(_cct, 20) << _error_handler_context << ":" << r << dendl;
  if (_error_handler_context != nullptr) {
    ldout(_cct, 20) << dendl;
    _error_handler_context->complete(r);
    _error_handler_context = nullptr;
  }
}

ServerHandler::ServerHandler(CephContext *cct,
                         std::shared_ptr<struct rpma_peer> peer,
                         struct rpma_ep *ep,
                         const std::weak_ptr<Reactor> reactor_manager)
  : ConnectionHandler(cct, reactor_manager), data_manager(cct) {
  int ret = 0;

  _peer = peer;
  init_send_recv_buffer();

  struct rpma_conn_req *req_ptr = nullptr;
  struct rpma_conn_cfg *cfg_ptr = nullptr;

  unique_rpma_cfg_ptr cfg;
  ret = rpma_conn_cfg_new(&cfg_ptr);
  if (ret) {
    throw std::runtime_error(std::string("new cfg failed: ") + rpma_err_2str(ret));
  }
  cfg.reset(cfg_ptr);

  rpma_conn_cfg_set_sq_size(cfg_ptr, _cct->_conf->rwl_replica_sq_size);
  rpma_conn_cfg_set_rq_size(cfg_ptr, _cct->_conf->rwl_replica_rq_size);
  rpma_conn_cfg_set_cq_size(cfg_ptr, _cct->_conf->rwl_replica_cq_size);
  rpma_conn_cfg_set_timeout(cfg_ptr, _cct->_conf->rwl_replica_establish_timeout_ms); //ms

  ret = rpma_ep_next_conn_req(ep, cfg_ptr, &req_ptr);
  if (ret) {
    throw std::runtime_error(std::string("receive an incoming connection request failed: ") + rpma_err_2str(ret));
  }

  // prepare a receive for the client's response
  std::unique_ptr<RpmaReqRecv> recv = std::make_unique<RpmaReqRecv>([self=this](){
    self->deal_require();
  });
  ret = (*recv)(req_ptr, recv_mr.get(), 0, MSG_SIZE, recv.get());
  if (ret == 0) {
    recv.release();
  } else {
    rpma_conn_req_delete(&req_ptr);
    throw std::runtime_error(std::string("Put an initial receive to be prepared for the first"
                                         "message of the client's ping-pong failed.") + rpma_err_2str(ret));
  }

  struct rpma_conn *conn;
  ret = rpma_conn_req_connect(&req_ptr, nullptr, &conn);
  if (ret) {
    throw std::runtime_error("accept the connection request and obtain the connection object failed.");
  }
  _conn.reset(conn);

  // get the connection's main CQ
  ret = rpma_conn_get_cq(_conn.get(), &cq);
  if (ret) {
    throw std::runtime_error("get CQ error");
  }

  init_conn_fd();
}

ServerHandler::~ServerHandler() {
  ldout(_cct, 20) << dendl;
}

int ServerHandler::register_self() {
  int ret = 0;
  if (auto reactor = _reactor_manager.lock()) {
    if ((ret = reactor->register_handler(shared_from_this(), CONNECTION_EVENT))) {
      return ret;
    }
    if ((ret = reactor->register_handler(shared_from_this(), COMPLETION_EVENT))) {
      reactor->remove_handler(shared_from_this(), CONNECTION_EVENT);
      return ret;
    }
  }
  return ret;
}

int ServerHandler::remove_self() {
  int ret = 0;
  if (auto reactor = _reactor_manager.lock()) {
    ret = reactor->remove_handler(shared_from_this(), CONNECTION_EVENT);
    ret |= reactor->remove_handler(shared_from_this(), COMPLETION_EVENT);
  }
  return ret;
}

int ServerHandler::register_mr_to_descriptor(RwlReplicaInitRequestReply& init_reply) {
  int ret = 0;

  int usage = RPMA_MR_USAGE_FLUSH_TYPE_PERSISTENT | RPMA_MR_USAGE_WRITE_DST;

  // register the memory
  rpma_mr_local *mr{nullptr};
  if ((ret = rpma_mr_reg(_peer.get(), data_manager.get_pointer(), data_manager.size(),
                       usage, &mr))) {
    ldout(_cct, 1) << "rpma_mr_reg error: " << rpma_err_2str(ret) << dendl;
    init_reply.type = RWL_REPLICA_INIT_FAILED;
    return ret;
  }
  data_mr.reset(mr);

  struct rpma_peer_cfg *pcfg = NULL;

  // create a peer configuration structure
  ret = rpma_peer_cfg_new(&pcfg);
  if (ret == RPMA_E_NOMEM) {
    ldout(_cct, 1) << "rpma_peer_cfg_new can't alloc memory: " << rpma_err_2str(ret) << dendl;
    init_reply.type = RWL_REPLICA_INIT_FAILED;
    return ret;
  }

  // configure peer's direct write to pmem support
  rpma_peer_cfg_set_direct_write_to_pmem(pcfg, true);

  // get size of the memory region's descriptor
  size_t mr_desc_size;
  ret = rpma_mr_get_descriptor_size(mr, &mr_desc_size);

  // get size of the peer config descriptor
  size_t pcfg_desc_size;
  ret = rpma_peer_cfg_get_descriptor_size(pcfg, &pcfg_desc_size);

  // calculate data descriptor size for the client write
  init_reply.desc.mr_desc_size = mr_desc_size;
  init_reply.desc.pcfg_desc_size = pcfg_desc_size;
  init_reply.desc.descriptors.resize(mr_desc_size + pcfg_desc_size);

  // get the memory region's descriptor
  rpma_mr_get_descriptor(data_mr.get(), &init_reply.desc.descriptors[0]);

  // Get the peer's configuration descriptor.
  // The pcfg_desc descriptor is saved in the `descriptors`
  // just after the mr_desc descriptor.
  rpma_peer_cfg_get_descriptor(pcfg, &init_reply.desc.descriptors[mr_desc_size]);

  rpma_peer_cfg_delete(&pcfg);

  return 0;
}

int ServerHandler::get_local_descriptor() {
  RwlReplicaInitRequest init;
  auto it = recv_bl.cbegin();
  init.decode(it);
  ldout(_cct, 5) << "\ncache_id: " << init.info.cache_id << "\n"
                 << "cache_size: " << init.info.cache_size << "\n"
                 << "pool_name: "  << init.info.pool_name << "\n"
                 << "image_name: " << init.info.image_name << "\n"
                 << dendl;

  if (data_manager.get_pointer() == nullptr) {
    data_manager.init(std::move(init.info));
  }

  RwlReplicaInitRequestReply init_reply(RWL_REPLICA_INIT_SUCCESSED);
  if (!data_manager.is_pmem()) {
    init_reply.type = RWL_REPLICA_INIT_FAILED;
  } else {
    register_mr_to_descriptor(init_reply);
  }

  ldout(_cct, 20) << "succeed to register: "
                  << ((init_reply.type == RWL_REPLICA_INIT_SUCCESSED) ? "true" : "false")
                  << dendl;

  bufferlist bl;
  init_reply.encode(bl);
  ceph_assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  return 0;
}

int ServerHandler::close() {
  data_mr.reset();
  RwlReplicaFinishedRequestReply reply(RWL_REPLICA_FINISHED_SUCCCESSED);
  if (!data_manager.close_and_remove()) {
    reply.type = RWL_REPLICA_FINISHED_FAILED;
  }
  ldout(_cct, 5) << "succeed to remove cachefile: "
                 << ((reply.type != RWL_REPLICA_FINISHED_FAILED) ? "true" : "false")
                 << dendl;

  bufferlist bl;
  reply.encode(bl);
  ceph_assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  return 0;
}

void ServerHandler::deal_require() {
  RwlReplicaRequest request;
  auto it = recv_bl.cbegin();
  request.decode(it);
  switch (request.type) {
    case RWL_REPLICA_INIT_REQUEST:
      get_local_descriptor();
      break;
    case RWL_REPLICA_FINISHED_REQUEST:
      close();
      break;
    default:
      ldout(_cct, 1) << "the operation isn't supported now. op: "
                     << request.type
                     << dendl;
  }

  //When it meet the close operation, don't do the recv operation
  // prepare a receive for the client's response
  if (request.type != RWL_REPLICA_FINISHED_REQUEST) {
    recv([self=this](){
      self->deal_require();
    });
  }
  // send the data to the client
  send(nullptr);
}


ClientHandler::ClientHandler(CephContext *cct,
                             const std::string& addr,
                             const std::string& port,
                             const std::weak_ptr<Reactor> reactor_manager)
    : ConnectionHandler(cct, reactor_manager), _address(addr), _port(port) {
  int ret = 0;
  struct rpma_peer *peer = nullptr;
  ret = common_peer_via_address(addr.c_str(), RPMA_UTIL_IBV_CONTEXT_REMOTE, &peer);
  if (ret) {
    throw std::runtime_error("lookup an ibv_context via the address and create a new peer using it failed");
  }
  _peer.reset(peer, RpmaPeerDeleter());

  init_send_recv_buffer();

  struct rpma_conn_req *req_ptr = nullptr;
  struct rpma_conn_cfg *cfg_ptr = nullptr;

  unique_rpma_cfg_ptr cfg;
  ret = rpma_conn_cfg_new(&cfg_ptr);
  if (ret) {
    throw std::runtime_error("new cfg failed");
  }
  cfg.reset(cfg_ptr);

  rpma_conn_cfg_set_sq_size(cfg_ptr, _cct->_conf->rwl_replica_sq_size);
  rpma_conn_cfg_set_rq_size(cfg_ptr, _cct->_conf->rwl_replica_rq_size);
  rpma_conn_cfg_set_cq_size(cfg_ptr, _cct->_conf->rwl_replica_cq_size);
  rpma_conn_cfg_set_timeout(cfg_ptr, _cct->_conf->rwl_replica_establish_timeout_ms); //ms

  ret = rpma_conn_req_new(peer, addr.c_str(), port.c_str(), cfg_ptr, &req_ptr);
  if (ret) {
    throw std::runtime_error("create a new outgoing connection request object failed");
  }

  struct rpma_conn *conn;
  ret = rpma_conn_req_connect(&req_ptr, nullptr, &conn);
  if (ret) {
    throw std::runtime_error("initiate processing the connection request");
  }
  _conn.reset(conn);

  // get the connection's main CQ
  ret = rpma_conn_get_cq(_conn.get(), &cq);
  if (ret) {
    throw std::runtime_error("get CQ error");
  }

  init_conn_fd();
}

ClientHandler::~ClientHandler() {
  ldout(_cct, 20)  << dendl;
}

int ClientHandler::register_self() {
  int ret = 0;
  if (auto reactor = _reactor_manager.lock()) {
    if ((ret = reactor->register_handler(shared_from_this(), CONNECTION_EVENT))) {
      return ret;
    }
    if ((ret = reactor->register_handler(shared_from_this(), COMPLETION_EVENT))) {
      reactor->remove_handler(shared_from_this(), CONNECTION_EVENT);
      return ret;
    }
  }
  return ret;
}

int ClientHandler::remove_self() {
  int ret = 0;
  if (auto reactor = _reactor_manager.lock()) {
    ret = reactor->remove_handler(shared_from_this(), CONNECTION_EVENT);
    ret |= reactor->remove_handler(shared_from_this(), COMPLETION_EVENT);
  }
  return ret;
}

int ClientHandler::get_remote_descriptor() {
  RwlReplicaInitRequestReply init_reply;
  auto it = recv_bl.cbegin();
  init_reply.decode(it);
  int ret = 0;
  ldout(_cct, 5) << "init request reply: " << init_reply.type << dendl;
  if (init_reply.type == RWL_REPLICA_INIT_SUCCESSED) {
    struct RpmaConfigDescriptor *dst_data = &(init_reply.desc);

    if (dst_data->pcfg_desc_size) {
      struct rpma_peer_cfg *pcfg = nullptr;
      // apply pmem config for the connection
      ret = rpma_peer_cfg_from_descriptor(&dst_data->descriptors[dst_data->mr_desc_size], dst_data->pcfg_desc_size, &pcfg);
      if (ret == RPMA_E_NOMEM) {
        ldout(_cct, 1) << "rpma_peer_cfg_from_descriptor can't alloc memory" << dendl;
        return RPMA_E_NOMEM;
      }
      rpma_conn_apply_remote_peer_cfg(_conn.get(), pcfg);
      rpma_peer_cfg_delete(&pcfg);
    }

    // Create a remote memory region from received descriptor
    struct rpma_mr_remote* mr_ptr;
    if ((ret = rpma_mr_remote_from_descriptor(&dst_data->descriptors[0], dst_data->mr_desc_size, &mr_ptr))) {
      ldout(_cct, 1) << rpma_err_2str(ret) << dendl;
      return ret;
    }
    _image_mr.reset(mr_ptr);

    //get the remote memory region size
    size_t size;
    rpma_mr_remote_get_size(mr_ptr, &size);

    if (size < _image_size) {
      ldout(_cct, 1) << "Remote memory region size too small "
                     << "for writing the  data of the assumed size ("
                     << size << " < " << _image_size << ")"
                     << dendl;
      _image_mr.reset();
      return -1;
    }
  }
  return ret;
}

int ClientHandler::init_replica(epoch_t cache_id, uint64_t cache_size, std::string pool_name, std::string image_name) {
  _image_size = cache_size;
  ldout(_cct, 20) << dendl;
  RwlReplicaInitRequest init(RWL_REPLICA_INIT_REQUEST);
  init.info.cache_id = cache_id;
  init.info.cache_size = cache_size;
  init.info.pool_name = pool_name;
  init.info.image_name = image_name;
  bufferlist bl;
  init.encode(bl);
  ceph_assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  {
    std::lock_guard locker(message_lock);
    recv_completed = false;
  }
  recv([this] () mutable {
    get_remote_descriptor();
    {
      std::lock_guard locker(message_lock);
      this->recv_completed = true;
    }
    this->cond_var.notify_one();
  });
  send(nullptr);
  std::unique_lock locker(message_lock);
  cond_var.wait(locker, [this]{return this->recv_completed;});

  return (recv_completed == true ? (_image_mr == nullptr ? -ENOSPC : 0) : -1);
}

int ClientHandler::close_replica() {
  ldout(_cct, 20) << dendl;
  RwlReplicaFinishedRequest finish(RWL_REPLICA_FINISHED_REQUEST);
  bufferlist bl;
  finish.encode(bl);
  ceph_assert(bl.length() < MSG_SIZE);
  memcpy(send_bl.c_str(), bl.c_str(), bl.length());
  {
    std::lock_guard locker(message_lock);
    recv_completed = false;
    finished_success = false;
  }
  recv([this] () mutable {
    RwlReplicaFinishedRequestReply reply;
    auto it = recv_bl.cbegin();
    reply.decode(it);
    {
      std::lock_guard locker(message_lock);
      this->recv_completed = true;
      if (reply.type == RWL_REPLICA_FINISHED_SUCCCESSED) {
        this->finished_success = true;
      }
    }
    this->cond_var.notify_one();
  });
  send(nullptr);
  std::unique_lock locker(message_lock);
  cond_var.wait(locker, [this]{return this->recv_completed;});

  return ((recv_completed == true && finished_success == true) ? 0 : -1);
}

int ClientHandler::disconnect() {
  return _conn.disconnect();
}

int ClientHandler::set_head(void *head_ptr, uint64_t size) {
  data_header = head_ptr;
  data_size = size;
  rpma_mr_local *mr{nullptr};
  int ret = rpma_mr_reg(_peer.get(), head_ptr, size, RPMA_MR_USAGE_WRITE_SRC, &mr);
  if (ret) {
    return ret;
  }
  data_mr.reset(mr);
  return 0;
}

int ClientHandler::write(size_t offset,
                         size_t len) {
  ceph_assert(data_mr);
  ceph_assert(offset + len <= data_size);
  ceph_assert(len <= 1024 * 1024 * 1024);
  RpmaWrite write;
  return write(_conn.get(), _image_mr.get(), offset, data_mr.get(), offset, len, RPMA_F_COMPLETION_ON_ERROR, nullptr);
}

int ClientHandler::flush(size_t offset,
                         size_t len,
                         std::function<void()> callback) {
  ceph_assert(data_mr);
  std::unique_ptr<RpmaFlush> uflush = std::make_unique<RpmaFlush>(callback);
  int ret = (*uflush)(_conn.get(), _image_mr.get(), offset, len, RPMA_FLUSH_TYPE_PERSISTENT, RPMA_F_COMPLETION_ALWAYS, uflush.get());
  if (ret == 0) {
    uflush.release();
  }
  return ret;
}

// a sequence of RDMA Write operations is followed by one RDMA Read operation
// when the Initiator software inserts a durability point (RDMA Read address and length does not matter).
// The RDMA Read is sent on the same connection as all of the previous RDMA Writes.
//
// In this method, non-allocating Writes ensure that data reach a memory controller
// as soon as they are pushed out from the NIC—what is ensured by RDMA Read and PCIe Read fencing mechanism.
// https://software.intel.com/content/www/us/en/develop/articles/persistent-memory-replication-over-traditional-rdma-part-1-understanding-remote-persistent.html
int ClientHandler::flush(std::function<void()> callback) {
  return flush(0, data_size, callback);
}

} //namespace ceph::librbd::cache::pwl::rwl::replica
