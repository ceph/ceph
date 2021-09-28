#ifndef _EVENT_OP_H_
#define _EVENT_OP_H_

#include "EventHandler.h"
#include "Reactor.h"
#include "MemoryManager.h"
#include "RpmaOp.h"
#include "Types.h"
#include "include/rados/librados.hpp"

#include <atomic>
#include <set>
#include <condition_variable>
#include <mutex>

namespace librbd::cache::pwl::rwl::replica {

struct RpmaPeerDeleter {
  void operator() (struct rpma_peer *peer_ptr);
};

struct RpmaEpDeleter {
  void operator() (struct rpma_ep *ep_ptr);
};

struct RpmaMRDeleter {
  void operator() (struct rpma_mr_local *mr_ptr);
};

struct RpmaRemoteMRDeleter {
  void operator() (struct rpma_mr_remote *mr_ptr);
};

struct RpmaConnCfgDeleter {
  void operator() (struct rpma_conn_cfg *cfg_ptr);
};

using unique_rpma_peer_ptr      = std::unique_ptr<struct rpma_peer, RpmaPeerDeleter>;
using unique_rpma_ep_ptr        = std::unique_ptr<struct rpma_ep, RpmaEpDeleter>;
using unique_rpma_mr_ptr        = std::unique_ptr<struct rpma_mr_local, RpmaMRDeleter>;
using unique_rpma_remote_mr_ptr = std::unique_ptr<struct rpma_mr_remote, RpmaRemoteMRDeleter>;
using unique_rpma_cfg_ptr       = std::unique_ptr<struct rpma_conn_cfg, RpmaConnCfgDeleter>;

class RpmaConn {
  struct rpma_conn *conn{nullptr};
  std::atomic<bool> disconnected{true};
public:
  RpmaConn(struct rpma_conn *conn): conn(conn), disconnected(false) {}
  RpmaConn() : conn(nullptr), disconnected(true) {}
  RpmaConn(const RpmaConn &) = delete;
  RpmaConn& operator=(const RpmaConn &) = delete;
  ~RpmaConn();

  void reset(struct rpma_conn *conn);

  struct rpma_conn *get() const;

  int disconnect();
};

class EventHandlerInterface : public EventHandler {
public:
  EventHandlerInterface(CephContext *cct, std::weak_ptr<Reactor> reactor_ptr): _reactor_manager(reactor_ptr), _cct(cct) {}
  virtual ~EventHandlerInterface() {}

  virtual int register_self() = 0;
  virtual int remove_self() = 0;

  virtual const char* name() const = 0;
protected:
  std::weak_ptr<Reactor> _reactor_manager;
  CephContext *_cct;
};

// Handles client connection requests.
class AcceptorHandler : public EventHandlerInterface, public std::enable_shared_from_this<AcceptorHandler> {
public:
  AcceptorHandler(CephContext *cct,
                  const std::string& addr,
                  const std::string& port,
                  const std::weak_ptr<Reactor> reactor_manager);
  virtual ~AcceptorHandler();
  AcceptorHandler(const AcceptorHandler &) = delete;
  AcceptorHandler& operator=(const AcceptorHandler &) = delete;

  virtual int register_self() override;
  virtual int remove_self() override;

  virtual int handle(EventType et) override;
  virtual Handle get_handle(EventType et) const override;

  virtual const char* name() const override { return "AcceptorHandler"; }

private:
  Handle _fd;
  std::string _address;
  std::string _port;
  std::shared_ptr<struct rpma_peer> _peer;
  unique_rpma_ep_ptr _ep;
};

class ConnectionHandler : public EventHandlerInterface {
public:
  ConnectionHandler(CephContext *cct, const std::weak_ptr<Reactor> reactor_manager);
  virtual ~ConnectionHandler();

  virtual int handle(EventType et) override;
  virtual Handle get_handle(EventType et) const override;

  int handle_completion();
  int handle_connection_event();

  virtual const char* name() const override { return "ConnectionHandler"; }

  int send(std::function<void()> callback);
  int recv(std::function<void()> callback);

  bool connecting() { return connected.load(); }
  // wait for the connection to establish
  bool wait_established();
  bool wait_disconnected();

protected:
  // Notice: call this function after peer is initialized.
  void init_send_recv_buffer();
  // Notice: call this function after conn is initialized.
  void init_conn_fd();

  std::shared_ptr<struct rpma_peer> _peer;
  RpmaConn _conn;
  struct rpma_cq *cq{nullptr};

  bufferlist send_bl;
  unique_rpma_mr_ptr send_mr;
  bufferlist recv_bl;
  unique_rpma_mr_ptr recv_mr;

  Handle _conn_fd;
  Handle _comp_fd;

private:
  std::atomic<bool> connected{false};
  std::mutex connect_lock;
  std::condition_variable connect_cond_var;
};

class ServerHandler : public ConnectionHandler, public std::enable_shared_from_this<ServerHandler>{
public:
  ServerHandler(CephContext *cct,
              std::shared_ptr<struct rpma_peer> peer,
              struct rpma_ep *ep,
              const std::weak_ptr<Reactor> reactor_manager);
  virtual ~ServerHandler();

  ServerHandler(const ServerHandler &) = delete;
  ServerHandler& operator=(const ServerHandler &) = delete;

  virtual int register_self() override;
  virtual int remove_self() override;

  virtual const char* name() const override { return "ServerHandler"; }
private:
  int register_mr_to_descriptor(RwlReplicaInitRequestReply& init_reply);
  int get_local_descriptor();
  void deal_require();
  int close();

  // memory resource
  MemoryManager data_manager;
  unique_rpma_mr_ptr data_mr;
};

class ClientHandler : public ConnectionHandler, public std::enable_shared_from_this<ClientHandler> {
public:
  ClientHandler(CephContext *cct,
                const std::string& addr,
                const std::string& port,
                const std::weak_ptr<Reactor> reactor_manager);
  virtual ~ClientHandler();

  ClientHandler(const ClientHandler &) = delete;
  ClientHandler& operator=(const ClientHandler &) = delete;

  virtual int register_self() override;
  virtual int remove_self() override;

  virtual const char* name() const override { return "ClientHandler"; }

  int disconnect();

  int write(size_t offset,
            size_t len);
  int flush(size_t offset,
            size_t len,
            std::function<void()> callback = nullptr);
  int flush(std::function<void()> callback = nullptr);

  int get_remote_descriptor();
  int init_replica(epoch_t cache_id, uint64_t cache_size, std::string pool_name, std::string image_name);
  int close_replica();
  int set_head(void *head_ptr, uint64_t size);

private:
  std::string _address;
  std::string _port;

  void *data_header;
  size_t data_size;
  unique_rpma_mr_ptr data_mr;

  size_t _image_size;
  unique_rpma_remote_mr_ptr _image_mr;

  std::mutex message_lock;
  std::condition_variable cond_var;
  bool recv_completed;
  bool finished_success;
};

} //namespace ceph::librbd::cache::pwl::rwl::replica
#endif //_EVENT_OP_H_
