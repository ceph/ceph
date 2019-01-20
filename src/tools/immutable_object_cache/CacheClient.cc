// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "CacheClient.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_immutable_obj_cache
#undef dout_prefix
#define dout_prefix *_dout << "ceph::cache::CacheClient: " << this << " " \
                           << __func__ << ": "

namespace ceph {
namespace immutable_obj_cache {

  CacheClient::CacheClient(const std::string& file, CephContext* ceph_ctx)
    : cct(ceph_ctx), m_io_service_work(m_io_service),
      m_dm_socket(m_io_service), m_ep(stream_protocol::endpoint(file)),
      m_io_thread(nullptr), m_session_work(false), m_writing(false),
      m_reading(false), m_sequence_id(0),
      m_lock("ceph::cache::cacheclient::m_lock"),
      m_header_buffer(new char[sizeof(ObjectCacheMsgHeader)])
  {
    // TODO : release these resources.
    // TODO : configure it.
    m_use_dedicated_worker = true;
    m_worker_thread_num = 2;
    if(m_use_dedicated_worker) {
      m_worker = new boost::asio::io_service();
      m_worker_io_service_work = new boost::asio::io_service::work(*m_worker);
      for(uint64_t i = 0; i < m_worker_thread_num; i++) {
        std::thread* thd = new std::thread([this](){m_worker->run();});
        m_worker_threads.push_back(thd);
      }
    }
  }

  CacheClient::~CacheClient() {
    stop();
    delete m_header_buffer;
  }

  void CacheClient::run(){
     m_io_thread.reset(new std::thread([this](){m_io_service.run(); }));
  }

  bool CacheClient::is_session_work() {
    return m_session_work.load() == true;
  }

  int CacheClient::stop() {
    m_session_work.store(false);
    m_io_service.stop();

    if(m_io_thread != nullptr) {
      m_io_thread->join();
    }
    return 0;
  }

  // just when error occur, call this method.
  void CacheClient::close() {
    m_session_work.store(false);
    boost::system::error_code close_ec;
    m_dm_socket.close(close_ec);
    if(close_ec) {
       ldout(cct, 20) << "close: " << close_ec.message() << dendl;
    }
    ldout(cct, 20) << "session don't work, later all request will be" <<
                      " dispatched to rados layer" << dendl;
  }

  int CacheClient::connect() {
    boost::system::error_code ec;
    m_dm_socket.connect(m_ep, ec);
    if(ec) {
      if(ec == boost::asio::error::connection_refused) {
        ldout(cct, 20) << ec.message()
                       << " : immutable-object-cache daemon is down?"
                       << "Now data will be read from ceph cluster " << dendl;
      } else {
        ldout(cct, 20) << "connect: " << ec.message() << dendl;
      }

      if(m_dm_socket.is_open()) {
        // Set to indicate what error occurred, if any.
        // Note that, even if the function indicates an error,
        // the underlying descriptor is closed.
        boost::system::error_code close_ec;
        m_dm_socket.close(close_ec);
        if(close_ec) {
          ldout(cct, 20) << "close: " << close_ec.message() << dendl;
        }
      }
      return -1;
    }

    ldout(cct, 20) <<"connect success"<< dendl;

    return 0;
  }

  void CacheClient::lookup_object(std::string pool_name, std::string oid,
                                  GenContext<ObjectCacheRequest*>* on_finish) {

    ObjectCacheRequest* req = new ObjectCacheRequest();
    req->m_head.version = 0;
    req->m_head.reserved = 0;
    req->m_head.type = RBDSC_READ;
    req->m_head.padding = 0;
    req->m_head.seq = ++m_sequence_id;

    req->m_data.m_pool_name = pool_name;
    req->m_data.m_oid = oid;
    req->m_process_msg = on_finish;
    req->encode();

    ceph_assert(req->get_head_buffer().length() == sizeof(ObjectCacheMsgHeader));
    ceph_assert(req->get_data_buffer().length() == req->m_head.data_len);

   {
      Mutex::Locker locker(m_lock);
      m_outcoming_bl.append(req->get_head_buffer());
      m_outcoming_bl.append(req->get_data_buffer());
      ceph_assert(m_seq_to_req.find(req->m_head.seq) == m_seq_to_req.end());
      m_seq_to_req[req->m_head.seq] = req;
    }

    // try to send message to server.
    try_send();

    // try to receive ack from server.
    try_receive();
  }

  void CacheClient::try_send() {
    if(!m_writing.load()) {
      m_writing.store(true);
      send_message();
    }
  }

  void CacheClient::send_message() {
    bufferlist bl;
    {
      Mutex::Locker locker(m_lock);
      bl.swap(m_outcoming_bl);
      ceph_assert(m_outcoming_bl.length() == 0);
    }

    // send bytes as many as possible.
    boost::asio::async_write(m_dm_socket,
        boost::asio::buffer(bl.c_str(), bl.length()),
        boost::asio::transfer_exactly(bl.length()),
        [this, bl](const boost::system::error_code& err, size_t cb) {
        if (err || cb != bl.length()) {
           fault(ASIO_ERROR_WRITE, err);
           return;
        }
        ceph_assert(cb == bl.length());

        {
           Mutex::Locker locker(m_lock);
           if(m_outcoming_bl.length() == 0) {
             m_writing.store(false);
             return;
           }
        }

        // still have left bytes, continue to send.
        send_message();
    });
    try_receive();
  }

  void CacheClient::try_receive() {
    if(!m_reading.load()) {
      m_reading.store(true);
      receive_message();
    }
  }

  void CacheClient::receive_message() {
    ceph_assert(m_reading.load());
    read_reply_header();
  }

  void CacheClient::read_reply_header() {

    /* one head buffer for all arrived reply. */
    // bufferptr bp_head(buffer::create_static(sizeof(ObjectCacheMsgHeader), m_header_buffer));

    /* create new head buffer for every reply */
    bufferptr bp_head(buffer::create(sizeof(ObjectCacheMsgHeader)));
    auto raw_ptr = bp_head.c_str();

    boost::asio::async_read(m_dm_socket,
      boost::asio::buffer(raw_ptr, sizeof(ObjectCacheMsgHeader)),
      boost::asio::transfer_exactly(sizeof(ObjectCacheMsgHeader)),
      boost::bind(&CacheClient::handle_reply_header,
                  this, bp_head,
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
  }

  void CacheClient::handle_reply_header(bufferptr bp_head,
                                        const boost::system::error_code& ec,
                                        size_t bytes_transferred) {
    if(ec || bytes_transferred != sizeof(ObjectCacheMsgHeader)) {
      fault(ASIO_ERROR_READ, ec);
      return;
    }

    ceph_assert(bytes_transferred == bp_head.length());

    ObjectCacheMsgHeader* head = (ObjectCacheMsgHeader*)bp_head.c_str();
    uint64_t data_len = head->data_len;
    uint64_t seq_id = head->seq;
    ceph_assert(m_seq_to_req.find(seq_id) != m_seq_to_req.end());

    bufferptr bp_data(buffer::create(data_len));
    read_reply_data(std::move(bp_head), std::move(bp_data), data_len, seq_id);
  }

  void CacheClient::read_reply_data(bufferptr&& bp_head, bufferptr&& bp_data,
                                    const uint64_t data_len, const uint64_t seq_id) {

    auto raw_ptr = bp_data.c_str();
    boost::asio::async_read(m_dm_socket, boost::asio::buffer(raw_ptr, data_len),
      boost::asio::transfer_exactly(data_len),
      boost::bind(&CacheClient::handle_reply_data,
                  this, std::move(bp_head), std::move(bp_data), data_len, seq_id,
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));

  }

  void CacheClient::handle_reply_data(bufferptr bp_head, bufferptr bp_data,
                                      const uint64_t data_len, const uint64_t seq_id,
                                      const boost::system::error_code& ec,
                                      size_t bytes_transferred) {
    if (ec || bytes_transferred != data_len) {
      fault(ASIO_ERROR_WRITE, ec);
      return;
    }

    bufferlist head_buffer;
    bufferlist data_buffer;
    head_buffer.append(std::move(bp_head));
    data_buffer.append(std::move(bp_data));
    ceph_assert(head_buffer.length() == sizeof(ObjectCacheMsgHeader));
    ceph_assert(data_buffer.length() == data_len);

    ObjectCacheRequest* reply = decode_object_cache_request(head_buffer, data_buffer);
    data_buffer.clear();
    ceph_assert(data_buffer.length() == 0);

    process(reply, seq_id);

    {
      Mutex::Locker locker(m_lock);
      if(m_seq_to_req.size() == 0 && m_outcoming_bl.length()) {
        m_reading.store(false);
        return;
      }
    }
    if(is_session_work()) {
      receive_message();
    }

  }

  void CacheClient::process(ObjectCacheRequest* reply, uint64_t seq_id) {
    ObjectCacheRequest* current_request = nullptr;
    {
      Mutex::Locker locker(m_lock);
      ceph_assert(m_seq_to_req.find(seq_id) != m_seq_to_req.end());
      current_request = m_seq_to_req[seq_id];
      m_seq_to_req.erase(seq_id);
    }

    ceph_assert(current_request != nullptr);
    auto process_reply = new FunctionContext([this, current_request, reply]
      (bool dedicated) {
       if(dedicated) {
         // dedicated thrad to execute this context.
       }
       current_request->m_process_msg->complete(reply);
       delete current_request;
       delete reply;
    });

    if(m_use_dedicated_worker) {
      m_worker->post([process_reply]() {
        process_reply->complete(true);
      });
    } else {
      process_reply->complete(false);
    }
  }

  void CacheClient::fault(const int err_type, const boost::system::error_code& ec) {
    ldout(cct, 20) << "fault." << ec.message() << dendl;
    // if one request fails, just call its callback, then close this socket.
    if(!m_session_work.load()) {
      return;
    }

    // when current session don't work, ASIO will don't receive any new request from hook.
    // On the other hand, for pending request of ASIO, cancle these request, then call their callback.
    // there request which are cancled by fault, will be re-dispatched to RADOS layer.
    //
    // make sure just have one thread to modify execute below code.
    m_session_work.store(false);

    if(err_type == ASIO_ERROR_MSG_INCOMPLETE) {
       ldout(cct, 20) << "ASIO In-complete message." << ec.message() << dendl;
       ceph_assert(0);
    }

    if(err_type == ASIO_ERROR_READ) {
       ldout(cct, 20) << "ASIO async read fails : " << ec.message() << dendl;
    }

    if(err_type == ASIO_ERROR_WRITE) {
       ldout(cct, 20) << "ASIO asyn write fails : " << ec.message() << dendl;
       // CacheClient should not occur this error.
       ceph_assert(0);
    }

    if(err_type == ASIO_ERROR_CONNECT) {
       ldout(cct, 20) << "ASIO async connect fails : " << ec.message() << dendl;
    }

    // currently, for any asio error, just shutdown RO.
    close();

    // all pending request, which have entered into ASIO, will be re-dispatched to RADOS.
    {
      Mutex::Locker locker(m_lock);
      for(auto it : m_seq_to_req) {
        it.second->m_head.type = RBDSC_READ_RADOS;
        it.second->m_process_msg->complete(it.second);
      }
      m_seq_to_req.clear();
    }

    ldout(cct, 20) << "Because ASIO fails, just shutdown RO. Later all reading \
                       will be re-dispatched RADOS layer"  << ec.message() << dendl;
  }


  // TODO : use async + wait_event
  // TODO : accept one parameter : ObjectCacheRequest
  int CacheClient::register_client(Context* on_finish) {
    ObjectCacheRequest* message = new ObjectCacheRequest();
    message->m_head.version = 0;
    message->m_head.seq = m_sequence_id++;
    message->m_head.type = RBDSC_REGISTER;
    message->m_head.reserved = 0;
    message->encode();

    bufferlist bl;
    bl.append(message->get_head_buffer());
    bl.append(message->get_data_buffer());

    uint64_t ret;
    boost::system::error_code ec;

    ret = boost::asio::write(m_dm_socket,
      boost::asio::buffer(bl.c_str(), bl.length()), ec);

    if(ec || ret != bl.length()) {
      fault(ASIO_ERROR_WRITE, ec);
      return -1;
    }

    ret = boost::asio::read(m_dm_socket,
      boost::asio::buffer(m_header_buffer, sizeof(ObjectCacheMsgHeader)), ec);
    if(ec || ret != sizeof(ObjectCacheMsgHeader)) {
      fault(ASIO_ERROR_READ, ec);
      return -1;
    }

    ObjectCacheMsgHeader* head = (ObjectCacheMsgHeader*)m_header_buffer;
    uint64_t data_len = head->data_len;
    bufferptr bp_data(buffer::create(data_len));

    ret = boost::asio::read(m_dm_socket, boost::asio::buffer(bp_data.c_str(), data_len), ec);
    if(ec || ret != data_len) {
      fault(ASIO_ERROR_READ, ec);
      return -1;
    }

    bufferlist data_buffer;
    data_buffer.append(std::move(bp_data));
    ObjectCacheRequest* req = decode_object_cache_request(head, data_buffer);
    if (req->m_head.type == RBDSC_REGISTER_REPLY) {
      on_finish->complete(true);
    } else {
      on_finish->complete(false);
    }

    delete req;
    m_session_work.store(true);

    return 0;
  }

} // namespace immutable_obj_cache
} // namespace ceph
