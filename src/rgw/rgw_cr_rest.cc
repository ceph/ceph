// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_cr_rest.h"

#include "rgw_coroutine.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

RGWCRHTTPGetDataCB::RGWCRHTTPGetDataCB(RGWCoroutinesEnv *_env, RGWCoroutine *_cr, RGWHTTPStreamRWRequest *_req) : lock("RGWCRHTTPGetDataCB"), env(_env), cr(_cr), req(_req) {
  io_id = req->get_io_id(RGWHTTPClient::HTTPCLIENT_IO_READ |RGWHTTPClient::HTTPCLIENT_IO_CONTROL);
  req->set_in_cb(this);
}

#define GET_DATA_WINDOW_SIZE 2 * 1024 * 1024

int RGWCRHTTPGetDataCB::handle_data(bufferlist& bl, bool *pause) {
  if (data.length() < GET_DATA_WINDOW_SIZE / 2) {
    notified = false;
  }

  {
    uint64_t bl_len = bl.length();

    Mutex::Locker l(lock);

    if (!got_all_extra_data) {
      uint64_t max = extra_data_len - extra_data.length();
      if (max > bl_len) {
        max = bl_len;
      }
      bl.splice(0, max, &extra_data);
      bl_len -= max;
      got_all_extra_data = extra_data.length() == extra_data_len;
    }

    data.append(bl);
  }

  uint64_t data_len = data.length();
  if (data_len >= GET_DATA_WINDOW_SIZE && !notified) {
    notified = true;
    env->manager->io_complete(cr, io_id);
  }
  if (data_len >= 2 * GET_DATA_WINDOW_SIZE) {
    *pause = true;
    paused = true;
  }
  return 0;
}

void RGWCRHTTPGetDataCB::claim_data(bufferlist *dest, uint64_t max) {
  bool need_to_unpause = false;

  {
    Mutex::Locker l(lock);

    if (data.length() == 0) {
      return;
    }

    if (data.length() < max) {
      max = data.length();
    }

    data.splice(0, max, dest);
    need_to_unpause = (paused && data.length() <= GET_DATA_WINDOW_SIZE);
  }

  if (need_to_unpause) {
    req->unpause_receive();
  }
}

RGWStreamReadHTTPResourceCRF::~RGWStreamReadHTTPResourceCRF()
{
  if (req) {
    req->cancel();
    req->wait(null_yield);
    delete req;
  }
}

int RGWStreamReadHTTPResourceCRF::init()
{
  env->stack->init_new_io(req);

  in_cb.emplace(env, caller, req);

  int r = http_manager->add_request(req);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWStreamWriteHTTPResourceCRF::send()
{
  env->stack->init_new_io(req);

  req->set_write_drain_cb(&write_drain_notify_cb);

  int r = http_manager->add_request(req);
  if (r < 0) {
    return r;
  }

  return 0;
}

bool RGWStreamReadHTTPResourceCRF::has_attrs()
{
  return got_attrs;
}

void RGWStreamReadHTTPResourceCRF::get_attrs(std::map<string, string> *attrs)
{
  req->get_out_headers(attrs);
}

int RGWStreamReadHTTPResourceCRF::decode_rest_obj(map<string, string>& headers, bufferlist& extra_data) {
  /* basic generic implementation */
  for (auto header : headers) {
    const string& val = header.second;

    rest_obj.attrs[header.first] = val;
  }

  return 0;
}

int RGWStreamReadHTTPResourceCRF::read(bufferlist *out, uint64_t max_size, bool *io_pending)
{
    reenter(&read_state) {
    io_read_mask = req->get_io_id(RGWHTTPClient::HTTPCLIENT_IO_READ | RGWHTTPClient::HTTPCLIENT_IO_CONTROL);
    while (!req->is_done() ||
           in_cb->has_data()) {
      *io_pending = true;
      if (!in_cb->has_data()) {
        yield caller->io_block(0, io_read_mask);
      }
      got_attrs = true;
      if (need_extra_data() && !got_extra_data) {
        if (!in_cb->has_all_extra_data()) {
          continue;
        }
        extra_data.claim_append(in_cb->get_extra_data());
        map<string, string> attrs;
        req->get_out_headers(&attrs);
        int ret = decode_rest_obj(attrs, extra_data);
        if (ret < 0) {
          ldout(cct, 0) << "ERROR: " << __func__ << " decode_rest_obj() returned ret=" << ret << dendl;
          return ret;
        }
        got_extra_data = true;
      }
      *io_pending = false;
      in_cb->claim_data(out, max_size);
      if (out->length() == 0) {
        /* this may happen if we just read the prepended extra_data and didn't have any data
         * after. In that case, retry reading, so that caller doesn't assume it's EOF.
         */
        continue;
      }
      if (!req->is_done() || out->length() >= max_size) {
        yield;
      }
    }
  }
  return 0;
}

bool RGWStreamReadHTTPResourceCRF::is_done()
{
  return req->is_done();
}

RGWStreamWriteHTTPResourceCRF::~RGWStreamWriteHTTPResourceCRF()
{
  if (req) {
    req->cancel();
    req->wait(null_yield);
    delete req;
  }
}

void RGWStreamWriteHTTPResourceCRF::send_ready(const rgw_rest_obj& rest_obj)
{
  req->set_send_length(rest_obj.content_len);
  for (auto h : rest_obj.attrs) {
    req->append_header(h.first, h.second);
  }
}

#define PENDING_WRITES_WINDOW (1 * 1024 * 1024)

void RGWStreamWriteHTTPResourceCRF::write_drain_notify(uint64_t pending_size)
{
  lock_guard l(blocked_lock);
  if (is_blocked && (pending_size < PENDING_WRITES_WINDOW / 2)) {
    env->manager->io_complete(caller, req->get_io_id(RGWHTTPClient::HTTPCLIENT_IO_WRITE | RGWHTTPClient::HTTPCLIENT_IO_CONTROL));
    is_blocked = false;
  }
}

void RGWStreamWriteHTTPResourceCRF::WriteDrainNotify::notify(uint64_t pending_size)
{
  crf->write_drain_notify(pending_size);
}

int RGWStreamWriteHTTPResourceCRF::write(bufferlist& data, bool *io_pending)
{
  reenter(&write_state) {
    while (!req->is_done()) {
      *io_pending = false;
      if (req->get_pending_send_size() >= PENDING_WRITES_WINDOW) {
        *io_pending = true;
        {
          lock_guard l(blocked_lock);
          is_blocked = true;

          /* it's ok to unlock here, even if io_complete() arrives before io_block(), it'll wakeup
           * correctly */
        }
        yield caller->io_block(0, req->get_io_id(RGWHTTPClient::HTTPCLIENT_IO_WRITE | RGWHTTPClient::HTTPCLIENT_IO_CONTROL));
      }
      yield req->add_send_data(data);
    }
    return req->get_status();
  }
  return 0;
}

int RGWStreamWriteHTTPResourceCRF::drain_writes(bool *need_retry)
{
  reenter(&drain_state) {
    *need_retry = true;
    yield req->finish_write();
    *need_retry = !req->is_done();
    while (!req->is_done()) {
      yield caller->io_block(0, req->get_io_id(RGWHTTPClient::HTTPCLIENT_IO_CONTROL));
      *need_retry = !req->is_done();
    }

    map<string, string> headers;
    req->get_out_headers(&headers);
    handle_headers(headers);

    return req->get_req_retcode();
  }
  return 0;
}

RGWStreamSpliceCR::RGWStreamSpliceCR(CephContext *_cct, RGWHTTPManager *_mgr,
                           shared_ptr<RGWStreamReadHTTPResourceCRF>& _in_crf,
                           shared_ptr<RGWStreamWriteHTTPResourceCRF>& _out_crf) : RGWCoroutine(_cct), cct(_cct), http_manager(_mgr),
                                                               in_crf(_in_crf), out_crf(_out_crf) {}
RGWStreamSpliceCR::~RGWStreamSpliceCR() { }

int RGWStreamSpliceCR::operate() {
  reenter(this) {
    {
      int ret = in_crf->init();
      if (ret < 0) {
        return set_cr_error(ret);
      }
    }

    do {

      bl.clear();

      do {
        yield {
          ret = in_crf->read(&bl, 4 * 1024 * 1024, &need_retry);
          if (ret < 0)  {
            return set_cr_error(ret);
          }
        }

        if (retcode < 0) {
          ldout(cct, 20) << __func__ << ": in_crf->read() retcode=" << retcode << dendl;
          return set_cr_error(ret);
        }
      } while (need_retry);

      ldout(cct, 20) << "read " << bl.length() << " bytes" << dendl;

      if (!in_crf->has_attrs()) {
        assert (bl.length() == 0);
        continue;
      }

      if (!sent_attrs) {
        int ret = out_crf->init();
        if (ret < 0) {
          return set_cr_error(ret);
        }
        out_crf->send_ready(in_crf->get_rest_obj());
        ret = out_crf->send();
        if (ret < 0) {
          return set_cr_error(ret);
        }
        sent_attrs = true;
      }

      if (bl.length() == 0 && in_crf->is_done()) {
        break;
      }

      total_read += bl.length();

      do {
        yield {
          ldout(cct, 20) << "writing " << bl.length() << " bytes" << dendl;
          ret = out_crf->write(bl, &need_retry);
          if (ret < 0)  {
            return set_cr_error(ret);
          }
        }

        if (retcode < 0) {
          ldout(cct, 20) << __func__ << ": out_crf->write() retcode=" << retcode << dendl;
          return set_cr_error(ret);
        }
      } while (need_retry);
    } while (true);

    do {
      yield {
        int ret = out_crf->drain_writes(&need_retry);
        if (ret < 0) {
          return set_cr_error(ret);
        }
      }
    } while (need_retry);

    return set_cr_done();
  }
  return 0;
}

