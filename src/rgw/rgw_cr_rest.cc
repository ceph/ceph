#include "rgw_cr_rest.h"

#include "rgw_coroutine.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/assert.h"

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

class RGWCRHTTPGetDataCB : public RGWGetDataCB {
  Mutex lock;
  RGWCoroutinesEnv *env;
  RGWCoroutine *cr;
  int64_t io_id;
  bufferlist data;
  bufferlist extra_data;
public:
  RGWCRHTTPGetDataCB(RGWCoroutinesEnv *_env, RGWCoroutine *_cr, int64_t _io_id) : lock("RGWCRHTTPGetDataCB"), env(_env), cr(_cr), io_id(_io_id) {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    {
      Mutex::Locker l(lock);

      if (!has_all_extra_data()) {
        off_t max = extra_data_len - extra_data.length();
        if (max > bl_len) {
          max = bl_len;
        }
        bl.splice(0, max, &extra_data);
        bl_len -= max;
      }

      if (bl_len == bl.length()) {
        data.claim_append(bl);
      } else {
        bl.splice(0, bl_len, &data);
      }
    }

    env->manager->io_complete(cr, io_id);
    return 0;
  }

  void claim_data(bufferlist *dest, uint64_t max) {
    Mutex::Locker l(lock);

    if (data.length() == 0) {
      return;
    }

    if (data.length() < max) {
      max = data.length();
    }

    data.splice(0, max, dest);
  }

  bufferlist& get_extra_data() {
    return extra_data;
  }

  bool has_data() {
    return (data.length() > 0);
  }

  bool has_all_extra_data() {
    return (extra_data.length() == extra_data_len);
  }
};


RGWStreamReadHTTPResourceCRF::~RGWStreamReadHTTPResourceCRF()
{
  delete in_cb;
}

int RGWStreamReadHTTPResourceCRF::init()
{
  env->stack->init_new_io(req);

  in_cb = new RGWCRHTTPGetDataCB(env, caller, req->get_io_id());

  req->set_in_cb(in_cb);

  int r = http_manager->add_request(req);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWStreamWriteHTTPResourceCRF::init()
{
  env->stack->init_new_io(req);

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
#warning need to lock in_req->headers
  *attrs = req->get_out_headers();
}

int RGWStreamReadHTTPResourceCRF::read(bufferlist *out, uint64_t max_size, bool *io_pending)
{
    reenter(&read_state) {
    while (!req->is_done()) {
      *io_pending = true;
      if (!in_cb->has_data()) {
        yield caller->io_block(0, req->get_io_id());
      }
      got_attrs = true;
      if (need_extra_data() && !got_extra_data) {
        if (!in_cb->has_all_extra_data()) {
          continue;
        }
        extra_data.claim_append(in_cb->get_extra_data());
        got_extra_data = true;
      }
      *io_pending = false;
      in_cb->claim_data(out, max_size);
      if (!req->is_done()) {
        yield;
      }
    }
  }
  return 0;
}

void RGWStreamWriteHTTPResourceCRF::send_ready(const map<string, string>& attrs)
{
  for (auto h : attrs) {
    if (h.first == "CONTENT_LENGTH") {
      req->set_send_length(atoi(h.second.c_str()));
    } else {
      req->append_header(h.first, h.second);
    }
  }
}

int RGWStreamWriteHTTPResourceCRF::write(bufferlist& data)
{
#warning write need to throttle and block
  reenter(&write_state) {
    while (!req->is_done()) {
      yield req->add_send_data(data);
    }
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
      yield caller->io_block(0, req->get_io_id());
      *need_retry = !req->is_done();
    }
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

      if (bl.length() == 0) {
        break;
      }

      if (!in_crf->has_attrs()) {
        /* shouldn't happen */
        ldout(cct, 0) << "ERROR: " << __func__ << ": can't handle !in_ctf->has_attrs" << dendl;
        return set_cr_error(-EIO);
      }

      if (!sent_attrs) {
        map<string, string> attrs;
        in_crf->get_attrs(&attrs);
        out_crf->send_ready(attrs);
        int ret = out_crf->init();
        if (ret < 0) {
          return set_cr_error(ret);
        }
        sent_attrs = true;
      }

      total_read += bl.length();

      yield {
        ret = out_crf->write(bl);
        if (ret < 0)  {
          return set_cr_error(ret);
        }
      }

      if (retcode < 0) {
        ldout(cct, 20) << __func__ << ": out_crf->write() retcode=" << retcode << dendl;
        return set_cr_error(ret);
      }

      ldout(cct, 20) << "wrote " << bl.length() << " bytes" << dendl;
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

TestSpliceCR::TestSpliceCR(CephContext *_cct, RGWHTTPManager *_mgr,
                           RGWHTTPStreamRWRequest *_in_req,
                           RGWHTTPStreamRWRequest *_out_req) : RGWCoroutine(_cct), cct(_cct), http_manager(_mgr),
                                                               in_req(_in_req), out_req(_out_req) {
    in_crf.reset(new RGWStreamReadHTTPResourceCRF(cct, get_env(), this, http_manager));
    in_crf->set_req(in_req);
    out_crf.reset(new RGWStreamWriteHTTPResourceCRF(cct, get_env(), this, http_manager));
    out_crf->set_req(out_req);
}

int TestSpliceCR::operate() {
  reenter(this) {
    yield call(new RGWStreamSpliceCR(cct, http_manager, in_crf, out_crf));

    if (retcode < 0) {
      return set_cr_error(retcode);
    }

    return set_cr_done();
  }

  return 0;
};

