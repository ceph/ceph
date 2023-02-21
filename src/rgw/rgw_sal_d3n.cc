// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_sal_d3n.h"
#include "rgw_aio_throttle.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace sal {

class RadosStore;

int D3NFilterDriver::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  FilterDriver::initialize(cct, dpp);
  d3n_cache->init(cct);

  return 0;
}

std::unique_ptr<Object> D3NFilterDriver::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> obj = next->get_object(k);
  return std::make_unique<D3NFilterObject>(std::move(obj), this);
}

std::unique_ptr<Object> D3NFilterBucket::get_object(const rgw_obj_key& k)
{
  std::unique_ptr<Object> obj = next->get_object(k);
  return std::make_unique<D3NFilterObject>(std::move(obj), this, this->filter);
}

std::unique_ptr<Object::ReadOp> D3NFilterObject::get_read_op()
{
  std::unique_ptr<Object::ReadOp> rop = next->get_read_op();
  return std::make_unique<D3NFilterReadOp>(std::move(rop), this, filter);
}

int D3NFilterObject::D3NFilterReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
{
  return next->prepare(y, dpp);
}

int D3NFilterObject::D3NFilterReadOp::get_obj_iterate_cb(const DoutPrefixProvider *dpp, const rgw_raw_obj& read_obj, off_t obj_ofs,
                                 off_t read_ofs, off_t len, bool is_head_obj,
                                 RGWObjState *astate, void *arg)
{
  ldpp_dout(dpp, 30) << "D3NFilterObject::get_obj_iterate_cb::" << __func__ << "(): is head object : " << is_head_obj << dendl;
  librados::ObjectReadOperation op;
  struct get_obj_priv_data* priv_data = static_cast<struct get_obj_priv_data*>(arg);
  struct get_obj_data *d = priv_data->data;
  D3NFilterDriver* filter = priv_data->filter;

  std::string oid, key;

  if (is_head_obj) {
    // only when reading from the head object do we need to do the atomic test
    int r = d->rgwrados->append_atomic_test(dpp, astate, op);
    if (r < 0)
      return r;

    if (astate &&
        obj_ofs < astate->data.length()) {
      unsigned chunk_len = std::min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);

      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      if (r < 0)
        return r;

      len -= chunk_len;
      d->offset += chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
        return 0;
    }

    rgw_rados_ref ref;
    r = rgw_get_rados_ref(dpp, d->rgwrados->get_rados_handle(), read_obj, &ref);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "D3NFilterObject::get_obj_iterate_cb:: Error: failed to open rados context for " << read_obj << ", r=" << r << dendl;
      return r;
    }

    ldpp_dout(dpp, 20) << "D3NFilterObject::get_obj_iterate_cb::" << __func__ << "(): oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
    op.read(read_ofs, len, nullptr, nullptr);

    const uint64_t cost = len;
    const uint64_t id = obj_ofs; // use logical object offset for sorting replies

    auto completed = d->aio->get(ref.obj, rgw::Aio::librados_op(ref.ioctx, std::move(op), d->yield), cost, id);
    return d->flush(std::move(completed));
  } else {
    ldpp_dout(dpp, 20) << "D3NFilterObject::get_obj_iterate_cb::" << __func__ << "(): oid=" << read_obj.oid << ", is_head_obj=" << is_head_obj << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << ", len=" << len << dendl;
    int r;

    op.read(read_ofs, len, nullptr, nullptr);

    const uint64_t cost = len;
    const uint64_t id = obj_ofs; // use logical object offset for sorting replies
    oid = read_obj.oid;

    rgw_rados_ref ref;
    r = rgw_get_rados_ref(dpp, d->rgwrados->get_rados_handle(), read_obj, &ref);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "D3NFilterObject::get_obj_iterate_cb:: Error: failed to open rados context for " << read_obj << ", r=" << r << dendl;
      return r;
    }
    const bool is_compressed = (astate->attrset.find(RGW_ATTR_COMPRESSION) != astate->attrset.end());
    const bool is_encrypted = (astate->attrset.find(RGW_ATTR_CRYPT_MODE) != astate->attrset.end());
    if (read_ofs != 0 || astate->size != astate->accounted_size || is_compressed || is_encrypted) {
      d->d3n_bypass_cache_write = true;
      ldpp_dout(dpp, 20) << "D3NFilterObject::get_obj_iterate_cb:: " << __func__ << "(): Note - bypassing datacache: oid=" << read_obj.oid << ", read_ofs!=0 = " << read_ofs << ", size=" << astate->size << " != accounted_size=" << astate->accounted_size << ", is_compressed=" << is_compressed << ", is_encrypted=" << is_encrypted  << dendl;
      auto completed = d->aio->get(ref.obj, rgw::Aio::librados_op(ref.ioctx, std::move(op), d->yield), cost, id);
      r = d->flush(std::move(completed));
      return r;
    }

    if (filter->get_d3n_cache()->get(oid, len)) {
      // Read From Cache
      ldpp_dout(dpp, 20) << "D3NFilterObject::get_obj_iterate_cb:: " << __func__ << "(): READ FROM CACHE: oid=" << read_obj.oid << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << ", len=" << len << dendl;
      auto completed = d->aio->get(ref.obj, rgw::Aio::d3n_cache_op(dpp, d->yield, read_ofs, len, d->rgwrados->d3n_data_cache->cache_location), cost, id);
      r = d->flush(std::move(completed));
      if (r < 0) {
        lsubdout(g_ceph_context, rgw, 0) << "D3NFilterObject::get_obj_iterate_cb:: " << __func__ << "(): Error: failed to drain/flush, r= " << r << dendl;
      }
      return r;
    } else {
      // Write To Cache
      ldpp_dout(dpp, 20) << "D3NFilterObject::get_obj_iterate_cb:: " << __func__ << "(): WRITE TO CACHE: oid=" << read_obj.oid << ", obj-ofs=" << obj_ofs << ", read_ofs=" << read_ofs << " len=" << len << dendl;
      auto completed = d->aio->get(ref.obj, rgw::Aio::librados_op(ref.ioctx, std::move(op), d->yield), cost, id);
      return d->flush(std::move(completed));
    }
  }
  ldpp_dout(dpp, 20) << "D3NFilterObject::get_obj_iterate_cb:: " << __func__ << "(): Warning: Check head object cache handling flow, oid=" << read_obj.oid << dendl;

  return 0;
}


int D3NFilterObject::D3NFilterReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			RGWGetDataCB* cb, optional_yield y)
{
  rgw::sal::Driver* driver = filter->get_next();
  rgw::sal::RadosStore* rados_store = static_cast<rgw::sal::RadosStore*>(driver);
  RGWRados* rados = rados_store->getRados();
  CephContext *cct = rados->ctx();
  const uint64_t chunk_size = cct->_conf->rgw_get_obj_max_req_size;
  const uint64_t window_size = cct->_conf->rgw_get_obj_window_size;

  rctx = std::make_unique<RGWObjectCtx>(driver);
  auto aio = rgw::make_throttle(window_size, y);
  get_obj_data data(rados, cb, &*aio, ofs, y);
  get_obj_priv_data priv_data(&data, filter);
  
  int r = rados->iterate_obj(dpp, *rctx, source->get_bucket()->get_info(),
			                      source->get_obj(), ofs, end, chunk_size, get_obj_iterate_cb, &priv_data, y);
  
  if (r < 0) {
    ldpp_dout(dpp, 0) << "iterate_obj() failed with " << r << dendl;
    data.cancel();
    return r;
  }

  return data.drain();
}

} }// namespace rgw::sal

extern "C" {

rgw::sal::Driver* newD3NFilter(rgw::sal::Driver* next)
{
  rgw::sal::D3NFilterDriver* filter = new rgw::sal::D3NFilterDriver(next);

  return filter;
}

}