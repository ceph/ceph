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

#pragma once

#include "rgw_sal_filter.h"
#include "rgw_sal.h"
#include "rgw_d3n_datacache.h"

namespace rgw { namespace sal {

class D3NFilterDriver : public FilterDriver {
  private:
    std::unique_ptr<D3nDataCache> d3n_cache;

  public:
    D3NFilterDriver(Driver* next) : FilterDriver(next) 
    {
      d3n_cache = std::make_unique<D3nDataCache>();
    }
    virtual ~D3NFilterDriver() = default;

    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
    D3nDataCache* get_d3n_cache() { return d3n_cache.get(); }
    Driver* get_next() { return next;}
};

class D3NFilterBucket: public FilterBucket {
private:
  D3NFilterDriver* filter;
public:
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;

  D3NFilterBucket(D3NFilterDriver *filter, std::unique_ptr<Bucket> b)
      : FilterBucket(std::move(b)),
	      filter(filter) {}
};

class D3NFilterObject : public FilterObject {
private:
  D3NFilterDriver* filter;

public:
  struct D3NFilterReadOp : FilterReadOp {

    class D3NFilterGetCB: public RGWGetDataCB {
    public:
      D3NFilterGetCB(D3NFilterDriver* filter, std::string& oid) : filter(filter), oid(oid) {};
      int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override;
      void set_client_cb(RGWGetDataCB* client_cb) { this->client_cb = client_cb;}
      void set_ofs(uint64_t ofs) { this->ofs = ofs; }
      int flush_last_part();
      void bypass_cache_write() { this->write_to_cache = false; }
    private:
      D3NFilterDriver* filter;
      std::string oid;
      RGWGetDataCB* client_cb;
      uint64_t ofs = 0, len = 0;
      bufferlist bl_rem;
      bool last_part{false};
      D3nGetObjData d3n_get_data;
      bool write_to_cache{true};
    };
  
    D3NFilterObject* source;
    D3NFilterDriver* filter;
    std::unique_ptr<RGWObjectCtx> rctx;

    D3NFilterReadOp(std::unique_ptr<ReadOp> next, D3NFilterObject* source, D3NFilterDriver* filter) : FilterReadOp(std::move(next)),
										 source(source),
                     filter(filter) { std::string oid = source->get_bucket()->get_marker() + "_" + source->get_key().get_oid();
                                      cb = std::make_unique<D3NFilterGetCB>(filter, oid); }
    virtual ~D3NFilterReadOp() = default;
    virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			RGWGetDataCB* cb, optional_yield y) override;
    virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;

  private:
    RGWGetDataCB* client_cb;
    std::unique_ptr<D3NFilterGetCB> cb;
    std::unique_ptr<rgw::Aio> aio;
    uint64_t offset = 0; // next offset to write to client
    rgw::AioResultList completed; // completed read results, sorted by offset

    int flush(const DoutPrefixProvider* dpp, rgw::AioResultList&& results);
    void cancel();
    int drain(const DoutPrefixProvider* dpp);
  };
  D3NFilterObject(std::unique_ptr<Object> next, D3NFilterDriver* filter) : FilterObject(std::move(next)),
									                                                          filter(filter) {}
  D3NFilterObject(std::unique_ptr<Object> next, Bucket* bucket, D3NFilterDriver* filter) : FilterObject(std::move(next), bucket),
			                                                              filter(filter) {}
  virtual std::unique_ptr<Object::ReadOp> get_read_op() override;
};
} } // namespace rgw::sal