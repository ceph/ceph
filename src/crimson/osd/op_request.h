// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include "osd/osd_types.h"
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/intrusive/list_hook.hpp>
#include "msg/Message.h"
class OpRequest;

using OpRef = boost::intrusive_ptr<OpRequest>;
class OpRequest : public boost::intrusive_ref_counter<OpRequest,
                    boost::thread_unsafe_counter>,
                  public boost::intrusive::list_base_hook<> {
private:
  Message* request; /// the logical request we are tracking
  osd_reqid_t reqid;
  entity_inst_t req_src_inst;
public:
  epoch_t sent_epoch = 0;     ///< client's map epoch
  epoch_t min_epoch = 0;      ///< min epoch needed to handle this msg

  OpRequest(Message* req);
  ~OpRequest(){ request->put();}  
  const Message *get_req() const { return request; }
  Message *get_nonconst_req() { return request; }
};
