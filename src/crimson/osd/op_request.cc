// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 

#include "op_request.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"

OpRequest::OpRequest(Message* req) :
  request(req)
{
  if (req->get_type() == CEPH_MSG_OSD_OP) {
    reqid = static_cast<MOSDOp*>(req)->get_reqid();
  } else if (req->get_type() == MSG_OSD_REPOP) {
    reqid = static_cast<MOSDRepOp*>(req)->reqid;
  } else if (req->get_type() == MSG_OSD_REPOPREPLY) {
    reqid = static_cast<MOSDRepOpReply*>(req)->reqid;
  }
  req_src_inst = req->get_source_inst();
}
