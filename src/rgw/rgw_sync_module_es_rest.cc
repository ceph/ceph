#include "rgw_sync_module_es.h"
#include "rgw_sync_module_es_rest.h"
#include "rgw_es_query.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

class RGWMetadataSearchOp : public RGWOp {
  RGWElasticSyncModuleInstance *es_module;
protected:
  string expression;

public:
  RGWMetadataSearchOp(RGWElasticSyncModuleInstance *_es_module) : es_module(_es_module) {}

  int verify_permission() {
    return 0;
  }
  virtual int get_params() = 0;
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "metadata_search"; }
  virtual RGWOpType get_type() { return RGW_OP_METADATA_SEARCH; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

void RGWMetadataSearchOp::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}


void RGWMetadataSearchOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0)
    return;

  ESQueryCompiler es_query(expression);
  
  bool valid = es_query.compile();
  if (!valid) {
    ldout(s->cct, 10) << "invalid query, failed generating request json" << dendl;
    op_ret = -EINVAL;
    return;
  }

  RGWRESTConn *conn = es_module->get_rest_conn();
  // conn->
}

class RGWMetadataSearch_ObjStore_S3 : public RGWMetadataSearchOp {
public:
  RGWMetadataSearch_ObjStore_S3(RGWElasticSyncModuleInstance *_es_module) : RGWMetadataSearchOp(_es_module) {}

  int get_params() override {
    expression = s->info.args.get("query");
    return 0;
  }
  void send_response() override {
    if (op_ret)
      set_req_state_err(s, op_ret);
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    // TODO

  }
};

class RGWHandler_REST_MDSearch_S3 : public RGWHandler_REST_S3 {
  RGWElasticSyncModuleInstance *es_module;
protected:
  RGWOp *op_get() {
    if (!s->info.args.exists("query")) {
      return nullptr;
    }
    return new RGWMetadataSearch_ObjStore_S3(es_module);
  }
  RGWOp *op_head() {
    return nullptr;
  }
  RGWOp *op_post() {
    return nullptr;
  }
public:
  RGWHandler_REST_MDSearch_S3(const rgw::auth::StrategyRegistry& auth_registry,
                              RGWElasticSyncModuleInstance *_es_module) : RGWHandler_REST_S3(auth_registry), es_module(_es_module) {}
  virtual ~RGWHandler_REST_MDSearch_S3() {}
};


RGWHandler_REST* RGWRESTMgr_MDSearch_S3::get_handler(struct req_state* const s,
                                                     const rgw::auth::StrategyRegistry& auth_registry,
                                                     const std::string& frontend_prefix)
{
  int ret =
    RGWHandler_REST_S3::init_from_header(s,
					RGW_FORMAT_XML, true);
  if (ret < 0) {
    return nullptr;
  }

  if (!s->object.empty()) {
    return nullptr;
  }

  RGWHandler_REST *handler = new RGWHandler_REST_MDSearch_S3(auth_registry, es_module);

  ldout(s->cct, 20) << __func__ << " handler=" << typeid(*handler).name()
		    << dendl;
  return handler;
}

