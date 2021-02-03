#include "rgw_cr_sip.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_sync_info.h"

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int SIProviderCRMgr::Instance::GetNextStageCR::operate()
{
  reenter(this) {
    yield call(mgri->get_stages_cr(&stages));
    if (retcode < 0) {
      return set_cr_error(retcode);
    }

    bool found = (sid.empty()); /* for empty stage id return the first stage */
    for (auto& stage : stages) {
      if (found) {
        *next_sid = stage;
        return set_cr_done();
      }

      if (stage == sid) {
        found = true;
      }
    }

    int ret = (found ? -ENODATA : -ENOENT);
    return set_cr_error(ret);
  }

  return 0;
}

template <class T>
class RGWSafeRetAsyncCR : public RGWCoroutine {
  friend struct Action;

  RGWAsyncRadosProcessor *async_rados;

  T *pret;
  std::function<int(T *)> cb;
  std::function<int()> cb_void;

  struct Action : public RGWGenericAsyncCR::Action {
    RGWSafeRetAsyncCR *caller;
    T ret;

    Action(RGWSafeRetAsyncCR *_caller) : caller(_caller) {}

    int operate() override {
      return caller->cb(&ret);
    }
  };

  std::shared_ptr<Action> action;

public:
  RGWSafeRetAsyncCR(CephContext *cct,
                    RGWAsyncRadosProcessor *_async_rados,
                    T *_pret,
                    std::function<int(T *)> _cb) : RGWCoroutine(cct),
                               async_rados(_async_rados),
                               pret(_pret),
                               cb(_cb) {}
  int operate() {
    reenter(this) {
      action = make_shared<Action>(this);

      yield call(new RGWGenericAsyncCR(cct, async_rados, action));

      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      /* now it is safe to copy return value */
      if (pret) {
        *pret = action->ret;
      }

      return set_cr_done();
    }
    return 0;
  }
};

template <class T>
class RGWAsyncLambdaCR : public RGWCoroutine {
  friend struct Action;

  RGWAsyncRadosProcessor *async_rados;

  std::function<int()> cb;

  struct Action : public RGWGenericAsyncCR::Action {
    RGWAsyncLambdaCR *caller;

    Action(RGWAsyncLambdaCR *_caller) : caller(_caller) {}

    int operate() override {
      return caller->cb();
    }
  };

  std::shared_ptr<Action> action;

public:
  RGWAsyncLambdaCR(CephContext *cct,
                   RGWAsyncRadosProcessor *_async_rados,
                   std::function<int()> _cb) : RGWCoroutine(cct),
                               async_rados(_async_rados),
                               cb(_cb) {}

  int operate() {
    reenter(this) {
      action = make_shared<Action>(this);

      yield call(new RGWGenericAsyncCR(cct, async_rados, action));

      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }
    return 0;
  }
};


template <typename... Ts>
class RGWSafeMultiRetAsyncCR : public RGWCoroutine
{
  friend struct Action;

  RGWAsyncRadosProcessor *async_rados;

  std::tuple<Ts*...> pret;
  std::function<int(Ts*... )> cb;

  struct Action : public RGWGenericAsyncCR::Action {
    RGWSafeMultiRetAsyncCR *caller;
    std::tuple<Ts...> ret;

    Action(RGWSafeMultiRetAsyncCR *_caller) : caller(_caller) {}

    int operate() {
      return call_cb(ret,
                     std::index_sequence_for<Ts...>());
    }

    template<std::size_t... Is>
    int call_cb(std::tuple<Ts...>& v,
                std::index_sequence<Is...>) {
      return caller->cb(&std::get<Is>(v)...);
    }
  };

  std::shared_ptr<Action> action;

public:
  RGWSafeMultiRetAsyncCR(CephContext *cct,
                    RGWAsyncRadosProcessor *_async_rados,
                    Ts*... _pret,
                    std::function<int(Ts*... )> _cb) : RGWCoroutine(cct),
                                                       pret(_pret...),
                                                       cb(_cb) {}

  int operate() {
    reenter(this) {
      action = make_shared<Action>(this);

      yield call(new RGWGenericAsyncCR(cct, async_rados, action));

      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      apply_ret(pret,
                action->ret,
                std::index_sequence_for<Ts...>());

      return set_cr_done();
    }

    return 0;
  }

  template <class T>
  int assign_ret(T* p, T& v) {
    if (p) {
      *p = v;
    }
    return 0;
  }

  template<typename... Is>
  void apply_ret_helper(Is...) {}

  template<std::size_t... Is>
  void apply_ret(std::tuple<Ts*...>& p,
                std::tuple<Ts...>& v,
                std::index_sequence<Is...>) {
    apply_ret_helper(assign_ret(std::get<Is>(p), std::get<Is>(v))... );
  }
};


SIProviderCRMgrInstance_Local *SIProviderCRMgr_Local::alloc_instance(SIProviderRef& _provider)
{
  return new SIProviderCRMgrInstance_Local(this, _provider);
}

RGWCoroutine *SIProviderCRMgr_Local::list_cr(std::vector<std::string> *providers)
{
   *providers = ctl.si.mgr->list_sip();
   return nullptr;
}

SIProviderCRMgrInstance_Local::SIProviderCRMgrInstance_Local(SIProviderCRMgr_Local *_mgr,
                                                             SIProviderRef& _provider) : SIProviderCRMgr::Instance(_mgr->get_dpp()),
                                                                         mgr(_mgr),
                                                                         provider(_provider) {
}

RGWCoroutine *SIProviderCRMgrInstance_Local::get_stages_cr(std::vector<SIProvider::stage_id_t> *stages)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<std::vector<SIProvider::stage_id_t> >(cct,
                               mgr->async_rados,
                               stages,
                               [=](std::vector<SIProvider::stage_id_t> *_stages) {
                                 *_stages = pvd->get_stages(dpp);
                                 return 0;
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<SIProvider::StageInfo>(cct,
                               mgr->async_rados,
                               stage_info,
                               [=](SIProvider::StageInfo *_stage_info) {
                                 return pvd->get_stage_info(dpp, sid, _stage_info);
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::init_cr()
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<SIProvider::Info>(cct,
                               mgr->async_rados,
                               &info,
                               [=](SIProvider::Info *_info) {
                                 *_info = pvd->get_info(dpp);
                                 return 0;
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<SIProvider::fetch_result>(cct,
                               mgr->async_rados,
                               result,
                               [=](SIProvider::fetch_result *_result) {
                                 return pvd->fetch(dpp, sid, shard_id, marker, max, _result);
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<rgw_sip_pos>(cct,
                               mgr->async_rados,
                               pos,
                               [=](rgw_sip_pos *_pos) {
                                 return pvd->get_start_marker(dpp, sid, shard_id, &_pos->marker, &_pos->timestamp);
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos, bool *disabled)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeMultiRetAsyncCR<rgw_sip_pos, bool>(cct,
                               mgr->async_rados,
                               pos,
                               disabled,
                               [=](rgw_sip_pos *_pos, bool *_disabled) {
                                 return pvd->get_cur_state(dpp, sid, shard_id, &_pos->marker, &_pos->timestamp, _disabled, null_yield);
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const string& marker)
{
  auto pvd = provider; /* capture another reference */
  return new RGWAsyncLambdaCR<void>(cct,
                               mgr->async_rados,
                               [=]() {
                                 return pvd->trim(dpp, sid, shard_id, marker);
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::update_marker_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                                      const RGWSI_SIP_Marker::SetParams& params)
{
  auto pvd = provider; /* capture another reference */
  return new RGWAsyncLambdaCR<void>(cct,
                               mgr->async_rados,
                               [=]() {
                                 auto marker_handler = mgr->svc.sip_marker->get_handler(provider);
                                 if (!marker_handler) {
                                   ldpp_dout(dpp, 0) << "ERROR: can't get sip marker handler" << dendl;
                                   return -EIO;
                                 }

                                 RGWSI_SIP_Marker::Handler::modify_result result;

                                 int r = marker_handler->set_marker(sid, shard_id, params, &result);
                                 if (r < 0) {
                                   ldpp_dout(dpp, 0) << "ERROR: failed to set target marker info: r=" << r << dendl;
                                   return r;
                                 }
                                 return 0;
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::set_min_source_pos_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                                           const string& pos)
{
  auto pvd = provider; /* capture another reference */
  return new RGWAsyncLambdaCR<void>(cct,
                               mgr->async_rados,
                               [=]() {
                                 auto marker_handler = mgr->svc.sip_marker->get_handler(provider);
                                 if (!marker_handler) {
                                   ldpp_dout(dpp, 0) << "ERROR: can't get sip marker handler" << dendl;
                                   return -EIO;
                                 }

                                 int r = marker_handler->set_min_source_pos(sid, shard_id, pos);
                                 if (r < 0) {
                                   ldpp_dout(dpp, 0) << "ERROR: failed to set marker min source pos info: r=" << r << dendl;
                                   return r;
                                 }
                                 return 0;
                               });
}

RGWCoroutine *SIProviderCRMgrInstance_Local::get_marker_info_cr(RGWSI_SIP_Marker::HandlerRef& marker_handler,
                                                        const SIProvider::stage_id_t& sid, int shard_id,
                                                        RGWSI_SIP_Marker::stage_shard_info *info)
{
  auto mh = marker_handler;
  return new RGWSafeRetAsyncCR<RGWSI_SIP_Marker::stage_shard_info>(cct,
                               mgr->async_rados,
                               info,
                               [=](RGWSI_SIP_Marker::stage_shard_info *_info) {
                                 return mh->get_info(sid, shard_id, _info);
                               });
}

SIProviderCRMgrInstance_REST *SIProviderCRMgr_REST::alloc_instance(const string& remote_provider_name,
                                                                   SIProvider::TypeHandlerProvider *type_provider,
                                                                   std::optional<string> instance)
{
  return new SIProviderCRMgrInstance_REST(this,
                                          remote_provider_name,
                                          type_provider,
                                          instance);
}

SIProviderCRMgrInstance_REST *SIProviderCRMgr_REST::alloc_instance(const string& data_type,
                                                                   SIProvider::StageType stage_type,
                                                                   SIProvider::TypeHandlerProvider *type_provider,
                                                                   std::optional<string> instance)
{
  return new SIProviderCRMgrInstance_REST(this,
                                          data_type,
                                          stage_type,
                                          type_provider,
                                          instance);
}

struct SIProviderRESTCRs {
  class ListProvidersCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;

    string path;
    std::vector<std::string> *providers;
  public:
    ListProvidersCR(SIProviderCRMgr_REST *_mgr,
                    std::vector<std::string> *_providers) : RGWCoroutine(_mgr->ctx()),
                                                            mgr(_mgr),
                                                            providers(_providers) {
      path = mgr->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          rgw_http_param_pair pairs[] = { { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgr->ctx(),
                                         mgr->conn,
                                         mgr->http_manager,
                                         path,
                                         pairs,
                                         providers));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class GetStagesInfoCR : public RGWCoroutine {
    SIProviderCRMgrInstance_REST *mgri;

    string path;
    SIProvider::Info *info;
  public:
    GetStagesInfoCR(SIProviderCRMgrInstance_REST *_mgri,
                    SIProvider::Info *_info) : RGWCoroutine(_mgri->ctx()),
                                               mgri(_mgri),
                                               info(_info) {
      path = mgri->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          string stage_type_str;
          if (mgri->stage_type) {
            stage_type_str = SIProvider::stage_type_to_str(*mgri->stage_type);
          }
          const char *instance_key = (mgri->instance ? "instance" : "");
          const char *instance_val = (mgri->instance ? mgri->instance->c_str() : "");
          const char *provider_key = (mgri->remote_provider_name ? "provider": "");
          const char *provider_val = (mgri->remote_provider_name ? mgri->remote_provider_name->c_str() : "");
          const char *data_type_key = (mgri->data_type ? "data-type" : "");
          const char *data_type_val = (mgri->data_type ? mgri->data_type->c_str() : "");
          const char *stage_type_key = (mgri->stage_type ? "stage-type" : "");
          const char *stage_type_val = (mgri->stage_type ? stage_type_str.c_str() : "");
          rgw_http_param_pair pairs[] = { { "info", nullptr },
					  { provider_key, provider_val },
					  { data_type_key, data_type_val },
					  { stage_type_key, stage_type_val },
					  { instance_key, instance_val },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgri->ctx(),
                                         mgri->conn,
                                         mgri->http_manager,
                                         path,
                                         pairs,
                                         info));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class GetStagesCR : public RGWCoroutine {
    SIProviderCRMgrInstance_REST *mgri;
    std::vector<SIProvider::stage_id_t> *result;

    SIProvider::Info info;
  public:
    GetStagesCR(SIProviderCRMgrInstance_REST *_mgri,
                std::vector<SIProvider::stage_id_t> *_result) : RGWCoroutine(_mgri->ctx()),
                                                                mgri(_mgri),
                                                                result(_result) {
    }

    int operate() override {
      reenter(this) {
        yield call(new GetStagesInfoCR(mgri, &info));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        result->clear();
        result->reserve(info.stages.size());

        for (auto& sinfo : info.stages) {
          result->push_back(sinfo.sid);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class GetStageInfoCR : public RGWCoroutine {
    SIProviderCRMgrInstance_REST *mgri;
    SIProvider::stage_id_t sid;

    SIProvider::Info info;
    SIProvider::StageInfo *sinfo;
  public:
    GetStageInfoCR(SIProviderCRMgrInstance_REST *_mgri,
                   const SIProvider::stage_id_t& _sid,
                   SIProvider::StageInfo *_sinfo) : RGWCoroutine(_mgri->ctx()),
                                                    mgri(_mgri),
                                                    sid(_sid),
                                                    sinfo(_sinfo) {
    }

    int operate() override {
      reenter(this) {
        yield call(new GetStagesInfoCR(mgri, &info));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        for (auto& si : info.stages) {
          if (si.sid == sid) {
            *sinfo = si;
            return set_cr_done();
          }
        }

        ldpp_dout(mgri->get_dpp(), 10) << "GetStageInfoCR(): sid not found: provider=" << mgri->remote_provider_name << " sid=" << sid << dendl;

        return set_cr_error(-ENOENT);
      }

      return 0;
    }
  };

  class FetchCR : public RGWCoroutine {
    SIProviderCRMgrInstance_REST *mgri;
    SIProvider::stage_id_t sid;
    int shard_id;
    string marker;
    int max;

    string  path;

    bufferlist bl;
    SIProvider::fetch_result *result;

  public:
    FetchCR(SIProviderCRMgrInstance_REST *_mgri,
            const SIProvider::stage_id_t& _sid,
            int _shard_id,
            const string& _marker,
            int _max,
            SIProvider::fetch_result *_result) : RGWCoroutine(_mgri->ctx()),
                                                 mgri(_mgri),
                                                 sid(_sid),
                                                 shard_id(_shard_id),
                                                 marker(_marker),
                                                 max(_max),
                                                 result(_result) {
      path = mgri->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgri->instance ? "instance" : "");
          const char *instance_val = (mgri->instance ? mgri->instance->c_str() : "");
          char max_buf[16];
          snprintf(max_buf, sizeof(max_buf), "%d", max);
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          rgw_http_param_pair pairs[] = { { "provider" , mgri->get_info().name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
					  { "max" , max_buf },
					  { "marker" , marker.c_str() },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgri->ctx(),
                                         mgri->conn,
                                         mgri->http_manager,
                                         path,
                                         pairs,
                                         &bl));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }


        JSONParser p;
        if (!p.parse(bl.c_str(), bl.length())) {
          ldpp_dout(mgri->get_dpp(), 0) << "ERROR: failed to parse fetch result: bl=" << bl.to_str() << dendl;
          return set_cr_error(-EIO);
        }

        auto type_handler = mgri->type_provider->get_type_handler();
        if (!type_handler) {
          ldpp_dout(mgri->get_dpp(), 0) << "ERROR: " << __func__ << "(): get_type_provider for sid=" << sid << " is null, likely a bug" << dendl;
          return set_cr_error(-EIO);
        }

        int r = type_handler->decode_json_results(sid, &p, result);
        if (r < 0) {
          ldpp_dout(mgri->get_dpp(), 0) << "ERROR: failed to decode fetch result: bl=" << bl.to_str() << dendl;
          return set_cr_error(r);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class GetStagesStatusCR : public RGWCoroutine {
    SIProviderCRMgrInstance_REST *mgri;
    SIProvider::stage_id_t sid;
    int shard_id;

    rgw_sip_pos *start_pos;
    rgw_sip_pos *cur_pos;
    bool *disabled;

    string path;

    struct {
      struct {
        rgw_sip_pos start;
        rgw_sip_pos current;

        void decode_json(JSONObj *obj) {
          JSONDecoder::decode_json("start", start, obj);
          JSONDecoder::decode_json("current", current, obj);
        }
      } markers;

      bool disabled{false};

      void decode_json(JSONObj *obj) {
        JSONDecoder::decode_json("markers", markers, obj);
        JSONDecoder::decode_json("disabled", disabled, obj);
      }
    } status;

  public:
    GetStagesStatusCR(SIProviderCRMgrInstance_REST *_mgri,
                      const SIProvider::stage_id_t& _sid,
                      int _shard_id,
                      rgw_sip_pos *_start_pos,
                      rgw_sip_pos *_cur_pos,
                      bool *_disabled) : RGWCoroutine(_mgri->ctx()),
                                              mgri(_mgri),
                                              sid(_sid),
                                              shard_id(_shard_id),
                                              start_pos(_start_pos),
                                              cur_pos(_cur_pos),
                                              disabled(_disabled) {
      path = mgri->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgri->instance ? "instance" : "");
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          const char *instance_val = (mgri->instance ? mgri->instance->c_str() : "");
          rgw_http_param_pair pairs[] = { { "status", nullptr },
					  { "provider" , mgri->get_info().name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgri->ctx(),
                                         mgri->conn,
                                         mgri->http_manager,
                                         path,
                                         pairs,
                                         &status));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        if (start_pos) {
          *start_pos = status.markers.start;
        }

        if (cur_pos) {
          *cur_pos = status.markers.current;
        }

        if (disabled) {
          *disabled = status.disabled;
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class TrimCR : public RGWCoroutine {
    SIProviderCRMgrInstance_REST *mgri;
    SIProvider::stage_id_t sid;
    int shard_id;
    string marker;

    string path;

  public:
    TrimCR(SIProviderCRMgrInstance_REST *_mgri,
           const SIProvider::stage_id_t& _sid,
           int _shard_id,
           const string& _marker) : RGWCoroutine(_mgri->ctx()),
                                              mgri(_mgri),
                                              sid(_sid),
                                              shard_id(_shard_id),
                                              marker(_marker) {
      path = mgri->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgri->instance ? "instance" : "");
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          const char *instance_val = (mgri->instance ? mgri->instance->c_str() : "");
          rgw_http_param_pair pairs[] = { { "provider" , mgri->get_info().name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
					  { "marker" , marker.c_str() },
	                                  { nullptr, nullptr } };
          call(new RGWDeleteRESTResourceCR(mgri->ctx(),
                                           mgri->conn,
                                           mgri->http_manager,
                                           path,
                                           pairs));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class UpdateMarkerCR : public RGWCoroutine {
    SIProviderCRMgrInstance_REST *mgri;
    SIProvider::stage_id_t sid;
    int shard_id;

    RGWSI_SIP_Marker::SetParams params;

    string path;

  public:
    UpdateMarkerCR(SIProviderCRMgrInstance_REST *_mgri,
           const SIProvider::stage_id_t& _sid,
           int _shard_id,
           const RGWSI_SIP_Marker::SetParams& _params) : RGWCoroutine(_mgri->ctx()),
                                                         mgri(_mgri),
                                                         sid(_sid),
                                                         shard_id(_shard_id),
                                                         params(_params)  {
      path = mgri->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgri->instance ? "instance" : "");
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          const char *instance_val = (mgri->instance ? mgri->instance->c_str() : "");
          rgw_http_param_pair pairs[] = { { "marker-info", nullptr },
                                          { "provider" , mgri->get_info().name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
	                                  { nullptr, nullptr } };
          call(new RGWPutRESTResourceCR<RGWSI_SIP_Marker::SetParams, int>(mgri->ctx(),
                                                                     mgri->conn,
                                                                     mgri->http_manager,
                                                                     path,
                                                                     pairs,
                                                                     params,
                                                                     nullptr));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        return set_cr_done();
      }

      return 0;
    }
  };
};

RGWCoroutine *SIProviderCRMgr_REST::list_cr(std::vector<std::string> *providers)
{
   return new SIProviderRESTCRs::ListProvidersCR(this, providers);
}

RGWCoroutine *SIProviderCRMgrInstance_REST::init_cr()
{
  return new SIProviderRESTCRs::GetStagesInfoCR(this, &info);
}

RGWCoroutine *SIProviderCRMgrInstance_REST::get_stages_cr(std::vector<SIProvider::stage_id_t> *stages)
{
  return new SIProviderRESTCRs::GetStagesCR(this, stages);
}

RGWCoroutine *SIProviderCRMgrInstance_REST::get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *sinfo)
{
  return new SIProviderRESTCRs::GetStageInfoCR(this, sid, sinfo);
}

RGWCoroutine *SIProviderCRMgrInstance_REST::fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result)
{
  return new SIProviderRESTCRs::FetchCR(this, sid, shard_id,
                                        marker, max, result);
}

RGWCoroutine *SIProviderCRMgrInstance_REST::get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos)
{
  return new SIProviderRESTCRs::GetStagesStatusCR(this, sid, shard_id,
                                                  pos, nullptr, nullptr);
}

RGWCoroutine *SIProviderCRMgrInstance_REST::get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos, bool *disabled)
{
  return new SIProviderRESTCRs::GetStagesStatusCR(this, sid, shard_id,
                                                  nullptr, pos,
                                                  disabled);
}

RGWCoroutine *SIProviderCRMgrInstance_REST::trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const string& marker)
{
  return new SIProviderRESTCRs::TrimCR(this, sid, shard_id, marker);
}

RGWCoroutine *SIProviderCRMgrInstance_REST::update_marker_cr(const SIProvider::stage_id_t& sid, int shard_id,
                                                     const RGWSI_SIP_Marker::SetParams& params)
{
  return new SIProviderRESTCRs::UpdateMarkerCR(this, sid, shard_id, params);
}

SIProvider::TypeHandler *SIProviderCRMgrInstance_REST::get_type_handler()
{
  return type_provider->get_type_handler();
}

