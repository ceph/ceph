#include "rgw_cr_sip.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_sync_info.h"

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int SIProviderCRMgr::GetNextStageCR::operate()
{
  reenter(this) {
    yield call(mgr->get_stages_cr(&stages));
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


#if WILL_BE_NEEDED_IN_THE_FUTURE

template <typename... Ts>
class RGWSafeRetAsyncCR : public RGWCoroutine
{
  friend struct Action;

  RGWAsyncRadosProcessor *async_rados;

  std::tuple<Ts*...> pret;
  std::function<int(Ts*... )> cb;

  struct Action : public RGWGenericAsyncCR::Action {
    RGWSafeRetAsyncCR *caller;
    std::tuple<Ts...> ret;

    Action(RGWSafeRetAsyncCR *_caller) : caller(_caller) {}

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
  RGWSafeRetAsyncCR(CephContext *cct,
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
    *p = v;
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

#endif


RGWCoroutine *SIProviderCRMgr_Local::get_stages_cr(std::vector<SIProvider::stage_id_t> *stages)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<std::vector<SIProvider::stage_id_t> >(cct,
                               async_rados,
                               stages,
                               [=](std::vector<SIProvider::stage_id_t> *_stages) {
                                 *_stages = pvd->get_stages();
                                 return 0;
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *stage_info)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<SIProvider::StageInfo>(cct,
                               async_rados,
                               stage_info,
                               [=](SIProvider::StageInfo *_stage_info) {
                                 return pvd->get_stage_info(sid, _stage_info);
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::get_info_cr(SIProvider::Info *info)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<SIProvider::Info>(cct,
                               async_rados,
                               info,
                               [=](SIProvider::Info *_info) {
                                 *_info = pvd->get_info();
                                 return 0;
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<SIProvider::fetch_result>(cct,
                               async_rados,
                               result,
                               [=](SIProvider::fetch_result *_result) {
                                 return pvd->fetch(sid, shard_id, marker, max, _result);
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<rgw_sip_pos>(cct,
                               async_rados,
                               pos,
                               [=](rgw_sip_pos *_pos) {
                                 return pvd->get_start_marker(sid, shard_id, &_pos->marker, &_pos->timestamp);
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos)
{
  auto pvd = provider; /* capture another reference */
  return new RGWSafeRetAsyncCR<rgw_sip_pos>(cct,
                               async_rados,
                               pos,
                               [=](rgw_sip_pos *_pos) {
                                 return pvd->get_cur_state(sid, shard_id, &_pos->marker, &_pos->timestamp);
                               });
}

RGWCoroutine *SIProviderCRMgr_Local::trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const string& marker)
{
  auto pvd = provider; /* capture another reference */
  return new RGWAsyncLambdaCR<void>(cct,
                               async_rados,
                               [=]() {
                                 return pvd->trim(sid, shard_id, marker);
                               });
}

struct SIProviderRESTCRs {
  class GetStagesInfoCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;

    string path;
    SIProvider::Info *info;
  public:
    GetStagesInfoCR(SIProviderCRMgr_REST *_mgr,
                    SIProvider::Info *_info) : RGWCoroutine(_mgr->ctx()),
                                               mgr(_mgr),
                                               info(_info) {
      path = mgr->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgr->instance ? "instance" : "");
          const char *instance_val = (mgr->instance ? mgr->instance->c_str() : "");
          rgw_http_param_pair pairs[] = { { "info", nullptr },
					  { "provider" , mgr->remote_provider_name.c_str() },
					  { instance_key , instance_val },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgr->ctx(),
                                         mgr->conn,
                                         mgr->http_manager,
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
    SIProviderCRMgr_REST *mgr;
    std::vector<SIProvider::stage_id_t> *result;

    SIProvider::Info info;
  public:
    GetStagesCR(SIProviderCRMgr_REST *_mgr,
                std::vector<SIProvider::stage_id_t> *_result) : RGWCoroutine(_mgr->ctx()),
                                                                mgr(_mgr),
                                                                result(_result) {
    }

    int operate() override {
      reenter(this) {
        yield call(new GetStagesInfoCR(mgr, &info));
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
    SIProviderCRMgr_REST *mgr;
    SIProvider::stage_id_t sid;

    SIProvider::Info info;
    SIProvider::StageInfo *sinfo;
  public:
    GetStageInfoCR(SIProviderCRMgr_REST *_mgr,
                   const SIProvider::stage_id_t& _sid,
                   SIProvider::StageInfo *_sinfo) : RGWCoroutine(_mgr->ctx()),
                                                    mgr(_mgr),
                                                    sid(_sid),
                                                    sinfo(_sinfo) {
    }

    int operate() override {
      reenter(this) {
        yield call(new GetStagesInfoCR(mgr, &info));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        for (auto& si : info.stages) {
          if (si.sid == sid) {
            *sinfo = si;
            return set_cr_done();
          }
        }

        ldout(mgr->ctx(), 10) << "GetStageInfoCR(): sid not found: provider=" << mgr->remote_provider_name << " sid=" << sid << dendl;

        return set_cr_error(-ENOENT);
      }

      return 0;
    }
  };

  class FetchCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;
    SIProvider::stage_id_t sid;
    int shard_id;
    string marker;
    int max;

    string  path;

    bufferlist bl;
    SIProvider::fetch_result *result;

  public:
    FetchCR(SIProviderCRMgr_REST *_mgr,
            const SIProvider::stage_id_t& _sid,
            int _shard_id,
            const string& _marker,
            int _max,
            SIProvider::fetch_result *_result) : RGWCoroutine(_mgr->ctx()),
                                                 mgr(_mgr),
                                                 sid(_sid),
                                                 shard_id(_shard_id),
                                                 marker(_marker),
                                                 max(_max),
                                                 result(_result) {
      path = mgr->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgr->instance ? "instance" : "");
          const char *instance_val = (mgr->instance ? mgr->instance->c_str() : "");
          char max_buf[16];
          snprintf(max_buf, sizeof(max_buf), "%d", max);
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          rgw_http_param_pair pairs[] = { { "provider" , mgr->remote_provider_name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
					  { "max" , max_buf },
					  { "marker" , marker.c_str() },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgr->ctx(),
                                         mgr->conn,
                                         mgr->http_manager,
                                         path,
                                         pairs,
                                         &bl));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }


        JSONParser p;
        if (!p.parse(bl.c_str(), bl.length())) {
          ldout(cct, 0) << "ERROR: failed to parse fetch result: bl=" << bl.to_str() << dendl;
          return set_cr_error(-EIO);
        }

        auto type_handler = mgr->type_provider->get_type_handler();
        if (!type_handler) {
          ldout(cct, 0) << "ERROR: " << __func__ << "(): get_type_provider for sid=" << sid << " is null, likely a bug" << dendl;
          return set_cr_error(-EIO);
        }

        int r = type_handler->decode_json_results(sid, &p, result);
        if (r < 0) {
          ldout(cct, 0) << "ERROR: failed to decode fetch result: bl=" << bl.to_str() << dendl;
          return set_cr_error(r);
        }

        return set_cr_done();
      }

      return 0;
    }
  };

  class GetStagesStatusCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;
    SIProvider::stage_id_t sid;
    int shard_id;

    rgw_sip_pos *start_pos;
    rgw_sip_pos *cur_pos;

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

      void decode_json(JSONObj *obj) {
        JSONDecoder::decode_json("markers", markers, obj);
      }
    } status;

  public:
    GetStagesStatusCR(SIProviderCRMgr_REST *_mgr,
                      const SIProvider::stage_id_t& _sid,
                      int _shard_id,
                      rgw_sip_pos *_start_pos,
                      rgw_sip_pos *_cur_pos) : RGWCoroutine(_mgr->ctx()),
                                              mgr(_mgr),
                                              sid(_sid),
                                              shard_id(_shard_id),
                                              start_pos(_start_pos),
                                              cur_pos(_cur_pos) {
      path = mgr->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgr->instance ? "instance" : "");
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          const char *instance_val = (mgr->instance ? mgr->instance->c_str() : "");
          rgw_http_param_pair pairs[] = { { "status", nullptr },
					  { "provider" , mgr->remote_provider_name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
	                                  { nullptr, nullptr } };
          call(new RGWReadRESTResourceCR(mgr->ctx(),
                                         mgr->conn,
                                         mgr->http_manager,
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

        return set_cr_done();
      }

      return 0;
    }
  };

  class TrimCR : public RGWCoroutine {
    SIProviderCRMgr_REST *mgr;
    SIProvider::stage_id_t sid;
    int shard_id;
    string marker;

    string path;

  public:
    TrimCR(SIProviderCRMgr_REST *_mgr,
           const SIProvider::stage_id_t& _sid,
           int _shard_id,
           const string& _marker) : RGWCoroutine(_mgr->ctx()),
                                              mgr(_mgr),
                                              sid(_sid),
                                              shard_id(_shard_id),
                                              marker(_marker) {
      path = mgr->path_prefix;
    }

    int operate() override {
      reenter(this) {
        yield {
          const char *instance_key = (mgr->instance ? "instance" : "");
          char shard_id_buf[16];
          snprintf(shard_id_buf, sizeof(shard_id_buf), "%d", shard_id);
          const char *instance_val = (mgr->instance ? mgr->instance->c_str() : "");
          rgw_http_param_pair pairs[] = { { "provider" , mgr->remote_provider_name.c_str() },
					  { instance_key , instance_val },
					  { "stage-id" , sid.c_str() },
					  { "shard-id" , shard_id_buf },
					  { "marker" , marker.c_str() },
	                                  { nullptr, nullptr } };
          call(new RGWDeleteRESTResourceCR(mgr->ctx(),
                                           mgr->conn,
                                           mgr->http_manager,
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
};

RGWCoroutine *SIProviderCRMgr_REST::get_stages_cr(std::vector<SIProvider::stage_id_t> *stages)
{
  return new SIProviderRESTCRs::GetStagesCR(this, stages);
}

RGWCoroutine *SIProviderCRMgr_REST::get_stage_info_cr(const SIProvider::stage_id_t& sid, SIProvider::StageInfo *sinfo)
{
  return new SIProviderRESTCRs::GetStageInfoCR(this, sid, sinfo);
}

RGWCoroutine *SIProviderCRMgr_REST::get_info_cr(SIProvider::Info *info)
{
  return new SIProviderRESTCRs::GetStagesInfoCR(this, info);
}

RGWCoroutine *SIProviderCRMgr_REST::fetch_cr(const SIProvider::stage_id_t& sid, int shard_id, std::string marker, int max, SIProvider::fetch_result *result)
{
  return new SIProviderRESTCRs::FetchCR(this, sid, shard_id,
                                        marker, max, result);
}

RGWCoroutine *SIProviderCRMgr_REST::get_start_marker_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos)
{
  return new SIProviderRESTCRs::GetStagesStatusCR(this, sid, shard_id,
                                                  pos, nullptr);
}

RGWCoroutine *SIProviderCRMgr_REST::get_cur_state_cr(const SIProvider::stage_id_t& sid, int shard_id, rgw_sip_pos *pos)
{
  return new SIProviderRESTCRs::GetStagesStatusCR(this, sid, shard_id,
                                                  nullptr, pos);
}

RGWCoroutine *SIProviderCRMgr_REST::trim_cr(const SIProvider::stage_id_t& sid, int shard_id, const string& marker)
{
  return new SIProviderRESTCRs::TrimCR(this, sid, shard_id, marker);
}

SIProvider::TypeHandler *SIProviderCRMgr_REST::get_type_handler()
{
  return type_provider->get_type_handler();
}

