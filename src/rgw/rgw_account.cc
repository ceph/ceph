#include "rgw_account.h"
#include "rgw_metadata.h"

#include "services/svc_account.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be.h"

RGWAccountCtl::RGWAccountCtl(RGWSI_Zone *zone_svc,
                             RGWSI_Account *account_svc,
                             RGWAccountMetadataHandler *_am_handler) : am_handler(_am_handler)
{
  svc.zone = zone_svc;
  svc.account = account_svc;
  be_handler = am_handler->get_be_handler();
}

RGWAccountMetadataHandler::RGWAccountMetadataHandler(RGWSI_Account *account_svc) {
  base_init(account_svc->ctx(), account_svc->get_be_handler());
  svc.account = account_svc;
}

int RGWAccountCtl::store_info(const DoutPrefixProvider* dpp,
                              const RGWAccountInfo& info,
                              RGWObjVersionTracker *objv_tracker,
                              const real_time& mtime,
                              bool exclusive,
                              std::map<std::string, bufferlist> *pattrs,
                              optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
   return svc.account->store_account_info(dpp, op->ctx(),
                                          info,
                                          objv_tracker,
                                          mtime,
                                          exclusive,
                                          pattrs,
                                          y);
                          });
}

void AccountQuota::dump(Formatter * const f) const
{
  f->open_object_section("AccountQuota");
  f->dump_unsigned("max_users", max_users);
  f->dump_unsigned("max_roles", max_roles);
  f->close_section();
}

void RGWAccountInfo::dump(Formatter * const f) const
{
  f->open_object_section("RGWAccountInfo");
  encode_json("id", id, f);
  encode_json("tenant", tenant, f);
  account_quota.dump(f);
  f->close_section();
}

void RGWAccountInfo::generate_test_instances(std::list<RGWAccountInfo*>& o)
{
  o.push_back(new RGWAccountInfo("tenant1","account1"));
}

int RGWAccountCtl::read_info(const DoutPrefixProvider* dpp,
                             const std::string& account_id,
                             RGWAccountInfo* info,
                             RGWObjVersionTracker * const objv_tracker,
                             real_time * const pmtime,
                             std::map<std::string, bufferlist> * pattrs,
                             optional_yield y)
{
  return be_handler->call([&](RGWSI_MetaBackend_Handler::Op *op) {
        return svc.account->read_account_info(dpp, op->ctx(), account_id,
                                              info, objv_tracker, pmtime,
                                              pattrs, y);

      }
    );
}
