#pragma once

#include <map>
#include <string>

#include "include/encoding.h"
#include "common/async/yield_context.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "rgw_common.h"
#include "rgw_iam_managed_policy.h"
#include "rgw_arn.h"

struct ManagedPolicyInfo;

  using VersionId = std::string;
  using PolicyDocument = std::string;
  struct ManagedPolicyAttachment
  {
    std::string policy_name;
    std::string track; // TODO: PENDING vs COMMITTED
  };

  struct ManagedPolicyInfo
  {
    std::string id;
    std::string policy_name;
    std::string path;
    std::string arn;
    std::string creation_date;
    PolicyDocument policy_document;
    std::string tenant;
    std::string description;
    std::multimap<std::string, std::string> tags;
    RGWObjVersionTracker objv_tracker;
    ceph::real_time mtime;
    rgw_account_id account_id;
    VersionId default_version;

    ManagedPolicyInfo() = default;

    ~ManagedPolicyInfo() = default;

    std::map<rgw::ARN, ManagedPolicyAttachment> attachments;
    std::map<VersionId, PolicyDocument> versions;
    //TODO impl. encode and decode

    void encode(bufferlist &bl) const{
      ENCODE_START(4, 1, bl);
      encode(id, bl);
      encode(policy_name, bl);
      encode(path, bl);
      encode(arn, bl);
      encode(creation_date, bl);
      encode(policy_document, bl);
      encode(tenant, bl);
      encode(description, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator &bl)
    {
      DECODE_START(4, bl);
      decode(id, bl);
      decode(policy_name, bl);
      decode(path, bl);
      decode(arn, bl);
      decode(creation_date, bl);
      decode(policy_document, bl);
      decode(tenant, bl);
      decode(description, bl);
      
      DECODE_FINISH(bl);
    }
  };
  WRITE_CLASS_ENCODER(ManagedPolicyInfo)

  namespace rgw::sal
  {
    class RGWCustomerManagedPolicy
    {
    public:
      static const std::string policy_arn_prefix;
      static constexpr int MAX_POLICY_NAME_LEN = 128;
      static constexpr int MAX_PATH_NAME_LEN = 512;

    protected:
      ManagedPolicyInfo info;

    public:
      bool validate_input(const DoutPrefixProvider *dpp);

      RGWCustomerManagedPolicy(std::string name,
                               std::string tenant,
                               rgw_account_id account_id,
                               std::string path = "",
                               std::string policy_document = "",
                               std::string description = "",
                               std::string default_version = "v1",
                               std::multimap<std::string, std::string> tags = {});

      explicit RGWCustomerManagedPolicy(std::string id);

      explicit RGWCustomerManagedPolicy(const ManagedPolicyInfo &info) : info(info) {}

      RGWCustomerManagedPolicy() = default;

      virtual ~RGWCustomerManagedPolicy() = default;

      // virtual interface
      virtual int load_by_name(const DoutPrefixProvider *dpp, optional_yield y) = 0;
      virtual int load_by_id(const DoutPrefixProvider *dpp, optional_yield y) = 0;
      virtual int store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y, const RGWAccountInfo &acc_info, std::map<std::string, bufferlist> &acc_attrs,
                   RGWObjVersionTracker &objv) = 0;
      virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) = 0;

      // gsetter && setter
      const std::string &get_id() const { return info.id; }
      const std::string &get_name() const { return info.policy_name; }
      const std::string &get_tenant() const { return info.tenant; }
      const rgw_account_id &get_account_id() const { return info.account_id; }
      const std::string &get_path() const { return info.path; }
      const std::string &get_create_date() const { return info.creation_date; }
      const std::string &get_policy_document() const { return info.policy_document; }
      RGWObjVersionTracker &get_objv_tracker() { return info.objv_tracker; }
      const RGWObjVersionTracker &get_objv_tracker() const { return info.objv_tracker; }
      const real_time &get_mtime() const { return info.mtime; }
      ManagedPolicyInfo &get_info() { return info; }

      void set_id(const std::string &id) { this->info.id = id; }
      void set_mtime(const real_time &mtime) { this->info.mtime = mtime; }
      int set_tags(const DoutPrefixProvider *dpp, const std::multimap<std::string, std::string> &tags_map);
      boost::optional<std::multimap<std::string, std::string>> get_tags();
      void erase_tags(const std::vector<std::string> &tagKeys);
      void update_policy_document(std::string &policy_document);
      ManagedPolicyAttachment get_attachment(const rgw::ARN &arn) const;
      void set_attachment(const rgw::ARN &arn, const ManagedPolicyAttachment &attachment);
      int delete_policy(const DoutPrefixProvider *dpp, const rgw::ARN &arn);
      PolicyDocument getVersion(const VersionId &versionId) const;
      void setVersion(const VersionId &versionId, const PolicyDocument &policy_document);
      int create(const DoutPrefixProvider *dpp, std::string &policy_id, optional_yield y, 
                  const RGWAccountInfo &acc_info, std::map<std::string, bufferlist> &acc_attrs,
                   RGWObjVersionTracker &objv);
    };
  
}
