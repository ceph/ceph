#include "rgw_policy.h"

#define dout_subsys ceph_subsys_rgw

void ManagedPolicyAttachment::generate_test_instances(std::list<ManagedPolicyAttachment*>& o)
{
  
  ManagedPolicyAttachment* u = new ManagedPolicyAttachment;
  u->arn = "arn:aws:iam::123456789012:policy/TestPolicy1";
  u->status = "PENDING";
  o.push_back(u);

  ManagedPolicyAttachment* v = new ManagedPolicyAttachment;
  v->arn = "arn:aws:iam::123456789012:policy/TestPolicy2";
  v->status = "ATTACHED";
  o.push_back(v);
}

void ManagedPolicyAttachment::dump(Formatter *f) const
{
  encode_json("arn", arn, f);
  encode_json("status", status, f);
}

void ManagedPolicyAttachment::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("status", status, obj);
}

void ManagedPolicyInfo::dump(Formatter * const f) const
{
  encode_json("id", id, f);
  encode_json("policy_name", name, f);
  encode_json("path", path, f);
  encode_json("creation_time", creation_date, f);
  encode_json("update_time", update_date, f);
  encode_json("policy_document", policy_document, f);
  encode_json("description", description, f);
  encode_json("default_version", default_version, f);
  encode_json("account_id", account_id, f);
  encode_json("tags", tags, f);
  encode_json_map("attachments", "key", "val", attachments, f);
  encode_json("versions", versions, f);
  encode_json("arn", arn, f);
  encode_json("attachment_count", attachment_count, f);
  encode_json("permissions_boundary_usage_count", attachment_count, f);
  encode_json("is_attachable", is_attachable, f);
}

static void decode_attachments(std::map<std::string, ManagedPolicyAttachment>& attachments, JSONObj* o)
{
  ManagedPolicyAttachment mpa;
  mpa.decode_json(o);
    if (!mpa.arn.empty()) {
    attachments[mpa.arn] = mpa;
  }
}

void ManagedPolicyInfo::decode_json(JSONObj * obj)
{
  JSONDecoder::decode_json("id", id, obj);
  JSONDecoder::decode_json("policy_name", name, obj);
  JSONDecoder::decode_json("path", path, obj);
  JSONDecoder::decode_json("creation_time", creation_date, obj);
  JSONDecoder::decode_json("update_time", update_date, obj);
  JSONDecoder::decode_json("policy_document", policy_document, obj);
  JSONDecoder::decode_json("description", description, obj);
  JSONDecoder::decode_json("default_version", default_version, obj);
  JSONDecoder::decode_json("account_id", account_id, obj);
  JSONDecoder::decode_json("tags", tags, obj);
  JSONDecoder::decode_json("attachments", attachments, decode_attachments, obj);
  JSONDecoder::decode_json("versions", versions, obj);
  JSONDecoder::decode_json("arn", arn, obj);
  JSONDecoder::decode_json("attachment_count", attachment_count, obj);
  JSONDecoder::decode_json("permissions_boundary_usage_count", attachment_count, obj);
  JSONDecoder::decode_json("is_attachable", is_attachable, obj);
}

void ManagedPolicyInfo::generate_test_instances(std::list<ManagedPolicyInfo*>& o)
{
  o.push_back(new ManagedPolicyInfo);
  auto p = new ManagedPolicyInfo;
  p->id = "id";
  p->name = "policy_name";
  p->path = "/path/";
  p->account_id = "account";
  o.push_back(p);
}
