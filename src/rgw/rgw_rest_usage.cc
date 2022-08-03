// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_op.h"
#include "rgw_usage.h"
#include "rgw_rest_usage.h"
#include "rgw_sal.h"

#include "include/str_list.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;


static inline RGWObjCategory   rgw_obj_get_category_by_name(string name)
{
	std::string::iterator end_pos = std::remove(name.begin(), name.end(), ' ');
	name.erase(end_pos, name.end()); 
	 
     if(name.compare("rgw.none") == 0)
			return  RGWObjCategory::None;
     if(name.compare("rgw.main") == 0)
			return RGWObjCategory:: Main;
     if(name.compare("rgw.shadow") == 0)
			return RGWObjCategory:: Shadow;
     if(name.compare("rgw.multimeta") == 0)
			return RGWObjCategory:: MultiMeta;
     if(name.compare("InfrequentAccess") == 0)
			return RGWObjCategory:: InfrequentAccess;
	 if(name.compare(RGW_STORAGE_CLASS_STANDARD) == 0)
	 		return RGWObjCategory:: Main;
	return RGWObjCategory::None;
}


class RGWOp_StorageClass_Get : public RGWRESTOp {

public:

	uint64_t ScSize;
	RGWOp_StorageClass_Get() {}

	int check_caps(const RGWUserCaps& caps) override {
		return caps.check_cap("usage", RGW_CAP_READ);
	}
	void execute(optional_yield y) override;
	const char* name() const override { return "get_storageclass"; }
	void send_response() override;
};


void RGWOp_StorageClass_Get::execute(optional_yield y) { 
	string sc = s->info.args.get("HTTP_X_AMZ_STORAGE_CLASS");
	
	string bucket_name ;
	string  tenant_name ; 
	RGWBucketInfo bucket_info;
	map<RGWObjCategory, RGWStorageStats> stats;   
	map<string, bufferlist> attrs;
	
	
	//RESTArgs::get_string(s, "uid", uid_str, &uid_str);
	RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);

	real_time mtime;
	op_ret  = store->getRados()->get_bucket_info(store->svc(),    
	        tenant_name, bucket_name, bucket_info,
	        &mtime, null_yield, this, &attrs);
	if (op_ret < 0)
	{
	    cerr << "error getting Storage class   bucket info =" << bucket_name << " ret=" << op_ret << std::endl;
		return ;
	}
	rgw_bucket& bucket = bucket_info.bucket;

	string bucket_ver, master_ver;
	string max_marker;
	op_ret = store->getRados()->get_bucket_stats(this, bucket_info, RGW_NO_SHARD,    
	          &bucket_ver, &master_ver, stats,
	          &max_marker);
	if (op_ret < 0)
	{
		cerr << "error getting Storage class  stats bucket=" << bucket_name << " ret=" << op_ret << std::endl;
		return ;
	}
	
	if(sc.empty()){
		sc = RGW_STORAGE_CLASS_STANDARD;
	}


	RGWObjCategory  cate = rgw_obj_get_category_by_name (sc);
	if(cate == RGWObjCategory::None){
	    op_ret = -ENOENT;
	    cerr << "error unknow category" << std::endl;
	}
	ScSize = stats [cate].size;
	return ;
/*

Formatter *formatter = flusher.get_formatter();
flusher.start(0);
formatter->open_object_section("storage class");
formatter-> dump_unsigned (storage_class, ScSize);   //???
formatter->close_section();

flusher.flush();
		*/	 
}
	 

void RGWOp_StorageClass_Get::send_response()
{
    string ss = s->info.args.get("HTTP_X_AMZ_STORAGE_CLASS");
	const char *sc = ss.c_str(); 
	set_req_state_err(s, op_ret);
	dump_errno(s);

	if (op_ret < 0) {
		end_header(s);
		return;
	}

	s->formatter->open_object_section("cs size ");
	encode_json(sc, ScSize, s->formatter);
	s->formatter->close_section();
	end_header(s, NULL, "application/json", s->formatter->get_len());
	flusher.flush();
}






class RGWOp_Usage_Get : public RGWRESTOp {

public:
  RGWOp_Usage_Get() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("usage", RGW_CAP_READ);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "get_usage"; }
};

void RGWOp_Usage_Get::execute(optional_yield y) {
  map<std::string, bool> categories;

  string uid_str;
  string bucket_name;
  uint64_t start, end;
  bool show_entries;
  bool show_summary;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  std::unique_ptr<rgw::sal::User> user = store->get_user(rgw_user(uid_str));
  std::unique_ptr<rgw::sal::Bucket> bucket;

  if (!bucket_name.empty()) {
    store->get_bucket(nullptr, user.get(), std::string(), bucket_name, &bucket, null_yield);
  }

  RESTArgs::get_epoch(s, "start", 0, &start);
  RESTArgs::get_epoch(s, "end", (uint64_t)-1, &end);
  RESTArgs::get_bool(s, "show-entries", true, &show_entries);
  RESTArgs::get_bool(s, "show-summary", true, &show_summary);

  string cat_str;
  RESTArgs::get_string(s, "categories", cat_str, &cat_str);

  if (!cat_str.empty()) {
    list<string> cat_list;
    list<string>::iterator iter;
    get_str_list(cat_str, cat_list);
    for (iter = cat_list.begin(); iter != cat_list.end(); ++iter) {
      categories[*iter] = true;
    }
  }

  op_ret = RGWUsage::show(this, store, user.get(), bucket.get(), start, end, show_entries, show_summary, &categories, flusher);
}

class RGWOp_Usage_Delete : public RGWRESTOp {

public:
  RGWOp_Usage_Delete() {}

  int check_caps(const RGWUserCaps& caps) override {
    return caps.check_cap("usage", RGW_CAP_WRITE);
  }
  void execute(optional_yield y) override;

  const char* name() const override { return "trim_usage"; }
};

void RGWOp_Usage_Delete::execute(optional_yield y) {
  string uid_str;
  string bucket_name;
  uint64_t start, end;

  RESTArgs::get_string(s, "uid", uid_str, &uid_str);
  RESTArgs::get_string(s, "bucket", bucket_name, &bucket_name);
  std::unique_ptr<rgw::sal::User> user = store->get_user(rgw_user(uid_str));
  std::unique_ptr<rgw::sal::Bucket> bucket;

  if (!bucket_name.empty()) {
    store->get_bucket(nullptr, user.get(), std::string(), bucket_name, &bucket, null_yield);
  }

  RESTArgs::get_epoch(s, "start", 0, &start);
  RESTArgs::get_epoch(s, "end", (uint64_t)-1, &end);

  if (rgw::sal::User::empty(user.get()) &&
      !bucket_name.empty() &&
      !start &&
      end == (uint64_t)-1) {
    bool remove_all;
    RESTArgs::get_bool(s, "remove-all", false, &remove_all);
    if (!remove_all) {
      op_ret = -EINVAL;
      return;
    }
  }

  op_ret = RGWUsage::trim(this, store, user.get(), bucket.get(), start, end);
}

RGWOp *RGWHandler_Usage::op_get()
{
  if(is_storageclass_op())
  {
    return new RGWOp_StorageClass_Get;
  }

  return new RGWOp_Usage_Get;
}

RGWOp *RGWHandler_Usage::op_delete()
{
  return new RGWOp_Usage_Delete;
}


