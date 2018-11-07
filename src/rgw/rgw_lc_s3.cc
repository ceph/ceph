#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_user.h"
#include "rgw_lc_s3.h"


#define dout_subsys ceph_subsys_rgw

static bool check_date(const string& _date)
{
  boost::optional<ceph::real_time> date = ceph::from_iso_8601(_date);
  if (boost::none == date) {
    return false;
  }
  struct timespec time = ceph::real_clock::to_timespec(*date);
  if (time.tv_sec % (24*60*60) || time.tv_nsec) {
    return false;
  }
  return true;
}

void LCExpiration_S3::decode_xml(XMLObj *obj) {
  bool days_exist = RGWXMLDecoder::decode_xml("Days", days, obj);
  bool dm_exist = RGWXMLDecoder::decode_xml("ExpiredObjectDeleteMarker", dm_expiration, obj);
  bool date_exist = RGWXMLDecoder::decode_xml("Date", date, obj);
  if ((!days_exist && !dm_exist && !date_exist) || (days_exist && dm_exist)
      || (days_exist && date_exist) || (dm_exist && date_exist)) {
    throw RGWXMLDecoder::err("invalid expiration action");
  }
  if (dm_exist && !dm_expiration) {
    throw RGWXMLDecoder::err("invalid delete marker expiration value");
  }
  if (date_exist && !check_date(date)) {
    throw RGWXMLDecoder::err("invalid date format");
  }

}

void LCNoncurExpiration_S3::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("NoncurrentDays", days, obj, true);
}

void LCMPExpiration_S3::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("DaysAfterInitiation", days, obj, true);
}

void RGWLifecycleConfiguration_S3::decode_xml(XMLObj *obj) {
  list<LCRule_S3> rules;
  do_decode_xml_obj(rules, "Rule", obj);
  if (rules.empty()) {
    throw RGWXMLDecoder::err("lifecycle config has no rules");
  }
  if (cct->_conf->rgw_lc_max_rules < rules.size()) {
    throw RGWXMLDecoder::err("the lifecycle config has too many rules");
  }

  // S3 generates a 48 bit random ID, maybe we could generate shorter IDs
  static constexpr auto LC_ID_LENGTH = 48;
  for (LCRule_S3& rule: rules) {
    string id;
    rule.get_id(id);
    if (id.empty()) {
      gen_rand_alphanumeric_lower(cct, &id, LC_ID_LENGTH);
      rule.set_id(&id);
    }
    add_rule(&rule);
  }
}

void LCFilter_S3::decode_xml(XMLObj *obj) {
  XMLObj *o = obj->find_first("And");
  bool single_cond = false;
  int num_conditions = 0;
  // If there is an AND condition, every tag is a child of and
  // else we only support single conditions and return false if we see multiple

  if (o == nullptr){
    o = obj;
    single_cond = true;
  }

  RGWXMLDecoder::decode_xml("Prefix", prefix, o);
  if (!prefix.empty())
    num_conditions++;
  auto tags_iter = o->find("Tag");
  obj_tags.clear();
  while (auto tag_xml =tags_iter.get_next()){
    std::string _key,_val;
    RGWXMLDecoder::decode_xml("Key", _key, tag_xml);
    RGWXMLDecoder::decode_xml("Value", _val, tag_xml);
    obj_tags.emplace_tag(std::move(_key), std::move(_val));
    num_conditions++;
  }
  if (single_cond && num_conditions > 1) {
    throw RGWXMLDecoder::err("invalid filter");
  }

}

void LCTransition_S3::decode_xml(XMLObj *obj) {
  bool days_exist = RGWXMLDecoder::decode_xml("Days", days, obj);
  bool date_exist = RGWXMLDecoder::decode_xml("Date", date, obj);
  if (days_exist && date_exist) {
    throw RGWXMLDecoder::err("transition action can't have both days and date");
  } else if (!days_exist && !date_exist) {
    throw RGWXMLDecoder::err("transition action must have days or date");
  }
  if (date_exist && !check_date(date)) {
    throw RGWXMLDecoder::err("invalid date format");
  }
  RGWXMLDecoder::decode_xml("StorageClass", storage_class, obj, true);
  if (storage_class.compare("STANDARD_IA") != 0 && storage_class.compare("ONEZONE_IA") != 0 &&
      storage_class.compare("GLACIER") != 0) {
    throw RGWXMLDecoder::err("invalid storage class");
  }
}

void LCNoncurTransition_S3::decode_xml(XMLObj *obj) {
  RGWXMLDecoder::decode_xml("NoncurrentDays", days, obj, true);
  RGWXMLDecoder::decode_xml("StorageClass", storage_class, obj, true);
  if (storage_class.compare("STANDARD_IA") != 0 && storage_class.compare("ONEZONE_IA") != 0 &&
      storage_class.compare("GLACIER") != 0) {
    throw RGWXMLDecoder::err("invalid storage class");
  }
}

void LCRule_S3::decode_xml(XMLObj *obj) {
  LCExpiration_S3 lc_expiration;
  LCNoncurExpiration_S3 lc_noncur_expiration;
  LCMPExpiration_S3 lc_mp_expiration;
  LCFilter_S3 lc_filter;
  list<LCTransition_S3> lc_transitions;
  list<LCNoncurTransition_S3> lc_noncur_transitions;
  id.clear();
  prefix.clear();
  status.clear();
  dm_expiration = false;

  RGWXMLDecoder::decode_xml("ID", id, obj);

  bool filter_exist = RGWXMLDecoder::decode_xml("Filter", lc_filter, obj);

  if (filter_exist){
    filter = lc_filter;
  } else {
    // Ideally the following code should be deprecated and we should return
    // False here, The new S3 LC configuration xml spec. makes Filter mandatory
    // and Prefix optional. However older clients including boto2 still generate
    // xml according to the older spec, where Prefix existed outside of Filter
    // and S3 itself seems to be sloppy on enforcing the mandatory Filter
    // argument. A day will come when S3 enforces their own xml-spec, but it is
    // not this day

    RGWXMLDecoder::decode_xml("Prefix", prefix, obj, true);
  }

  RGWXMLDecoder::decode_xml("Status", status, obj, true);
  if (status.compare("Enabled") != 0 && status.compare("Disabled") != 0)
    throw RGWXMLDecoder::err("invalid status value");

  bool cur_exist = RGWXMLDecoder::decode_xml("Expiration", lc_expiration, obj);
  bool noncur_exist = RGWXMLDecoder::decode_xml("NoncurrentVersionExpiration", lc_noncur_expiration, obj);
  bool mp_exist = RGWXMLDecoder::decode_xml("AbortIncompleteMultipartUpload", lc_mp_expiration, obj);
  do_decode_xml_obj(lc_transitions, "Transition", obj);
  do_decode_xml_obj(lc_noncur_transitions, "NoncurrentVersionTransition", obj);

  if (!cur_exist && !noncur_exist && !mp_exist &&
      lc_transitions.empty() && lc_noncur_transitions.empty()) {
    throw RGWXMLDecoder::err("rule has no actions");
  } else {
    if (cur_exist) {
      if (lc_expiration.has_days()) {
        expiration.set_days(lc_expiration.get_days_str());
      } else if (lc_expiration.has_date()) {
        expiration.set_date(lc_expiration.get_date());
      } else {
        dm_expiration = lc_expiration.get_dm_expiration();
      }
    }
    if (noncur_exist) {
      noncur_expiration = lc_noncur_expiration;
    }
    if (mp_exist) {
      mp_expiration = lc_mp_expiration;
    }
    for (LCTransition_S3& lc_transition: lc_transitions) {
      if (!add_transition(lc_transition)) {
        throw RGWXMLDecoder::err("same storage class in transition action");
      }
    }
    for (LCNoncurTransition_S3& lc_noncur_transition: lc_noncur_transitions) {
      if (!add_noncur_transition(lc_noncur_transition)) {
        throw RGWXMLDecoder::err("same storage class in noncurrent transition action");
      }
    }
  }
}

void LCRule_S3::to_xml(ostream& out) {
  out << "<Rule>" ;
  out << "<ID>" << id << "</ID>";
  if (!filter.empty()) {
    LCFilter_S3& lc_filter = static_cast<LCFilter_S3&>(filter);
    lc_filter.to_xml(out);
  } else {
    out << "<Prefix>" << prefix << "</Prefix>";
  }
  out << "<Status>" << status << "</Status>";
  if (!expiration.empty() || dm_expiration) {
    LCExpiration_S3 expir(expiration.get_days_str(), expiration.get_date(), dm_expiration);
    expir.to_xml(out);
  }
  if (!noncur_expiration.empty()) {
    LCNoncurExpiration_S3& noncur_expir = static_cast<LCNoncurExpiration_S3&>(noncur_expiration);
    noncur_expir.to_xml(out);
  }
  if (!mp_expiration.empty()) {
    LCMPExpiration_S3& mp_expir = static_cast<LCMPExpiration_S3&>(mp_expiration);
    mp_expir.to_xml(out);
  }
  if (!transitions.empty()) {
    for (auto &elem : transitions) {
      LCTransition_S3& tran = static_cast<LCTransition_S3&>(elem.second);
      tran.to_xml(out);
    }
  }
  if (!noncur_transitions.empty()) {
    for (auto &elem : noncur_transitions) {
      LCNoncurTransition_S3& noncur_tran = static_cast<LCNoncurTransition_S3&>(elem.second);
      noncur_tran.to_xml(out);
    }
  }
  out << "</Rule>";
}

int RGWLifecycleConfiguration_S3::rebuild(RGWRados *store, RGWLifecycleConfiguration& dest)
{
  int ret = 0;
  multimap<string, LCRule>::iterator iter;
  for (iter = rule_map.begin(); iter != rule_map.end(); ++iter) {
    LCRule& src_rule = iter->second;
    ret = dest.check_and_add_rule(&src_rule);
    if (ret < 0)
      return ret;
  }
  if (!dest.valid()) {
    ret = -ERR_INVALID_REQUEST;
  }
  return ret;
}



void RGWLifecycleConfiguration_S3::dump_xml(Formatter *f) const
{
	f->open_object_section_in_ns("LifecycleConfiguration", XMLNS_AWS_S3);

    for (auto iter = rule_map.begin(); iter != rule_map.end(); ++iter) {
		const LCRule_S3& rule = static_cast<const LCRule_S3&>(iter->second);
		rule.dump_xml(f);
	}

	f->close_section(); // Lifecycle
}


