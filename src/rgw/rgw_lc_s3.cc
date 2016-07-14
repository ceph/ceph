#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "rgw_user.h"
#include "rgw_lc_s3.h"


#define dout_subsys ceph_subsys_rgw

using namespace std;

bool LCExpiration_S3::xml_end(const char * el) {
  LCDays_S3 *lc_days = static_cast<LCDays_S3 *>(find_first("Days"));

  // ID is mandatory
  if (!lc_days)
    return false;
  days = lc_days->get_data();
  return true;
}

bool RGWLifecycleConfiguration_S3::xml_end(const char *el) {
  XMLObjIter iter = find("Rule");
  LCRule_S3 *rule = static_cast<LCRule_S3 *>(iter.get_next());
  while (rule) {
    add_rule(rule);
    rule = static_cast<LCRule_S3 *>(iter.get_next());
  }
  return true;
}

bool LCRule_S3::xml_end(const char *el) {
  LCID_S3 *lc_id;
  LCPrefix_S3 *lc_prefix;
  LCStatus_S3 *lc_status;
  LCExpiration_S3 *lc_expiration;

  id.clear();
  prefix.clear();
  status.clear();

  lc_id = static_cast<LCID_S3 *>(find_first("ID"));
  if (!lc_id)
    return false;
  id = lc_id->get_data();

  lc_prefix = static_cast<LCPrefix_S3 *>(find_first("Prefix"));
  if (!lc_prefix)
    return false;
  prefix = lc_prefix->get_data();

  lc_status = static_cast<LCStatus_S3 *>(find_first("Status"));
  if (!lc_status)
    return false;
  status = lc_status->get_data();

  lc_expiration = static_cast<LCExpiration_S3 *>(find_first("Expiration"));
  if (!lc_expiration)
    return false;
  expiration = *lc_expiration;

  return true;
}

void LCRule_S3::to_xml(CephContext *cct, ostream& out) {
  LCExpiration_S3& expir = static_cast<LCExpiration_S3&>(expiration);
  out << "<Rule>" ;
  out << "<ID>" << id << "</ID>";
  out << "<Prefix>" << prefix << "</Prefix>";
  out << "<Status>" << status << "</Status>";
  expir.to_xml(out);
  out << "</Rule>";
}

int RGWLifecycleConfiguration_S3::rebuild(RGWRados *store, RGWLifecycleConfiguration& dest)
{
  multimap<string, LCRule>::iterator iter;
  for (iter = rule_map.begin(); iter != rule_map.end(); ++iter) {
    LCRule& src_rule = iter->second;
    bool rule_ok = true;

    if (rule_ok) {
      dest.add_rule(&src_rule);
    }
  }

  return 0;
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

XMLObj *RGWLCXMLParser_S3::alloc_obj(const char *el)
{
  XMLObj * obj = NULL;
  if (strcmp(el, "LifecycleConfiguration") == 0) {
    obj = new RGWLifecycleConfiguration_S3(cct);
  } else if (strcmp(el, "Rule") == 0) {
    obj = new LCRule_S3();
  } else if (strcmp(el, "ID") == 0) {
    obj = new LCID_S3();
  } else if (strcmp(el, "Prefix") == 0) {
    obj = new LCPrefix_S3();
  } else if (strcmp(el, "Status") == 0) {
    obj = new LCStatus_S3();
  } else if (strcmp(el, "Expiration") == 0) {
    obj = new LCExpiration_S3();
  } else if (strcmp(el, "Days") == 0) {
    obj = new LCDays_S3();
  }
  return obj;
}
