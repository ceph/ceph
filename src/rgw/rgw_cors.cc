
#include <string.h>

#include <iostream>
#include <map>

#include "include/types.h"

#include "common/Formatter.h"

#include "rgw_cors.h"

#define dout_subsys ceph_subsys_rgw
using namespace std;

void RGWCORSRule::dump_origins(){
  unsigned num_origins = allowed_origins.size();
  dout(10) << "Allowed origins : " << num_origins << dendl;
  for(set<string>::iterator it = allowed_origins.begin();
      it != allowed_origins.end(); 
      it++){
    dout(10) << *it << "," << dendl;
  }
  dout(10) << "\n" << dendl;
}

void RGWCORSRule::erase_origin_if_present(string origin, bool *rule_empty){
  set<string>::iterator it = allowed_origins.find(origin);
  *rule_empty = false;
  if(it != allowed_origins.end()){
    dout(10) << "Found origin " << origin << ", set size:" << 
        allowed_origins.size() << dendl;
    allowed_origins.erase(it);
    if(allowed_origins.size() <= 0)*rule_empty = true;
  }
}

bool RGWCORSRule::is_origin_present(list<string> origins){
  if(allowed_origins.find("*") != allowed_origins.end()) 
    return true;
  for(list<string>::iterator it_o = origins.begin();
      it_o != origins.end(); it_o++){
    dout(10) << "** checking if " << *it_o << " is present" << dendl;
    if(allowed_origins.find(*it_o) != allowed_origins.end()){
      return true;
    }
  }
  return false;
}
void RGWCORSRule::format_exp_headers(string& s){
  s = "";
  for(list<string>::iterator it = exposable_hdrs.begin();
      it != exposable_hdrs.end(); it++){
      if(s.length() > 0)s.append(",");
      s.append((*it));
  }
}

void RGWCORSConfiguration::get_origins_list(const char *origin, list<string>& origins){
  const char *start = origin, *t;
  /*Get just the host name*/
  /*It could be http://www.example.com or www.example.com or http://example.com*/
  start = strstr(origin, "www.");
  if(start) start += 4;
  else{
    t = origin;
    start = strstr(origin, "://");
    if(start) start += 3;
    else start = t;
  }
  t = start;
  while(t){
    if(t == start)origins.push_back(t);
    else{
      t++;
      origins.push_back(string("*.") + string(t));
    }
    t = strchr(t, '.');
  }
}

RGWCORSRule * RGWCORSConfiguration::host_name_rule(const char *origin){
  list<string> origins;
  get_origins_list(origin, origins);
  for(list<RGWCORSRule>::iterator it_r = rules.begin(); 
      it_r != rules.end(); it_r++){
    RGWCORSRule& r = static_cast<RGWCORSRule &>(*it_r);
    if(r.is_origin_present(origins))return &r;
  }
  return NULL;
}

void RGWCORSConfiguration::erase_host_name_rule(string origin){
  bool rule_empty;
  unsigned loop = 0;
  /*Erase the host name from that rule*/
  dout(10) << "Num of rules : " << rules.size() << dendl;
  for(list<RGWCORSRule>::iterator it_r = rules.begin(); 
      it_r != rules.end(); it_r++, loop++){
    RGWCORSRule& r = static_cast<RGWCORSRule&>(*it_r);
    r.erase_origin_if_present(origin, &rule_empty);
    dout(10) << "Origin:" << origin << ", rule num:" << loop << ", emptying now:" << rule_empty << dendl;
    if(rule_empty){
      rules.erase(it_r);
      break;
    }
  }
}

void RGWCORSConfiguration::dump(){
  unsigned loop = 1;
  unsigned num_rules = rules.size();
  dout(10) << "Number of rules: " << num_rules << dendl;
  for(list<RGWCORSRule>::iterator it = rules.begin();
      it!= rules.end(); it++, loop++){
    dout(10) << " <<<<<<< Rule " << loop << " >>>>>>> " << dendl;
    (*it).dump_origins();
  }
}
