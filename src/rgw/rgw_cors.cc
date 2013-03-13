// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
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
}

void RGWCORSRule::erase_origin_if_present(string& origin, bool *rule_empty){
  set<string>::iterator it = allowed_origins.find(origin);
  if(!rule_empty)
    return;
  *rule_empty = false;
  if(it != allowed_origins.end()){
    dout(10) << "Found origin " << origin << ", set size:" << 
        allowed_origins.size() << dendl;
    allowed_origins.erase(it);
    *rule_empty = (allowed_origins.size() == 0);
  }
}

bool RGWCORSRule::is_origin_present(list<string>& origins){
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
      if(s.length() > 0)
        s.append(",");
      s.append((*it));
  }
}

void RGWCORSConfiguration::get_origins_list(const char *origin, list<string>& origins){
  const char *start, *t;
  string proto, host_name, sorigin = origin;
  parse_host_name(sorigin, host_name, proto);
  start = t = host_name.c_str();
  while(t){
    if(t == start)
      origins.push_back(proto+host_name);
    else{
      t++;
      origins.push_back(proto + string("*.") + string(t));
    }
    t = strchr(t, '.');
  }
}

RGWCORSRule * RGWCORSConfiguration::host_name_rule(const char *origin){
  list<string> origins;
  get_origins_list(origin, origins);
  for(list<RGWCORSRule>::iterator it_r = rules.begin(); 
      it_r != rules.end(); it_r++){
    RGWCORSRule& r = (*it_r);
    if(r.is_origin_present(origins))
      return &r;
  }
  return NULL;
}

void RGWCORSConfiguration::erase_host_name_rule(string& origin){
  bool rule_empty;
  unsigned loop = 0;
  /*Erase the host name from that rule*/
  dout(10) << "Num of rules : " << rules.size() << dendl;
  for(list<RGWCORSRule>::iterator it_r = rules.begin(); 
      it_r != rules.end(); it_r++, loop++){
    RGWCORSRule& r = (*it_r);
    r.erase_origin_if_present(origin, &rule_empty);
    dout(10) << "Origin:" << origin << ", rule num:" 
      << loop << ", emptying now:" << rule_empty << dendl;
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


void parse_host_name(string& in, string& host_name, string& proto){
  string _in = in;
  /*check for protocol name first*/
  proto = ""; /*For everything else except https*/
  if(_in.compare(0, 8, "https://") == 0)
    proto = "https://";
  unsigned off = _in.find("://");
  if(off != string::npos)
    _in.assign(_in, off+3, string::npos);
  
  /*remove www. prefix*/
  off = _in.find("www."); 
  if(off != string::npos){
    if(off == 0){
      /*there could be a hostname http://www.org or http://www.* */
      if((off + 3) != _in.find_last_of(".") ||
         ((_in.length() > 4) && (_in[4] == '*'))){
        _in.assign(_in, 4, string::npos);
      }
    }
  }
  host_name = _in;
}
