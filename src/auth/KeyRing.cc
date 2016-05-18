// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <map>
#include <memory>
#include <sstream>
#include <algorithm>

#include "auth/AuthMethodList.h"
#include "auth/Crypto.h"
#include "auth/KeyRing.h"
#include "common/ConfUtils.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/str_list.h"
#include "common/Formatter.h"

#define dout_subsys ceph_subsys_auth

#undef dout_prefix
#define dout_prefix *_dout << "auth: "

using namespace std;

int KeyRing::from_ceph_context(CephContext *cct)
{
  const md_config_t *conf = cct->_conf;
  string filename;

  int ret = ceph_resolve_file_search(conf->keyring, filename);
  if (!ret) {
    ret = load(cct, filename);
    if (ret < 0)
      lderr(cct) << "failed to load " << filename
		 << ": " << cpp_strerror(ret) << dendl;
  } else {
    lderr(cct) << "unable to find a keyring on " << conf->keyring
	       << ": " << cpp_strerror(ret) << dendl;
  }

  if (!conf->key.empty()) {
    EntityAuth ea;
    try {
      ea.key.decode_base64(conf->key);
      add(conf->name, ea);
      return 0;
    }
    catch (buffer::error& e) {
      lderr(cct) << "failed to decode key '" << conf->key << "'" << dendl;
      return -EINVAL;
    }
  }

  if (!conf->keyfile.empty()) {
    bufferlist bl;
    string err;
    int r = bl.read_file(conf->keyfile.c_str(), &err);
    if (r < 0) {
      lderr(cct) << err << dendl;
      return r;
    }
    string k(bl.c_str(), bl.length());
    EntityAuth ea;
    try {
      ea.key.decode_base64(k);
      add(conf->name, ea);
    }
    catch (buffer::error& e) {
      lderr(cct) << "failed to decode key '" << k << "'" << dendl;
      return -EINVAL;
    }
    return 0;
  }

  return ret;
}

KeyRing *KeyRing::create_empty()
{
  return new KeyRing();
}

int KeyRing::set_modifier(const char *type, const char *val, EntityName& name, map<string, bufferlist>& caps)
{
  if (!val)
    return -EINVAL;

  if (strcmp(type, "key") == 0) {
    CryptoKey key;
    string l(val);
    try {
      key.decode_base64(l);
    } catch (const buffer::error& err) {
      return -EINVAL;
    }
    set_key(name, key);
  } else if (strncmp(type, "caps ", 5) == 0) {
    const char *caps_entity = type + 5;
    if (!*caps_entity)
      return -EINVAL;
    string l(val);
    bufferlist bl;
    ::encode(l, bl);
    caps[caps_entity] = bl;
    set_caps(name, caps);
  } else if (strcmp(type, "auid") == 0) {
    uint64_t auid = strtoull(val, NULL, 0);
    set_uid(name, auid);
  } else
    return -EINVAL;

  return 0;
}

void KeyRing::encode_plaintext(bufferlist& bl)
{
  std::ostringstream os;
  print(os);
  string str = os.str();
  bl.append(str);
}

void KeyRing::encode_formatted(string label, Formatter *f, bufferlist& bl)
{
  std::ostringstream(os);
  f->open_array_section(label.c_str());
  for (map<EntityName, EntityAuth>::iterator p = keys.begin();
       p != keys.end();
       ++p) {

    f->open_object_section("auth_entities");
    f->dump_string("entity", p->first.to_str().c_str());
    std::ostringstream keyss;
    keyss << p->second.key;
    f->dump_string("key", keyss.str());
    if (p->second.auid != CEPH_AUTH_UID_DEFAULT)
      f->dump_int("auid", p->second.auid);
    f->open_object_section("caps");
    for (map<string, bufferlist>::iterator q = p->second.caps.begin();
 	 q != p->second.caps.end();
	 ++q) {
      bufferlist::iterator dataiter = q->second.begin();
      string caps;
      ::decode(caps, dataiter);
      f->dump_string(q->first.c_str(), caps);
    }
    f->close_section();	/* caps */
    f->close_section();	/* auth_entities */
  }
  f->close_section();	/* auth_dump */
  f->flush(bl);
}

void KeyRing::decode_plaintext(bufferlist::iterator& bli)
{
  int ret;
  bufferlist bl;
  bli.copy_all(bl);
  ConfFile cf;
  std::deque<std::string> parse_errors;

  if (cf.parse_bufferlist(&bl, &parse_errors, NULL) != 0) {
    throw buffer::malformed_input("cannot parse buffer");
  }

  for (ConfFile::const_section_iter_t s = cf.sections_begin();
	    s != cf.sections_end(); ++s) {
    string name = s->first;
    if (name == "global")
      continue;

    EntityName ename;
    map<string, bufferlist> caps;
    if (!ename.from_str(name)) {
      ostringstream oss;
      oss << "bad entity name in keyring: " << name;
      throw buffer::malformed_input(oss.str().c_str());
    }

    for (ConfSection::const_line_iter_t l = s->second.lines.begin();
	 l != s->second.lines.end(); ++l) {
      if (l->key.empty())
        continue;
      string k(l->key);
      std::replace(k.begin(), k.end(), '_', ' ');
      ret = set_modifier(k.c_str(), l->val.c_str(), ename, caps);
      if (ret < 0) {
	ostringstream oss;
	oss << "error setting modifier for [" << name << "] type=" << k
	    << " val=" << l->val;
	throw buffer::malformed_input(oss.str().c_str());
      }
    }
  }
}

void KeyRing::decode(bufferlist::iterator& bl) {
  __u8 struct_v;
  bufferlist::iterator start_pos = bl;
  try {
    ::decode(struct_v, bl);
    ::decode(keys, bl);
  } catch (buffer::error& err) {
    keys.clear();
    decode_plaintext(start_pos);
  }
}

int KeyRing::load(CephContext *cct, const std::string &filename)
{
  if (filename.empty())
    return -EINVAL;

  bufferlist bl;
  std::string err;
  int ret = bl.read_file(filename.c_str(), &err);
  if (ret < 0) {
    lderr(cct) << "error reading file: " << filename << ": " << err << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    decode(iter);
  }
  catch (const buffer::error& err) {
    lderr(cct) << "error parsing file " << filename << dendl;
  }

  ldout(cct, 2) << "KeyRing::load: loaded key file " << filename << dendl;
  return 0;
}

void KeyRing::print(ostream& out)
{
  for (map<EntityName, EntityAuth>::iterator p = keys.begin();
       p != keys.end();
       ++p) {
    out << "[" << p->first << "]" << std::endl;
    out << "\tkey = " << p->second.key << std::endl;
    if (p->second.auid != CEPH_AUTH_UID_DEFAULT)
      out << "\tauid = " << p->second.auid << std::endl;

    for (map<string, bufferlist>::iterator q = p->second.caps.begin();
	 q != p->second.caps.end();
	 ++q) {
      bufferlist::iterator dataiter = q->second.begin();
      string caps;
      ::decode(caps, dataiter);
      out << "\tcaps " << q->first << " = \"" << caps << '"' << std::endl;
    }
  }
}

void KeyRing::import(CephContext *cct, KeyRing& other)
{
  for (map<EntityName, EntityAuth>::iterator p = other.keys.begin();
       p != other.keys.end();
       ++p) {
    ldout(cct, 10) << " importing " << p->first << dendl;
    ldout(cct, 30) << "    " << p->second << dendl;
    keys[p->first] = p->second;
  }
}


