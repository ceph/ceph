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

#include "common/config.h"
#include "common/debug.h"
#include "include/str_list.h"
#include "common/ConfUtils.h"

#include "Crypto.h"
#include "auth/KeyRing.h"

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << "auth: "


using namespace std;

KeyRing g_keyring;

static string remove_quotes(string& s)
{
  int n = s.size();
  /* remove quotes from string */
  if (s[0] == '"' && s[n - 1] == '"')
    return s.substr(1, n-2);
  return s;
}

int KeyRing::set_modifier(const char *type, const char *val, EntityName& name, map<string, bufferlist>& caps)
{
  if (!val)
    return -EINVAL;

  if (strcmp(type, "key") == 0) {
    CryptoKey key;
    string l(val);
    l = remove_quotes(l);
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
      l = remove_quotes(l);
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


void KeyRing::decode_plaintext(bufferlist::iterator& bli)
{
  int ret;

  bufferlist::iterator iter = bli;

  // find out the size of the buffer
  char c;
  int len = 0;
  try {
    do {
      ::decode(c, iter);
      len++;
    } while (c);
  } catch (buffer::error& err) {
  }

  char *orig_src = new char[len + 1];
  orig_src[len] = '\0';
  iter = bli;
  int i;
  for (i = 0; i < len; i++) {
    ::decode(c, iter);
    orig_src[i] = c;
  }

  bufferlist bl;
  bl.append(orig_src, len);
  ConfFile cf;
  std::deque<std::string> parse_errors;
  if (cf.parse_bufferlist(&bl, &parse_errors) != 0) {
    derr << "cannot parse buffer" << dendl;
    goto done_err;
  }

  for (std::list<ConfSection*>::const_iterator p =
	    cf.get_section_list().begin();
       p != cf.get_section_list().end(); ++p) {
    string name = (*p)->get_name();
    if (name == "global")
      continue;

    EntityName ename;
    map<string, bufferlist> caps;
    if (!ename.from_str(name)) {
      derr << "bad entity name: " << name << dendl;
      goto done_err;
    }

    ConfList& cl = (*p)->get_list();
    ConfList::iterator cli;
    for (cli = cl.begin(); cli != cl.end(); ++ cli) {
      ConfLine *line = *cli;
      const char *type = line->get_var();
      if (!type)
        continue;

      ret = set_modifier(type, line->get_val(), ename, caps);
      if (ret < 0) {
        derr << "error setting modifier for [" << name << "] type=" << type << " val=" << line->get_val() << dendl;
        goto done_err;
      }
    }
  }

  delete[] orig_src;
  return;

done_err:
  delete[] orig_src;
  throw buffer::error();
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

int KeyRing::load(const std::string &filename)
{
  if (filename.empty())
    return -EINVAL;

  bufferlist bl;
  int ret = bl.read_file(filename.c_str(), true);
  if (ret < 0) {
    derr << "error reading file: " << filename << dendl;
    return ret;
  }

  try {
    bufferlist::iterator iter = bl.begin();
    decode(iter);
  }
  catch (const buffer::error& err) {
    derr << "error parsing file " << filename << dendl;
  }

  dout(2) << "KeyRing::load: loaded key file " << filename << dendl;
  return 0;
}

void KeyRing::print(ostream& out)
{
  for (map<EntityName, EntityAuth>::iterator p = keys.begin();
       p != keys.end();
       ++p) {
    out << "[" << p->first << "]" << std::endl;
    out << "\tkey = " << p->second.key << std::endl;
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

void KeyRing::import(KeyRing& other)
{
  for (map<EntityName, EntityAuth>::iterator p = other.keys.begin();
       p != other.keys.end();
       ++p) {
    dout(10) << " importing " << p->first << " " << p->second << dendl;
    keys[p->first] = p->second;
  }
}


