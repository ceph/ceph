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

#include "config.h"
#include "include/str_list.h"

#include "Crypto.h"
#include "auth/KeyRing.h"

#define DOUT_SUBSYS auth
#undef dout_prefix
#define dout_prefix *_dout << "auth: "


using namespace std;

KeyRing g_keyring;

int KeyRing::parse_name(char *line, EntityName& name)
{
  string s(line);
  if (!name.from_str(s))
    return -EINVAL;

  return 0;
}

int KeyRing::parse_caps(char *line, map<string, bufferlist>& caps)
{
  char *name;

  if (*line != '[')
    return -EINVAL;

  line++;

  while (*line && isspace(*line))
    line++;
  name = line;

  while (*line && !isspace(*line) && (*line != ']'))
    line++;

  if (isspace(*line)) {
    *line = '\0';
    line++;
    while (isspace(*line))
      line++;
  }

  if (*line != ']')
    return -EINVAL;
  *line = '\0';

  string n(name);

  line++;
  while (isspace(*line))
    line++;

  string val(line);
  bufferlist bl;
  ::encode(val, bl);
  caps[name] = bl;
  return 0;
}

int KeyRing::parse_modifier(char *line, EntityName& name, map<string, bufferlist>& caps)
{
  char *type = strsep(&line, ":");
  if (!line)
    return -EINVAL;

  while (*line && isspace(*line))
    line++;

  if (strcmp(type, "key") == 0) {
    CryptoKey key;
    string l(line);
    try {
      key.decode_base64(l);
    } catch (const buffer::error& err) {
      return -EINVAL;
    }
    set_key(name, key);
  } else if (strcmp(type, "caps") == 0) {
    int ret = parse_caps(line, caps);
    if (ret < 0)
      return ret;
    set_caps(name, caps);
  } else if (strcmp(type, "auid") == 0) {
    uint64_t auid = strtoull(line, NULL, 0);
    set_uid(name, auid);
  } else
    return -EINVAL;

  return 0;
}


void KeyRing::decode_plaintext(bufferlist::iterator& bl)
{
  int ret = -EINVAL;

  bufferlist::iterator iter = bl;

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
  iter = bl;
  int i;
  for (i = 0; i < len; i++) {
    ::decode(c, iter);
    orig_src[i] = c;
  }

  char *src = orig_src;

  char *line;
  int line_count;

  EntityName name;
  bool has_name = false;
  map<string, bufferlist> caps;

  for (line_count = 1; src; line_count++) {
    line = strsep(&src, "\n\r");

    int alpha_index = 0;
    bool had_alpha = 0;

    for (i = 0; line[i]; i++) {
      switch (line[i]) {
      case '#':
        if (had_alpha)
          goto parse_err;
        continue;
      case ' ':
      case '\t':
        continue;
      case '.': // this is a name
        ret = parse_name(line + alpha_index, name);
        if (ret < 0)
          goto parse_err;
        has_name = true;
        caps.clear();
        break;
      case ':': // this is a modifier
        if (!has_name)
          goto parse_err;
        ret = parse_modifier(line + alpha_index, name, caps);
        if (ret < 0)
          goto parse_err;
        break;
      default:
        if (!had_alpha)
          alpha_index = i;
        had_alpha = 1;
        break;
      }
    }
  }

  if (!has_name)
    goto parse_err;

  delete[] orig_src;

  return;

parse_err:
  derr << "parse error at line " << line_count << ":" << dendl;
  derr << line << dendl;
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

int KeyRing::load(const char *filename)
{
  if (!filename)
    return -EINVAL;

  bufferlist bl;
  int ret = bl.read_file(filename, true);
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
    out << p->first << std::endl;
    out << "\t key: " << p->second.key << std::endl;
    out << "\tauid: " << p->second.auid << std::endl;

    for (map<string, bufferlist>::iterator q = p->second.caps.begin();
	 q != p->second.caps.end();
	 ++q) {
      bufferlist::iterator dataiter = q->second.begin();
      string caps;
      ::decode(caps, dataiter);
      out << "\tcaps: [" << q->first << "] " << caps << std::endl;
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


