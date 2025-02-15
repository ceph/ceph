// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <stdint.h>
#include <boost/algorithm/string.hpp>
#include <sstream>

#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "rgw/rgw_b64.h"

namespace rgw {

  using std::string;

  class RGWToken {
  public:
    static constexpr auto type_name = "RGW_TOKEN";

    enum token_type : uint32_t {
      TOKEN_NONE,
	TOKEN_AD,
	TOKEN_KEYSTONE,
	TOKEN_LDAP,
    };

    static enum token_type to_type(const string& s) {
      if (boost::iequals(s, "ad"))
	return TOKEN_AD;
      if (boost::iequals(s, "ldap"))
	return TOKEN_LDAP;
      if (boost::iequals(s, "keystone"))
	return TOKEN_KEYSTONE;
      return TOKEN_NONE;
    }

    static const char* from_type(enum token_type type) {
      switch (type) {
      case TOKEN_AD:
	return "ad";
      case TOKEN_LDAP:
	return "ldap";
      case TOKEN_KEYSTONE:
	return "keystone";
      default:
	return "none";
      };
    }

    token_type type;
    string id;
    string key;

    virtual uint32_t version() const { return 1; };

    bool valid() const{
      return ((type != TOKEN_NONE) &&
	      (! id.empty()) &&
	      (! key.empty()));
    }

    RGWToken()
      : type(TOKEN_NONE) {};

    RGWToken(enum token_type _type, const std::string& _id,
	     const std::string& _key)
      : type(_type), id(_id), key(_key) {};

    explicit RGWToken(const string& json) {
      JSONParser p;
      p.parse(json.c_str(), json.length());
      JSONDecoder::decode_json(RGWToken::type_name, *this, &p);
    }

    RGWToken& operator=(const std::string& json) {
      JSONParser p;
      p.parse(json.c_str(), json.length());
      JSONDecoder::decode_json(RGWToken::type_name, *this, &p);
      return *this;
    }

    void encode(bufferlist& bl) const {
      uint32_t ver = version();
      string typestr{from_type(type)};
      ENCODE_START(1, 1, bl);
      encode(type_name, bl);
      encode(ver, bl);
      encode(typestr, bl);
      encode(id, bl);
      encode(key, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::const_iterator& bl) {
      string name;
      string typestr;
      uint32_t version;
      DECODE_START(1, bl);
      decode(name, bl);
      decode(version, bl);
      decode(typestr, bl);
      type = to_type(typestr);
      decode(id, bl);
      decode(key, bl);
      DECODE_FINISH(bl);
    }

    void dump(Formatter* f) const {
      ::encode_json("version", uint32_t(version()), f);
      ::encode_json("type", from_type(type), f);
      ::encode_json("id", id, f);
      ::encode_json("key", key, f);
    }

    void encode_json(Formatter* f) {
      RGWToken& token = *this;
      f->open_object_section(type_name);
      ::encode_json(type_name, token, f);
      f->close_section();
    }

    void decode_json(JSONObj* obj) {
      uint32_t version;
      string type_name;
      string typestr;
      JSONDecoder::decode_json("version", version, obj);
      JSONDecoder::decode_json("type", typestr, obj);
      type = to_type(typestr);
      JSONDecoder::decode_json("id", id, obj);
      JSONDecoder::decode_json("key", key, obj);
    }

    std::string encode_json_base64(Formatter* f) {
      encode_json(f);
      std::ostringstream os;
      f->flush(os);
      return to_base64(std::move(os.str()));
    }

    friend inline std::ostream& operator<<(std::ostream& os, const RGWToken& token);

    virtual ~RGWToken() {};
  };
  WRITE_CLASS_ENCODER(RGWToken)

  inline std::ostream& operator<<(std::ostream& os, const RGWToken& token)
  {
    os << "<<RGWToken"
       << " type=" << RGWToken::from_type(token.type)
       << " id=" << token.id
       << " key=" << token.key
       << ">>";
    return os;
  }

} /* namespace rgw */
