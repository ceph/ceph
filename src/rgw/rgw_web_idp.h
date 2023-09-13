// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

namespace rgw {
namespace web_idp {

//WebToken contains some claims from the decoded token which are of interest to us.
struct WebTokenClaims {
  //Subject of the token
  std::string sub;
  //Intended audience for this token
  std::string aud;
  //Issuer of this token
  std::string iss;
  //Human-readable id for the resource owner
  std::string user_name;
  //Client Id
  std::string client_id;
  //azp
  std::string azp;
};

}; /* namespace web_idp */
}; /* namespace rgw */
