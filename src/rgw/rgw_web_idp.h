#ifndef CEPH_RGW_WEB_IDP_H
#define CEPH_RGW_WEB_IDP_H

#include <utility>
#include <boost/optional.hpp>
#include <boost/utility/string_view.hpp>

#include "rgw_auth.h"
#include "rgw_common.h"

namespace rgw {
namespace web_idp {

//WebToken contains some claims from the decoded token which are of interest to us.
struct WebTokenClaims {
    //Subject of the token
    string sub;
    //Intended audience for this token
    string aud;
    //Issuer of this token
    string iss;
    //Human-readable id for the resource owner
    string user_name;
};

}; /* namespace web_idp */
}; /* namespace rgw */

#endif /* CEPH_RGW_WEB_IDP_H */
