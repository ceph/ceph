#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/nameser.h>
#include <resolv.h>

#include "acconfig.h"

#ifdef HAVE_ARPA_NAMESER_COMPAT_H
#include <arpa/nameser_compat.h>
#endif

#include "rgw_common.h"
#include "rgw_resolve.h"

#define dout_subsys ceph_subsys_rgw

class RGWDNSResolver {
  list<res_state> states;
  Mutex lock;

  int get_state(res_state *ps);
  void put_state(res_state s);


public:
  ~RGWDNSResolver();
  RGWDNSResolver() : lock("RGWDNSResolver") {}
  int resolve_cname(const string& hostname, string& cname, bool *found);
};

RGWDNSResolver::~RGWDNSResolver()
{
  list<res_state>::iterator iter;
  for (iter = states.begin(); iter != states.end(); ++iter) {
    struct __res_state *s = *iter;
    delete s;
  }
}


int RGWDNSResolver::get_state(res_state *ps)
{
  lock.Lock();
  if (!states.empty()) {
    res_state s = states.front();
    states.pop_front();
    lock.Unlock();
    *ps = s;
    return 0;
  }
  lock.Unlock();
  struct __res_state *s = new struct __res_state;
  s->options = 0;
  if (res_ninit(s) < 0) {
    delete s;
    dout(0) << "ERROR: failed to call res_ninit()" << dendl;
    return -EINVAL;
  }
  *ps = s;
  return 0;
}

void RGWDNSResolver::put_state(res_state s)
{
  Mutex::Locker l(lock);
  states.push_back(s);
}


int RGWDNSResolver::resolve_cname(const string& hostname, string& cname, bool *found)
{
  res_state res;

  *found = false;

  int r = get_state(&res);
  if (r < 0) {
    return r;
  }

  int ret;

#define LARGE_ENOUGH_DNS_BUFSIZE 1024
  unsigned char buf[LARGE_ENOUGH_DNS_BUFSIZE];

#define MAX_FQDN_SIZE 255
  char host[MAX_FQDN_SIZE + 1];
  const char *origname = hostname.c_str();
  unsigned char *pt, *answer;
  unsigned char *answend;
  int len = res_nquery(res, origname, C_IN, T_CNAME, buf, sizeof(buf));
  if (len < 0) {
    dout(20) << "res_query() failed" << dendl;
    ret = 0;
    goto done;
  }

  answer = buf;
  pt = answer + sizeof(HEADER);
  answend = answer + len;

  /* read query */
  if ((len = dn_expand(answer, answend, pt, host, sizeof(host))) < 0) {
    dout(0) << "ERROR: dn_expand() failed" << dendl;
    ret = -EINVAL;
    goto done;
  }
  pt += len;

  if (pt + 4 > answend) {
    dout(0) << "ERROR: bad reply" << dendl;
    ret = -EIO;
    goto done;
  }

  int type;
  GETSHORT(type, pt);

  if (type != T_CNAME) {
    dout(0) << "ERROR: failed response type: type=%d (was expecting " << T_CNAME << ")" << dendl;
    ret = -EIO;
    goto done;
  }

  pt += INT16SZ; /* class */

  /* read answer */

  if ((len = dn_expand(answer, answend, pt, host, sizeof(host))) < 0) {
    ret = 0;
    goto done;
  }
  pt += len;
  dout(20) << "name=" << host << dendl;

  if (pt + 10 > answend) {
    dout(0) << "ERROR: bad reply" << dendl;
    ret = -EIO;
    goto done;
  }

  GETSHORT(type, pt);
  pt += INT16SZ; /* class */
  pt += INT32SZ; /* ttl */
  pt += INT16SZ; /* size */

  if ((len = dn_expand(answer, answend, pt, host, sizeof(host))) < 0) {
    ret = 0;
    goto done;
  }
  dout(20) << "cname host=" << host << dendl;
  cname = host;

  *found = true;
  ret = 0;
done:
  put_state(res);
  return ret;
}

RGWResolver::~RGWResolver() {
  delete resolver;
}
RGWResolver::RGWResolver() {
  resolver = new RGWDNSResolver;
}

int RGWResolver::resolve_cname(const string& hostname, string& cname, bool *found) {
  return resolver->resolve_cname(hostname, cname, found);
};

RGWResolver *rgw_resolver;


void rgw_init_resolver()
{
  rgw_resolver = new RGWResolver();
}

void rgw_shutdown_resolver()
{
  delete rgw_resolver;
}
