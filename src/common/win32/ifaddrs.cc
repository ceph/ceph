#include <errno.h>
#include <winsock2.h>
#include <wincrypt.h>
#include <iphlpapi.h>
#include <ws2tcpip.h>
#include <ifaddrs.h>
#include <stdio.h>

#include "include/compat.h"

int getifaddrs(struct ifaddrs **ifap)
{
  int ret = 0;

  DWORD size, res = 0;
  res = GetAdaptersAddresses(
    AF_UNSPEC, GAA_FLAG_INCLUDE_PREFIX,
    NULL, NULL, &size);
  if (res != ERROR_BUFFER_OVERFLOW) {
    errno = ENOMEM;
    return -1;
  }

  PIP_ADAPTER_ADDRESSES adapter_addrs = (PIP_ADAPTER_ADDRESSES)malloc(size);
  res = GetAdaptersAddresses(
    AF_UNSPEC, GAA_FLAG_INCLUDE_PREFIX,
    NULL, adapter_addrs, &size);
  if (res != ERROR_SUCCESS) {
    errno = ENOMEM;
    return -1;
  }

  struct ifaddrs *out_list_head = NULL;
  struct ifaddrs *out_list_curr;

  for (PIP_ADAPTER_ADDRESSES curr_addrs = adapter_addrs;
       curr_addrs != NULL;
       curr_addrs = curr_addrs->Next) {
    if (curr_addrs->OperStatus != 1)
      continue;

    for (PIP_ADAPTER_UNICAST_ADDRESS unicast_addrs = curr_addrs->FirstUnicastAddress;
         unicast_addrs != NULL;
         unicast_addrs = unicast_addrs->Next) {
      SOCKADDR* unicast_sockaddr = unicast_addrs->Address.lpSockaddr;
      if (unicast_sockaddr->sa_family != AF_INET &&
          unicast_sockaddr->sa_family != AF_INET6)
        continue;
      out_list_curr = (struct ifaddrs*)calloc(sizeof(*out_list_curr), 1);
      if (!out_list_curr) {
        errno = ENOMEM;
        ret = -1;
        goto out;
      }

      out_list_curr->ifa_next = out_list_head;
      out_list_head = out_list_curr;

      out_list_curr->ifa_flags = IFF_UP;
      if (curr_addrs->IfType == IF_TYPE_SOFTWARE_LOOPBACK)
        out_list_curr->ifa_flags |= IFF_LOOPBACK;

      out_list_curr->ifa_addr = (struct sockaddr *) &out_list_curr->in_addrs;
      out_list_curr->ifa_netmask = (struct sockaddr *) &out_list_curr->in_netmasks;
      out_list_curr->ifa_name = out_list_curr->ad_name;

      if (unicast_sockaddr->sa_family == AF_INET) {
        ULONG subnet_mask = 0;
        if (ConvertLengthToIpv4Mask(unicast_addrs->OnLinkPrefixLength, &subnet_mask)) {
          errno = ENODATA;
          ret = -1;
          goto out;
        }
        struct sockaddr_in *addr4 = (struct sockaddr_in *) &out_list_curr->in_addrs;
        struct sockaddr_in *netmask4 = (struct sockaddr_in *) &out_list_curr->in_netmasks;
        netmask4->sin_family = unicast_sockaddr->sa_family;
        addr4->sin_family = unicast_sockaddr->sa_family;
        netmask4->sin_addr.S_un.S_addr = subnet_mask;
        addr4->sin_addr = ((struct sockaddr_in*) unicast_sockaddr)->sin_addr;
      } else {
        struct sockaddr_in6 *addr6 = (struct sockaddr_in6 *) &out_list_curr->in_addrs;
        (*addr6) = *(struct sockaddr_in6 *) unicast_sockaddr;
      }
      out_list_curr->speed = curr_addrs->TransmitLinkSpeed;
      // TODO maybe use friendly name instead of adapter GUID
      sprintf_s(out_list_curr->ad_name,
                sizeof(out_list_curr->ad_name),
                curr_addrs->AdapterName);
    }
  }
  ret = 0;
out:
  free(adapter_addrs);
  if (ret && out_list_head)
    free(out_list_head);
  else if (ifap)
    *ifap = out_list_head;

  return ret;
}

void freeifaddrs(struct ifaddrs *ifa)
{
  while (ifa) {
    struct ifaddrs *next = ifa->ifa_next;
    free(ifa);
    ifa = next;
  }
}
