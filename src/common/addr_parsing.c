// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(__FreeBSD__) || defined(_AIX)
#include <sys/socket.h>
#include <netinet/in.h>
#endif
#include <netdb.h>

#define BUF_SIZE 128
#define ROUND_UP_128(x) (-(-(x) & -128))

int safe_cat(char **pstr, int *plen, int pos, const char *src)
{
  size_t len2 = strlen(src);
  size_t new_size = pos + len2 + 1;
  if (*plen < new_size) {
    size_t round_up = ROUND_UP_128(new_size);
    void* p = realloc(*pstr, round_up);
    if (!p) {
      printf("Out of memory\n");
      exit(1);
    } else {
      *pstr = p;
    }
  }
  memcpy(*pstr + pos, src, len2 + 1);
  return pos + len2;
}

char *resolve_addrs(const char *orig_str)
{
  int len = BUF_SIZE;
  char *new_str = (char *)malloc(len);

  if (!new_str) {
    return NULL;
  }

  char *saveptr = NULL;
  char *buf = strdup(orig_str);
  const char *delim = ",; ";

  char *tok = strtok_r(buf, delim, &saveptr);

  int pos = 0;

  while (tok) {
    struct addrinfo hint;
    struct addrinfo *res, *ores;
    char *firstcolon, *lastcolon, *bracecolon;
    int r;
    int brackets = 0;

    firstcolon = strchr(tok, ':');
    lastcolon = strrchr(tok, ':');
    bracecolon = strstr(tok, "]:");

    char *port_str = 0;
    if (firstcolon && firstcolon == lastcolon) {
      /* host:port or a.b.c.d:port */
      *firstcolon = 0;
      port_str = firstcolon + 1;
    } else if (bracecolon) {
      /* [ipv6addr]:port */
      port_str = bracecolon + 1;
      *port_str = 0;
      port_str++;
    }
    if (port_str && !*port_str)
      port_str = NULL;

    if (*tok == '[' &&
	tok[strlen(tok)-1] == ']') {
      tok[strlen(tok)-1] = 0;
      tok++;
      brackets = 1;
    }

    //printf("name '%s' port '%s'\n", tok, port_str);

    // FIPS zeroization audit 20191115: this memset is fine.
    memset(&hint, 0, sizeof(hint));
    hint.ai_family = AF_UNSPEC;
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_protocol = IPPROTO_TCP;

    r = getaddrinfo(tok, port_str, &hint, &res);
    if (r < 0) {
      printf("server name not found: %s (%s)\n", tok,
	     gai_strerror(r));
      free(new_str);
      free(buf);
      return 0;
    }

    /* build resolved addr list */
    ores = res;
    while (res) {
      char host[40], port[40];
      getnameinfo(res->ai_addr, res->ai_addrlen,
		  host, sizeof(host),
		  port, sizeof(port),
		  NI_NUMERICSERV | NI_NUMERICHOST);
      /*printf(" host %s port %s flags %d family %d socktype %d proto %d sanonname %s\n",
	host, port,
	res->ai_flags, res->ai_family, res->ai_socktype, res->ai_protocol,
	res->ai_canonname);*/
      if (res->ai_family == AF_INET6)
	brackets = 1;  /* always surround ipv6 addrs with brackets */
      if (brackets)
	pos = safe_cat(&new_str, &len, pos, "[");
      pos = safe_cat(&new_str, &len, pos, host);
      if (brackets)
	pos = safe_cat(&new_str, &len, pos, "]");
      if (port_str) {
	pos = safe_cat(&new_str, &len, pos, ":");
	pos = safe_cat(&new_str, &len, pos, port);
      }
      res = res->ai_next;
      if (res)
	pos = safe_cat(&new_str, &len, pos, ",");
    }
    freeaddrinfo(ores);

    tok = strtok_r(NULL, delim, &saveptr);
    if (tok)
      pos = safe_cat(&new_str, &len, pos, ",");

  }

  //printf("new_str is '%s'\n", new_str);
  free(buf);
  return new_str;
}
