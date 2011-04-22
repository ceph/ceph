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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>

#define BUF_SIZE 128

int safe_cat(char **pstr, int *plen, int pos, const char *str2)
{
  int len2 = strlen(str2);

  while (*plen < pos + len2 + 1) {
    *plen += BUF_SIZE;
    *pstr = (char *)realloc(*pstr, (size_t)*plen);

    if (!*pstr) {
      printf("Out of memory\n");
      exit(1);
    }
  }

  strcpy((*pstr)+pos, str2);

  return pos + len2;
}

char *resolve_addrs(const char *orig_str)
{
  char *new_str;
  char *tok, *p, *port_str, *saveptr;
  int len, pos;
  char buf[strlen(orig_str) + 1];
  strcpy(buf, orig_str);

  len = BUF_SIZE;
  new_str = (char *)malloc(len);

  p = new_str;
  pos = 0;

  tok = strtok_r(buf, ",", &saveptr);

  while (tok) {
    struct addrinfo hint;
    struct addrinfo *res, *ores;
    char *firstcolon, *lastcolon, *bracecolon;
    int r;
    int brackets = 0;

    firstcolon = strchr(tok, ':');
    lastcolon = strrchr(tok, ':');
    bracecolon = strstr(tok, "]:");

    port_str = 0;
    if (firstcolon && firstcolon == lastcolon) {
      /* host:port or a.b.c.d:port */
      *firstcolon = 0;
      port_str = firstcolon + 1;
    } else if (bracecolon) {
      /* {ipv6addr}:port */
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

    memset(&hint, 0, sizeof(hint));
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_protocol = IPPROTO_TCP;

    r = getaddrinfo(tok, port_str, &hint, &res);
    if (r < 0) {
      printf("server name not found: %s (%s)\n", tok, strerror(errno));
      free(new_str);
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

    tok = strtok_r(NULL, ",", &saveptr);
    if (tok)
      pos = safe_cat(&new_str, &len, pos, ",");

  }

  //printf("new_str is '%s'\n", new_str);
  return new_str;
}
