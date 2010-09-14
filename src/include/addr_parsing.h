/*
 * addr_parsing.h
 *
 *  Created on: Sep 14, 2010
 *      Author: gregf
 *      contains functions used by Ceph to convert named addresses
 *      (eg ceph.newdream.net) into IP addresses (ie 127.0.0.1).
 */

#ifndef ADDR_PARSING_H_
#define ADDR_PARSING_H_
#define BUF_SIZE 128

static int safe_cat(char **pstr, int *plen, int pos, const char *str2)
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

char *mount_resolve_dest(char *orig_str)
{
        char *new_str;
        char *mount_path;
        char *tok, *p, *port_str;
        int len, pos;

        mount_path = strrchr(orig_str, ':');
        if (mount_path == orig_str) {
                printf("server address expected\n");
                return NULL;
        }

        if (mount_path) {
          *mount_path = '\0';
          mount_path++;
        }

        len = BUF_SIZE;
        new_str = (char *)malloc(len);

        p = new_str;
        pos = 0;

        tok = strtok(orig_str, ",");

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

                tok = strtok(NULL, ",");
                if (tok)
                        pos = safe_cat(&new_str, &len, pos, ",");

        }

        if (mount_path) {
          pos = safe_cat(&new_str, &len, pos, ":");
          pos = safe_cat(&new_str, &len, pos, mount_path);
        }

        //printf("new_str is '%s'\n", new_str);
        return new_str;
}


#endif /* ADDR_PARSING_H_ */
