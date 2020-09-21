/*
 * Copyright 2013, 2015 Cloudbase Solutions Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#ifndef SYSLOG_H
#define SYSLOG_H 1

#define LOG_EMERG       0       /* system is unusable */
#define LOG_ALERT       1       /* action must be taken immediately */
#define LOG_CRIT        2       /* critical conditions */
#define LOG_ERR         3       /* error conditions */
#define LOG_WARNING     4       /* warning conditions */
#define LOG_NOTICE      5       /* normal but significant condition */
#define LOG_INFO        6       /* informational */
#define LOG_DEBUG       7       /* debug-level messages */
#define LOG_NDELAY      8       /* don't delay open */

#define LOG_KERN      (0<<3)  /* kernel messages */
#define LOG_USER      (1<<3)  /* user-level messages */
#define LOG_MAIL      (2<<3)  /* mail system */
#define LOG_DAEMON    (3<<3)  /* system daemons */
#define LOG_AUTH      (4<<3)  /* security/authorization messages */
#define LOG_SYSLOG    (5<<3)  /* messages generated internally by syslogd */
#define LOG_LPR       (6<<3)  /* line printer subsystem */
#define LOG_NEWS      (7<<3)  /* network news subsystem */
#define LOG_UUCP      (8<<3)  /* UUCP subsystem */
#define LOG_CRON      (9<<3)  /* clock daemon */
#define LOG_AUTHPRIV  (10<<3) /* security/authorization messages */
#define LOG_FTP       (11<<3) /* FTP daemon */

#define LOG_LOCAL0      (16<<3) /* reserved for local use */
#define LOG_LOCAL1      (17<<3) /* reserved for local use */
#define LOG_LOCAL2      (18<<3) /* reserved for local use */
#define LOG_LOCAL3      (19<<3) /* reserved for local use */
#define LOG_LOCAL4      (20<<3) /* reserved for local use */
#define LOG_LOCAL5      (21<<3) /* reserved for local use */
#define LOG_LOCAL6      (22<<3) /* reserved for local use */
#define LOG_LOCAL7      (23<<3) /* reserved for local use */

static inline void
openlog(const char *ident, int option, int facility)
{
}

static inline void
syslog(int priority, const char *format, ...)
{
}

#endif /* syslog.h */
