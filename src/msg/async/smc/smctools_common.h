// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * smc-tools/smctools_common.h
 *
 * Copyright IBM Corp. 2017
 *
 * Author(s): Ursula Braun (ubraun@linux.ibm.com)
 * Author(s): Aliaksei Makarau <aliaksei.makarau@ibm.com>
 */
#pragma once

#include <net/if.h>
#include <linux/types.h>
#include <linux/inet_diag.h>

/***********************************************************
 * Mimic definitions in kernel/include/uapi/linux/smc.h
 ***********************************************************/
#define SMC_MAX_PNETID_LEN 16   /* Max. length of PNET id */
#define SMC_MAX_HOSTNAME_LEN 32 /* Max length of hostname */
#define SMC_MAX_EID_LEN 32      /* Max length of eid */
#define SMC_PCI_ID_STR_LEN 16   /* Max length of pci id string */
#define SMC_DIAG_MAX (__SMC_DIAG_MAX - 1)
#define SMC_DIAG_EXT_MAX (__SMC_DIAG_EXT_MAX - 1)

/* Netlink SMC_PNETID attributes */
enum {
  SMC_PNETID_UNSPEC,
  SMC_PNETID_NAME,
  SMC_PNETID_ETHNAME,
  SMC_PNETID_IBNAME,
  SMC_PNETID_IBPORT,
  __SMC_PNETID_MAX,
  SMC_PNETID_MAX = __SMC_PNETID_MAX - 1
};

/* SMC_GENL_FAMILY top level attributes */
enum {
  SMC_GEN_UNSPEC,
  SMC_GEN_SYS_INFO,    /* nest */
  SMC_GEN_LGR_SMCR,    /* nest */
  SMC_GEN_LINK_SMCR,   /* nest */
  SMC_GEN_LGR_SMCD,    /* nest */
  SMC_GEN_DEV_SMCD,    /* nest */
  SMC_GEN_DEV_SMCR,    /* nest */
  SMC_GEN_STATS,       /* nest */
  SMC_GEN_FBACK_STATS, /* nest */
  __SMC_GEN_MAX,
  SMC_GEN_MAX = __SMC_GEN_MAX - 1
};

/***********************************************************
 * Mimic definitions in kernel/include/uapi/linux/smc_diag.h
 ***********************************************************/
/* Sequence numbers */
enum {
  MAGIC_SEQ = 123456,
  MAGIC_SEQ_V2,
  MAGIC_SEQ_V2_ACK,
};

/* Request structure */
struct smc_diag_req {
  __u8 diag_family;
  __u8 pad[2];
  __u8 diag_ext; /* Query extended information */
  struct inet_diag_sockid id;
};

/* Base info structure. It contains socket identity (addrs/ports/cookie) based
 * on the internal clcsock, and more SMC-related socket data
 */
struct smc_diag_msg {
  __u8 diag_family;
  __u8 diag_state;
  __u8 diag_mode;
  __u8 diag_shutdown;
  struct inet_diag_sockid id;

  __u32 diag_uid;
  __u64 diag_inode;
};

/* Mode of a connection */
enum {
  SMC_DIAG_MODE_SMCR,
  SMC_DIAG_MODE_FALLBACK_TCP,
  SMC_DIAG_MODE_SMCD,
};

/* GET_SOCK_DIAG command extensions */
enum {
  SMC_DIAG_NONE,
  SMC_DIAG_CONNINFO,
  SMC_DIAG_LGRINFO,
  SMC_DIAG_SHUTDOWN,
  SMC_DIAG_DMBINFO,
  SMC_DIAG_FALLBACK,
  __SMC_DIAG_MAX,
};

struct smc_diag_cursor {
  __u16 reserved;
  __u16 wrap;
  __u32 count;
};

struct smc_diag_fallback {
  __u32 reason;
  __u32 peer_diagnosis;
};
