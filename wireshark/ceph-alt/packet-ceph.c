/* packet-ceph.c
 * Routines for Ceph Protocols
 * http://www.ceph.com
 *
 * Copyright 2013, Kevin Jones <k.j.jonez@gmail.com>
 *
 * This file contains parts of the original dissector code found in
 * the CEPH source tree. The author of that code was not marked.
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 * 
 * $Id$
 */

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include "../../epan/packet.h"
#include "../../epan/tvbuff.h"
#include "../../epan/tvbuff-int.h"
#include "../../epan/prefs.h"
#include "../../epan/range.h"
#include "../../epan/report_err.h"
#include "../../epan/conversation.h"
#include "../../epan/expert.h"
#include "../../epan/dissectors/packet-tcp.h" 

/* Protocol name */
#define PROTO_TAG_CEPH    "CEPHalt"

/****************************************************************************
 * Global handles
 ****************************************************************************/

/* Wireshark ID of the CEPH protocol */
static int g_proto_ceph = -1;

/* The registered dissector handle */
static dissector_handle_t g_ceph_handle;

/* 
 * The following hf_* variables are used to hold the Wireshark IDs of
 * our header fields; they are filled out when we call
 * proto_register_field_array() in proto_register_ceph()
 */
static gint hf_header = -1;
static gint hf_banner = -1;
static gint hf_entity_addr = -1;
static gint hf_entity_type = -1;
static gint hf_entity_num = -1;
static gint hf_banner_magic = -1;
static gint hf_banner_version = -1;
static gint hf_entity_erank = -1;
static gint hf_entity_nonce = -1;
static gint hf_sockaddr_in = -1;
static gint hf_sin_family = -1;
static gint hf_sin_port = -1;
static gint hf_sin_addr = -1;
static gint hf_hdr_tag = -1;
static gint hf_hdr_seq_ack = -1;
static gint hf_hdr_seq = -1;
static gint hf_hdr_tid = -1;
static gint hf_hdr_type = -1;
static gint hf_hdr_priority = -1;
static gint hf_hdr_version = -1;
static gint hf_hdr_mon_protocol = -1;
static gint hf_hdr_osd_protocol = -1;
static gint hf_hdr_mds_protocol = -1;
static gint hf_hdr_client_protocol = -1;
static gint hf_hdr_front_len = -1;
static gint hf_hdr_middle_len = -1;
static gint hf_hdr_data_off = -1;
static gint hf_hdr_data_len = -1;
static gint hf_hdr_src = -1;
static gint hf_hdr_crc = -1;
static gint hf_footer = -1;
static gint hf_footer_front_crc = -1;
static gint hf_footer_middle_crc = -1;
static gint hf_footer_data_crc = -1;
static gint hf_footer_sig = -1;
static gint hf_footer_flags = -1;
static gint hf_paxos_version = -1;
static gint hf_featureset_mask = -1;
static gint hf_featureset_id = -1;
static gint hf_featureset_name = -1;
static gint hf_compatset_compat = -1;
static gint hf_compatset_rocompat = -1;
static gint hf_compatset_incompat = -1;

static gint hf_connect_features = -1;
static gint hf_connect_host_type = -1;
static gint hf_connect_tag = -1;
static gint hf_connect_global_seq = -1;
static gint hf_connect_connect_seq = -1;
static gint hf_connect_protocol_version = -1;
static gint hf_connect_authorizer_protocol = -1;
static gint hf_connect_authorizer_len = -1;
static gint hf_connect_flags = -1;
static gint hf_connect_authentication_key = -1;
static gint hf_monmap = -1;
static gint hf_monmap_version = -1;
static gint hf_monmap_compat = -1;
static gint hf_monmap_epoch = -1;
static gint hf_monmap_name = -1;
static gint hf_monmap_lastchanged = -1;
static gint hf_monmap_created = -1;
static gint hf_monsubscribe = -1;
static gint hf_monsubscribe_name = -1;
static gint hf_monsubscribe_start = -1;
static gint hf_monsubscribe_flags = -1;
static gint hf_monsubscribeack = -1;
static gint hf_monsubscribeack_interval = -1;
static gint hf_mdsbeacon = -1;
static gint hf_mdsbeacon_globalid = -1;
static gint hf_mdsbeacon_state = -1;
static gint hf_mdsbeacon_seq = -1;
static gint hf_mdsbeacon_name = -1;
static gint hf_mdsbeacon_standbyforrank = -1;
static gint hf_mdsbeacon_standbyforname = -1;
static gint hf_auth = -1;
static gint hf_auth_protocol = -1;
static gint hf_auth_authlen = -1;
static gint hf_auth_authbytes = -1;
static gint hf_auth_monmapepoch = -1;
static gint hf_authreply = -1;
static gint hf_authreply_protocol = -1;
static gint hf_authreply_result = -1;
static gint hf_authreply_globalid = -1;
static gint hf_authreply_authlen = -1;
static gint hf_authreply_authbytes = -1;
static gint hf_authreply_msglen = -1;
static gint hf_authreply_msgstring = -1;

/* These are the ids of the subtrees that we may be creating */
static gint ett_ceph = -1;
static gint ett_header = -1;
static gint ett_banner = -1;
static gint ett_entity_addr = -1;
static gint ett_front = -1;
static gint ett_footer = -1;
static gint ett_sockaddr_in = -1;
static gint ett_entity_name = -1;
static gint ett_compat = -1;
static gint ett_rocompat = -1;
static gint ett_incompat = -1;

/****************************************************************************
 * Conversation helper - to find exact matches only
 ****************************************************************************/
static conversation_t* 
conv_find_exact(guint32 frame, const address* addr1, guint32 port1,
                        const address* addr2, guint32 port2) {
    conversation_t* conv;
    conv = find_conversation(frame, addr1, addr2,PT_TCP, port1, port2, 0);
    if (conv != NULL) {
        if (ADDRESSES_EQUAL(&conv->key_ptr->addr1, addr1) &&
            ADDRESSES_EQUAL(&conv->key_ptr->addr2, addr2) &&
            conv->key_ptr->port1 == port1 &&
            conv->key_ptr->port2 == port2)
        {
            return conv;
        }
        if (ADDRESSES_EQUAL(&conv->key_ptr->addr1, addr2) &&
            ADDRESSES_EQUAL(&conv->key_ptr->addr2, addr1) &&
            conv->key_ptr->port1 == port2 &&
            conv->key_ptr->port2 == port1)
        {
            return conv;
        }
    }
    return NULL;
}

/****************************************************************************
 * Entities (aka ceph service endpoints)
 * These should match the CEPH_ENTITY_ macros to make conversion easy from
 * on the wire formats.
 ****************************************************************************/
typedef enum _entity_type {
    ENTITY_TYPE_UNKNOWN = 0x00,
    ENTITY_TYPE_MON = 0x01,
    ENTITY_TYPE_MDS = 0x02,
    ENTITY_TYPE_OSD = 0x04,
    ENTITY_TYPE_CLIENT = 0x08,
    ENTITY_TYPE_AUTH = 0x20
} entity_type;

/*
 * This is what we record (within conv state) about each known endpoint.
 * Beware that conv_data maintains a pointer to these records so that should 
 * not be reallocated, see get_entity() for obtaining them. They are shared
 * this way so conversations may pass info to each other about endpoints.
 */
typedef struct _entity_data {
    entity_type type;
    char* name;
    address addr;
    guint16 port;
} entity_data;

/*
 * Map a entity type to a display string
 */
static const char* 
entityTypeDescription(entity_type type) {
    switch (type) {
        case ENTITY_TYPE_UNKNOWN:
            return "?";
        case ENTITY_TYPE_MON:
            return "mon";
        case ENTITY_TYPE_MDS:
            return "mds";
        case ENTITY_TYPE_OSD:
            return "osd";
        case ENTITY_TYPE_CLIENT:
            return "client";
        case ENTITY_TYPE_AUTH:
            return "auth";
    }
    assert(FALSE);
    return NULL;
}

/*
 * Test for a valid entity type
 */
static gboolean 
isEntityType(guint32 type) {
    switch (type) {
        case ENTITY_TYPE_MON:
        case ENTITY_TYPE_MDS:
        case ENTITY_TYPE_OSD:
        case ENTITY_TYPE_CLIENT:
        case ENTITY_TYPE_AUTH:
            return TRUE;
    }
    return FALSE;
}

/*
 * Find or create a new entity data entry from an address/port pair.
 */
static entity_data*
conv_get_entity(const address* addr, guint32 port) {
    conversation_t *conv=NULL;
    entity_data *edata; 

    /* 
     * Have we seen this conversation before? We record this as a conversation
     * to the same address/port. This is just to help keep the data together.
     */
    conv = conv_find_exact(-1, addr, port, addr, port);
    if (conv == NULL) {
        edata = (entity_data*)se_alloc(sizeof(entity_data));
        edata->type = ENTITY_TYPE_UNKNOWN;
        edata->name = NULL;
        SE_COPY_ADDRESS(&edata->addr,addr);
        edata->port = port;
        conv = conversation_new(0, addr, addr, PT_TCP, port, port, 0);
        conversation_add_proto_data(conv, g_proto_ceph, edata);
    } else {
        edata=(entity_data*)conversation_get_proto_data(conv, g_proto_ceph);
    }
    return edata;
}

/****************************************************************************
 * Data for tracking messages
 * This is mainly to track the startup handling which was confusing in the 
 * old dissector because the connect_reply would appear before its matching
 * request. Here we track in finer granularity and allow for some 
 * interweaving of messages. Of course the code also needs to handle traces 
 * which have conversations already running.
 ****************************************************************************/
enum msg_type {
    SERVER_BANNER  =0x1,    /* Server send banner to client */
    CLIENT_BANNER  =0x2,    /* Client responds to SERVER_BANNER */
    SERVER_ADDRESS =0x4,    /* Server sends its & client addresses to client */
    CLIENT_ADDRESS =0x8,    /* Client sends it own address to server */
    CONNECT_REQUEST=0x10,    /* Client send connection request to server */
    CONNECT_REPLY  =0x20,    /* Server responds to connect request */
    CONVERSING     =0x40    /* All other messages */
};
#define MSG_ANY        0xFF    /* Accept any type of message */

/*
 * Per message data, we record these during conversation startup 
 */
typedef struct _pdu_data {
    guint32 position;        /* The stream position msg seen at */
    guint32 toserver;        /* To/from the server */
    enum msg_type type;     /* The type of this message */
    guint32 seen;           /* Cumulative flags of previous messages */
} pdu_data;

/*
 * Per conversation data, there should be one of these for every detected
 * conversation between ceph entities. The to/from direction is arbitrary,
 * we probably don't know who the entities are when we establish the 
 * conversation.
 */
typedef struct _conv_data {
    emem_tree_t *pdu_table;    /* Message tree, stores accumulated pdu_data */
    entity_data* to;        /* Who is this to */
    entity_data* from;        /* Who is this from */
    guint direction_set;    /* Do we know to/from set correctly */
    char* displayT2F;        /* Cached 'to' to 'from' display string */
    char* displayF2T;        /* Cached 'from' to 'to' display string */
} conv_data;

/*
 * Find/create a conversation for the given source & destination
 */
static conv_data*
conv_get(packet_info *pinfo) {
    conversation_t* conv;
    conv_data* cdata;

    /* Have we seen this conversation before? */
    conv = conv_find_exact(pinfo->fd->num, &pinfo->src, pinfo->srcport, 
                &pinfo->dst, pinfo->destport);
    if (conv == NULL) {
        conv = conversation_new(pinfo->fd->num, &pinfo->src,
                    &pinfo->dst, pinfo->ptype,
                    pinfo->srcport, pinfo->destport, 0);
        conv = conv_find_exact(pinfo->fd->num, &pinfo->src, pinfo->srcport, 
                    &pinfo->dst, pinfo->destport);
    }

    cdata=(conv_data*)conversation_get_proto_data(conv, g_proto_ceph);
    if (cdata==NULL) {
        cdata = (conv_data*)se_alloc(sizeof(conv_data));
        cdata->pdu_table = se_tree_create(EMEM_TREE_TYPE_RED_BLACK, "ceph_pdu");
        cdata->to = conv_get_entity(&pinfo->dst, pinfo->destport);
        cdata->from = conv_get_entity(&pinfo->src, pinfo->srcport);
        cdata->direction_set = FALSE;
        cdata->displayT2F=NULL;
        cdata->displayF2T=NULL;
        conversation_add_proto_data(conv, g_proto_ceph, cdata);
    }
    return cdata;
}

/*
 * Reverse to/from in a stream to correct a wrong guess
 */
static void
conv_fixup(guint clientSrc, packet_info *pinfo) {
    conversation_t* conv;
    conv_data* cdata;
    entity_data* edata;
    guint fromClient;
    pdu_data* pdata;
    guint position;

    conv = conv_find_exact(pinfo->fd->num, &pinfo->src, pinfo->srcport, 
                &pinfo->dst, pinfo->destport);
    if (conv != NULL) {
        /* Do we need to reverse because from & to mixup */
        cdata=(conv_data*)conversation_get_proto_data(conv, g_proto_ceph);
        if (cdata->direction_set==FALSE) {
            cdata->direction_set=TRUE;
            fromClient = ADDRESSES_EQUAL(&pinfo->src, &cdata->from->addr) && 
                            pinfo->srcport == cdata->from->port;
            if (clientSrc != fromClient) {
                edata = cdata->from;
                cdata->from = cdata->to;
                cdata->to = edata;
                cdata->displayF2T = NULL;
                cdata->displayT2F = NULL;
            }

            /* & Reverse pdata as well correcting SERVER_BANNER if needed */
            position = 0xFFFFFFFF;
            while ((pdata = (pdu_data*)se_tree_lookup32_le(
                    cdata->pdu_table, position))!=NULL) {
                if (clientSrc != fromClient)
                    pdata->toserver = !pdata->toserver;
                if (pdata->toserver && pdata->type == SERVER_BANNER) {
                    pdata->type = CLIENT_BANNER;
                    pdata->seen &= (~SERVER_BANNER);
                    pdata->seen |= CLIENT_BANNER;
                }
                position = pdata->position -1;
            }
        }
    }
}

/*
 * Calculate the stream index for a given offset
 */
static guint32 
conv_position(packet_info *pinfo, tvbuff_t *tvb, guint32 offset) {
    return pinfo->fd->cum_bytes - tvb_reported_length_remaining (tvb, offset);
}

static guint32
conv_expecting(packet_info *pinfo, tvbuff_t *tvb, guint32 offset) {
    conv_data* cdata;
    pdu_data* pdata;
    guint32 allowed;
    guint32 position;

    cdata = conv_get(pinfo);
    
    /*
     * Return what should be happening next
     */
    position = conv_position(pinfo, tvb, offset);
    pdata = (pdu_data*)se_tree_lookup32_le(cdata->pdu_table, position);
    
    /*
     * If new may see any message type
     */
    if (pdata == 0) 
        return MSG_ANY;

    /*
     * If exact match, return what we actually saw in previous dissect
     */
    if (pdata->position == position) 
        return pdata->type;
    
    /*
     * Otherwise, work out what could be next, the difficulty here is that
     * the client banner may appear before or after the server sends its
     * addresses and also that we me mistake a client banner for a server
     * banner
     */
    allowed = 0;
    switch (pdata->type) {
        case SERVER_BANNER:
            return (CLIENT_BANNER | SERVER_ADDRESS | CLIENT_ADDRESS);
        case CLIENT_BANNER:
            if ((pdata->seen & SERVER_ADDRESS) == 0)
                allowed = SERVER_ADDRESS;
            allowed |= CLIENT_ADDRESS;
            break;
        case SERVER_ADDRESS:
            if ((pdata->seen & CLIENT_BANNER) == 0)
                allowed = CLIENT_BANNER;
            allowed |= CLIENT_ADDRESS;
            break;
        case CLIENT_ADDRESS:
            allowed = CONNECT_REQUEST;
            break;
        case CONNECT_REQUEST:
            allowed = CONNECT_REPLY;
            break;
        case CONNECT_REPLY:
        default:
            allowed = CONVERSING;
    }
    return allowed;
}

/*
 * Tag a position in a conversation as a specific type
 */
static void 
conv_tag(packet_info *pinfo, tvbuff_t *tvb, 
        guint32 offset, enum msg_type type) {
    conv_data* cdata;
    pdu_data* pdata;
    pdu_data* previous_pdata;
    guint32 position;

    /*
     * Get/Create a pdu_data to describe the message
     */
    cdata = conv_get(pinfo);
    position = conv_position(pinfo, tvb, offset);
    pdata = (pdu_data*)se_tree_lookup32(cdata->pdu_table, position);

    if (pdata == 0) {
        pdata = (pdu_data*)se_alloc(sizeof(pdu_data));
        pdata->position = position;
        pdata->toserver = (ADDRESSES_EQUAL(&cdata->to->addr, &pinfo->dst) &&
            cdata->to->port == pinfo->destport);
        pdata->type = type;
        pdata->seen = type;
        
        /*
         * If new we have to carry forward previously seen to allow for
         * checking on interleaving of messages, a pain I know..
         */
        previous_pdata = (pdu_data*)se_tree_lookup32_le(cdata->pdu_table, 
                position-1);
        if (previous_pdata != 0) {
            pdata->seen |= previous_pdata->seen;
        }

        /*
         * Insert new record
         */
        se_tree_insert32(cdata->pdu_table, position, pdata);
        
    } else {
        /*
         * Just update the existing record
         */
        pdata->type = type;
        pdata->seen |= type;
    }
}

static void
conv_setfrom(packet_info *pinfo, entity_type type) {
    conv_data* cdata;
    
    cdata = conv_get(pinfo);
    cdata->from->type = type;
    cdata->displayT2F = NULL;
    cdata->displayF2T = NULL;
}

static char*
conv_format(const entity_data* from, const entity_data* to) {
    const char* from_name;
    const char* to_name;

    from_name = entityTypeDescription(from->type);
    if (from->name) {
        from_name = se_strdup_printf("%s.%s", 
            entityTypeDescription(from->type), from->name);
    }

    to_name = entityTypeDescription(to->type);
    if (to->name) {
        to_name = se_strdup_printf("%s.%s", 
            entityTypeDescription(to->type), to->name);
    }

    return se_strdup_printf("%s -> %s", from_name, to_name);
}

/*
 * Get conversation display string
 */
static char*
conv_display(packet_info *pinfo) {
    conv_data* cdata;
    guint to;
    
    cdata = conv_get(pinfo);
    to = ADDRESSES_EQUAL(&cdata->to->addr, &pinfo->dst) &&
        cdata->to->port == pinfo->destport;

    if (to) {
        if (cdata->displayF2T == NULL) {
            cdata->displayF2T = conv_format(cdata->from, cdata->to);
        }
        return cdata->displayF2T;
    } else {
        if (cdata->displayT2F == NULL) {
            cdata->displayT2F = conv_format(cdata->to, cdata->from);
        }
        return cdata->displayT2F;
    }
}

/*
 * Sockaddr details extracted from protocol
 * Note: Don't map from tvb, only assign to avoid portability concerns
 */
typedef struct _sockaddr_info {
    guint16 port;
    guint32 addr;
} sockaddr_info;

/*
 * Entity info as extracted from protocol
 * Note: Don't map from tvb, only assign to avoid portability concerns
 */
typedef struct _entity_info {
    guint32 erank;
    guint32 nonce;
    sockaddr_info addr;
} entity_info;

/*
 * Recover conversation entity data from entity_info
 */
static entity_data*
conv_get_entity_frominfo(entity_info* info) {
    address addr;

    SET_ADDRESS(&addr, AT_IPv4, sizeof(info->addr.addr), &info->addr.addr);
    return conv_get_entity(&addr, info->addr.port);
}

/*
 * Set type & name for an entity
 * nameOffset is offset into tvb of where to find FT_UINT_STRING
 */
static void
conv_set_entity_name(entity_data* edata, tvbuff_t *tvb, 
        guint32 nameOffset, entity_type type) {
    guint32 len;

    if (type != ENTITY_TYPE_UNKNOWN)
        edata->type = type;

    /* Same name? */
    len = tvb_get_letohl(tvb, nameOffset);
    if (edata->name) {
        if (strlen(edata->name) == len &&
            tvb_memeql(tvb, nameOffset+4, edata->name, len)) {
                return;
        }
    }
    edata->name = (char*)se_alloc(len+1);
    tvb_memcpy(tvb, edata->name, nameOffset+4,len);
    edata->name[len]=0;
}

/****************************************************************************
 * INFO Column helpers
 ****************************************************************************/

/*
 * Flag for controlling formatting of INFO column handling, this needs
 * a manual reset before decoding possible multiple PDUs
 */
static guint g_firstPdu;

/*
 * Helper to prepare for a PDU display by setting info column.
 */
static void
setPDUInfo(packet_info *pinfo, const char* info) {

    /* 
     * Reset info column, with a fence to stop being changed by other PDUs that
     * might be contained in the same packet.
     */
    if(check_col(pinfo->cinfo,COL_INFO)){
        if (g_firstPdu == TRUE)
            col_add_fstr(pinfo->cinfo, COL_INFO, " %s", info);
        else
            col_add_fstr(pinfo->cinfo, COL_INFO, ", %s", info);
        col_set_fence(pinfo->cinfo,COL_INFO);
    }
}


/****************************************************************************
 * Start of dissector handlers
 * NOTES:
 * The coding style is odd, we avoid the normal use of structures to describe
 * on the wire formats because of the difficulty of getting *all* compilers
 * to handle the packing correctly. Wireshark coding style prefers direct
 * decoding so that is what we do. I have included structures in comments
 * just to make it easier to understand what the code is pulling apart.
 *
 * The dissector functions *may* check the data contents for sanity to allow 
 * them to be used where the content type is ambiguous. If the data provided
 * can not be for that dissector the return will be 0. If there is not enough
 * data then they return -1. Otherwise they return the number of bytes they
 * consumed so they can be used to dissect multiple PDUs per packet. 
 *
 ****************************************************************************/

/*
 * Message tag types, numbers must match protocol
 * There are two types of tags here, most are used with connect_reply to 
 * indicate status, but CLOSE, MSG, ACK & KEEPALIVE tags are used for
 * standalone messages. TAG_SEQ is another oddball, it indicates the server
 * is requesting an exhachange of sequence numbers following the connect_reply.
 */
enum msg_tag {
    MSGR_TAG_MIN=1,
    MSGR_TAG_READY=1,            /* S->C: ready for messages */
    MSGR_TAG_RESETSESSION=2,    /* S->C: reset, try again */
    MSGR_TAG_WAIT=3,            /* S->C: wait for racing incoming connection */
    MSGR_TAG_RETRY_SESSION=4,    /* S->C + cseq: try again with higher cseq */
    MSGR_TAG_RETRY_GLOBAL=5,    /* S->C + gseq: try again with higher gseq */
    MSGR_TAG_CLOSE=6,            /* closing pipe */
    MSGR_TAG_MSG=7,                /* message */
    MSGR_TAG_ACK=8,                /* message ack */
    MSGR_TAG_KEEPALIVE=9,        /* just a keepalive byte! */
    MSGR_TAG_BADPROTOVER=10,    /* bad protocol version */
    MSGR_TAG_BADAUTHORIZER=11,    /* bad authorizer */
    MSGR_TAG_FEATURES=12,        /* insufficient features */
    MSGR_TAG_SEQ=13,            /* 64-bit int follows with seen seq number */
    MSGR_TAG_MAX=13
};

static guint 
isMsgrTag(guint tag) {
    return (tag>=MSGR_TAG_MIN && tag<=MSGR_TAG_MAX);
}

static guint 
isMsgrConnectTag(guint tag) {
    return isMsgrTag(tag) && 
        (tag != MSGR_TAG_CLOSE) &&
        (tag != MSGR_TAG_MSG) &&
        (tag != MSGR_TAG_ACK) &&
        (tag != MSGR_TAG_KEEPALIVE);
}

static const char* 
replyTagDescription(guint tag) {
    switch (tag) {
        case MSGR_TAG_READY: return "Ready";
        case MSGR_TAG_RESETSESSION: return "Reset Session";
        case MSGR_TAG_WAIT: return "Wait";
        case MSGR_TAG_RETRY_SESSION: return "Retry Session";
        case MSGR_TAG_RETRY_GLOBAL: return "Retry Global";
        case MSGR_TAG_BADPROTOVER: return "Bad Protocol";
        case MSGR_TAG_BADAUTHORIZER: return "Bad Authorizer";
        case MSGR_TAG_FEATURES: return "Missing Features";
        case MSGR_TAG_SEQ: return "Sequence";
    }
    return NULL;
}

/****************************************************************************
 * Handshake handling (Banners & Addresses)
 ****************************************************************************/

#define CEPH_BANNER "ceph v027"
#define CEPH_BANNER_LEN (sizeof(CEPH_BANNER)-1)
#define CEPH_BANNER_MAGIC_LEN 4

/*
 * Dissect a banner, this can be used in either direction, client to server
 * or server to client.
 */
static gint 
dissect_banner(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint offset, guint client)
{
    proto_tree *ceph_banner_tree = NULL;
    proto_item *ceph_sub_item = NULL;

    if (tvb_strneql(tvb, offset, CEPH_BANNER, CEPH_BANNER_LEN) == 0) {
        
        /* All OK, just show the banner */
        setPDUInfo(pinfo, client?"Client Banner":"Server Banner");
        ceph_sub_item = proto_tree_add_item( tree, hf_banner, tvb, offset, 
                CEPH_BANNER_LEN, TRUE );
        ceph_banner_tree = proto_item_add_subtree(ceph_sub_item, ett_banner);
        proto_tree_add_item(ceph_banner_tree, hf_banner_magic, tvb, offset, 
            CEPH_BANNER_MAGIC_LEN, TRUE);
        proto_tree_add_item(ceph_banner_tree, hf_banner_version, tvb, offset+
            CEPH_BANNER_MAGIC_LEN, CEPH_BANNER_LEN-CEPH_BANNER_MAGIC_LEN, TRUE);

        return CEPH_BANNER_LEN;
    } else if (tvb_reported_length_remaining (tvb, offset) >= 
            (gint)CEPH_BANNER_LEN) {
        /* Enough data, but not a banner */
        return 0;
    } else {
        /* Not enough data */
        return -1;
    }
}

/*
 * Supported family types, these are defined here for portability.
 */
typedef enum _families {
    CEPH_AF_INET=2,
    CEPH_AF_INET6=10
} families;

static guint
isFamily(guint32 family) {
    return ((family==CEPH_AF_INET) || (family==CEPH_AF_INET6));
}

static const char* 
familyDescription(guint family) {
    switch (family) {
        case CEPH_AF_INET: return "inet V4";
        case CEPH_AF_INET6: return "inet V6";
    }
    return NULL;
}

/*
 * Storage structure for addresses, can really only be AF_INET or AF_INET6
 * but this is the structure used by the protocol.
 *
struct ceph_sockaddr_storage {
    guint16 ss_family;  
    guint8 __ss_pad1[6];
    guint64 __ss_align; 
    guint8 __ss_pad2[112];
}
*/
#define SIZE_OF_SOCKADDR_STORAGE    (2+6+8+112)

/*
 * Dissect a sockaddr_storage. This may be a IPv4 or IPv6 format but currently
 * only IPv4 is handled.
 */
static gint 
dissect_sockaddr_in(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint offset, sockaddr_info* info)
{
    proto_tree *ceph_sockaddr_tree;
    proto_item *ceph_sub_item = NULL;
    guint16 family;

    ceph_sub_item = proto_tree_add_item( tree, hf_sockaddr_in, tvb, offset, 
            16, TRUE );
    ceph_sockaddr_tree = proto_item_add_subtree(ceph_sub_item, ett_sockaddr_in);

    family = tvb_get_ntohs(tvb, offset);
    if (family == CEPH_AF_INET6) {
        expert_add_info_format(pinfo, ceph_sub_item, PI_UNDECODED, PI_WARN, 
            "Address using IPv6 addressing, dissecting is not yet supported");
    } else if (family == CEPH_AF_INET) {
        proto_tree_add_uint_format_value(ceph_sockaddr_tree, hf_sin_family, tvb,
            offset, 2,     family, "%d (%s)", family, familyDescription(family));
        proto_tree_add_item(ceph_sockaddr_tree, hf_sin_port, tvb, offset+2, 
            2, ENC_BIG_ENDIAN);
        proto_tree_add_item(ceph_sockaddr_tree, hf_sin_addr, tvb, offset+4, 
            4, FALSE);
        if (info) {
            info->port = tvb_get_ntohs(tvb, offset+2);
            info->addr = tvb_get_letohl(tvb, offset+4);
        }
    } else {
        expert_add_info_format(pinfo, ceph_sub_item, PI_UNDECODED, PI_WARN, 
            "Unknown family (%d) being used in address", family);
    }
    offset += 16;
    return offset;
}

/*
 * entity_addr - endpoint details including IP address
 *
struct ceph_entity_addr {
    guint32 type;
    guint32 nonce;                            // unique id for process (e.g. pid) 
    struct ceph_sockaddr_storage in_addr;
};
*/
#define SIZE_OF_ENTITY_ADDR    (4+4+SIZE_OF_SOCKADDR_STORAGE)

/*
 * Dissect an entity address, just a type
 */
static gint 
dissect_entity_addr(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint offset, entity_info* info)
{
    proto_tree *ceph_entity_tree = NULL;
    proto_item *ceph_sub_item = NULL;
    guint16 family;

    /* Check the sockaddr_in looks sane before full decoding */
    if (tvb_reported_length_remaining (tvb, offset) < SIZE_OF_ENTITY_ADDR) 
        return -1;

    /* TODO: Could really do with better sanity checking here */
    family = tvb_get_ntohs(tvb, offset+8);
    if (family != CEPH_AF_INET && family!= CEPH_AF_INET6) {
        return 0;
    }

    /* Decode it */
    if (tree) {
        ceph_sub_item = proto_tree_add_item( tree, hf_entity_addr, tvb, offset, 
                SIZE_OF_ENTITY_ADDR, TRUE );
        ceph_entity_tree = proto_item_add_subtree(ceph_sub_item, 
                ett_entity_addr);
        proto_tree_add_item(ceph_entity_tree, hf_entity_erank, tvb, offset, 
                4, TRUE);
        proto_tree_add_item(ceph_entity_tree, hf_entity_nonce, tvb, offset+4, 
                4, TRUE);
    }

    dissect_sockaddr_in(tvb, pinfo, ceph_entity_tree, offset+8, 
            info? &info->addr:NULL);

    if (info) {
        info->erank = tvb_get_letohl(tvb, offset);
        info->nonce = tvb_get_letohl(tvb, offset+4);
    }

    return SIZE_OF_ENTITY_ADDR;
}

/*
 * server-address - passed from server to client during handshake
 *
struct ceph_server_address {
    struct ceph_entity_addr server_addr;
    struct ceph_entity_addr client_addr;
};
*/
static gint
dissect_server_address(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint offset) {
    gint len1;
    gint len2;
    
    len1 = dissect_entity_addr(tvb, pinfo, tree, offset, NULL);
    if (len1 > 0) {
        offset += len1;
        len2 = dissect_entity_addr(tvb, pinfo, tree, offset, NULL);
        if (len2>0) {
            setPDUInfo(pinfo, "Server Addresses");
            conv_fixup(FALSE, pinfo);
            return len1+len2;
        }
        return len2;
    }
    return len1;
}

/*
 * client-address - passed from client to server during handshake
 *
struct ceph_client_address {
    struct ceph_entity_addr client_addr;
};
*/
static gint
dissect_client_address(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint offset) {
    gint len;

    /* To avoid identifying a server address as a client address we rely on the 
     * server sending its data un-fragmented after a banner, so either we have 
     * data for two valid addresses or this must be a client address.
     */
    if (tvb_reported_length_remaining (tvb, offset) >= 2*SIZE_OF_ENTITY_ADDR) {
        /* Might be a server address, peek to see */
        if (isFamily(tvb_get_ntohs(tvb, offset+8)) && 
            isFamily(tvb_get_ntohs(tvb, offset+SIZE_OF_ENTITY_ADDR+8)))
            return 0;
    }

    /* Is it just a client addr */
    len=dissect_entity_addr(tvb, pinfo, tree, offset, NULL);
    if (len>0) {
        setPDUInfo(pinfo,"Client Address");
        conv_fixup(TRUE, pinfo);
    }
    return len;
}


/****************************************************************************
 * Connection Request & Reply
 ****************************************************************************/

/*
 * msg_connect - Client->Server connection request
 *
struct ceph_msg_connect {
    guint64 features;            // supported feature bits 
    guint32 host_type;            // CEPH_ENTITY_TYPE_*
    guint32 global_seq;            // count connections initiated by this host 
    guint32 connect_seq;        // count connections initiated in this session 
    guint32 protocol_version;
    guint32 authorizer_protocol;
    guint32 authorizer_len;
    guint8  flags;                // CEPH_MSG_CONNECT_* 
}
*/
#define SIZE_OF_MSG_CONNECT    (8+4+4+4+4+4+4+1)

#define FEATURE_UID                        G_GINT64_CONSTANT(1<<0)
#define FEATURE_NOSRCADDR                G_GINT64_CONSTANT(1<<1)
#define FEATURE_MONCLOCKCHECK            G_GINT64_CONSTANT(1<<2)
#define FEATURE_FLOCK                    G_GINT64_CONSTANT(1<<3)
#define FEATURE_SUBSCRIBE2                G_GINT64_CONSTANT(1<<4)
#define FEATURE_MONNAMES                G_GINT64_CONSTANT(1<<5)
#define FEATURE_RECONNECT_SEQ            G_GINT64_CONSTANT(1<<6)
#define FEATURE_DIRLAYOUTHASH            G_GINT64_CONSTANT(1<<7)
#define FEATURE_OBJECTLOCATOR            G_GINT64_CONSTANT(1<<8)
#define FEATURE_PGID64                    G_GINT64_CONSTANT(1<<9)
#define FEATURE_INCSUBOSDMAP            G_GINT64_CONSTANT(1<<10)
#define FEATURE_PGPOOL3                    G_GINT64_CONSTANT(1<<11)
#define FEATURE_OSDREPLYMUX                G_GINT64_CONSTANT(1<<12)
#define FEATURE_OSDENC                    G_GINT64_CONSTANT(1<<13)
#define FEATURE_OMAP                    G_GINT64_CONSTANT(1<<14)
#define FEATURE_MONENC                    G_GINT64_CONSTANT(1<<15)
#define FEATURE_QUERY_T                    G_GINT64_CONSTANT(1<<16)
#define FEATURE_INDEP_PG_MAP            G_GINT64_CONSTANT(1<<17)
#define FEATURE_CRUSH_TUNABLES            G_GINT64_CONSTANT(1<<18)
#define FEATURE_CHUNKY_SCRUB            G_GINT64_CONSTANT(1<<19)
#define FEATURE_MON_NULLROUTE            G_GINT64_CONSTANT(1<<20)
#define FEATURE_MON_GV                    G_GINT64_CONSTANT(1<<21)
#define FEATURE_BACKFILL_RESERVATION    G_GINT64_CONSTANT(1<<22)
#define FEATURE_MSG_AUTH                G_GINT64_CONSTANT(1<<23)
#define FEATURE_RECOVERY_RESERVATION    G_GINT64_CONSTANT(1<<24)
#define FEATURE_CRUSH_TUNABLES2            G_GINT64_CONSTANT(1<<25)
#define FEATURE_CREATEPOOLID            G_GINT64_CONSTANT(1<<26)
#define FEATURE_REPLY_CREATE_INODE        G_GINT64_CONSTANT(1<<27)
#define FEATURE_OSD_HBMSGS                G_GINT64_CONSTANT(1<<28)
#define FEATURE_MDSENC                    G_GINT64_CONSTANT(1<<29)
#define FEATURE_OSDHASHPSPOOL            G_GINT64_CONSTANT(1<<30)
#define FEATURE_MON_SINGLE_PAXOS        G_GINT64_CONSTANT(1<<31)
#define FEATURE_OSD_SNAPMAPPER            G_GINT64_CONSTANT(0x100000000)

#define FEATURES_ALL           \
    (FEATURE_UID |             \
    FEATURE_NOSRCADDR |        \
    FEATURE_MONCLOCKCHECK |    \
    FEATURE_FLOCK |            \
    FEATURE_SUBSCRIBE2 |       \
    FEATURE_MONNAMES |         \
    FEATURE_RECONNECT_SEQ |    \
    FEATURE_DIRLAYOUTHASH |    \
    FEATURE_OBJECTLOCATOR |    \
    FEATURE_PGID64 |           \
    FEATURE_INCSUBOSDMAP |     \
    FEATURE_PGPOOL3 |          \
    FEATURE_OSDREPLYMUX |      \
    FEATURE_OSDENC |           \
    FEATURE_OMAP |             \
    FEATURE_QUERY_T |          \
    FEATURE_MONENC |           \
    FEATURE_INDEP_PG_MAP |     \
    FEATURE_CRUSH_TUNABLES |   \
    FEATURE_CHUNKY_SCRUB |     \
    FEATURE_MON_NULLROUTE |    \
    FEATURE_MON_GV |           \
    FEATURE_BACKFILL_RESERVATION | \
    FEATURE_MSG_AUTH |             \
    FEATURE_RECOVERY_RESERVATION | \
    FEATURE_CRUSH_TUNABLES2 |      \
    FEATURE_CREATEPOOLID |         \
    FEATURE_REPLY_CREATE_INODE |   \
    FEATURE_OSD_HBMSGS |           \
    FEATURE_MDSENC |               \
    FEATURE_OSDHASHPSPOOL |        \
    FEATURE_MON_SINGLE_PAXOS |     \
    FEATURE_OSD_SNAPMAPPER)

struct feature_description {
    guint64 feature;
    const char* description;
};

struct feature_description fdesc[] = {
    {FEATURE_UID, "uid"},
    {FEATURE_NOSRCADDR, "nosrcaddr"},
    {FEATURE_MONCLOCKCHECK, "monclockcheck"},
    {FEATURE_FLOCK, "flock"},
    {FEATURE_SUBSCRIBE2, "subscribe2"},
    {FEATURE_MONNAMES, "monnames"},
    {FEATURE_RECONNECT_SEQ, "reconnect_seq"},
    {FEATURE_DIRLAYOUTHASH, "dirlayouthash"},
    {FEATURE_OBJECTLOCATOR, "objectlocator"},
    {FEATURE_PGID64, "pgid64"},
    {FEATURE_INCSUBOSDMAP, "incsubosdmap"},
    {FEATURE_PGPOOL3, "pgpool3"},
    {FEATURE_OSDREPLYMUX, "osdreplymux"},
    {FEATURE_OSDENC, "osdenc"},
    {FEATURE_OMAP, "omap"},
    {FEATURE_QUERY_T, "query_t"},
    {FEATURE_MONENC, "monenc"},
    {FEATURE_INDEP_PG_MAP, "indep_pg_map"},
    {FEATURE_CRUSH_TUNABLES, "crush_tunables"},
    {FEATURE_CHUNKY_SCRUB, "chunk_scrub"},
    {FEATURE_MON_NULLROUTE, "mon_nullroute"},
    {FEATURE_MON_GV, " mon_gv"},
    {FEATURE_BACKFILL_RESERVATION, "backfill_reservation"},
    {FEATURE_MSG_AUTH, "msg_auth"},
    {FEATURE_RECOVERY_RESERVATION, "recovery_reservation"},
    {FEATURE_CRUSH_TUNABLES2, "crush_tunables2"},
    {FEATURE_CREATEPOOLID, "createpoolid"},
    {FEATURE_REPLY_CREATE_INODE, "reply_create_inode"},
    {FEATURE_OSD_HBMSGS, "osd_hbmsgs"},
    {FEATURE_MDSENC, "mdsenc"},
    {FEATURE_OSDHASHPSPOOL, "osdhashpspool"},
    {FEATURE_MON_SINGLE_PAXOS, "mon_single_paxos"},
    {FEATURE_OSD_SNAPMAPPER, "osd_snapmapper"}
};
#define FEATURE_COUNT (sizeof(fdesc)/sizeof(struct feature_description))

static char* featureDescription(guint64 features) {
    guint i;
    char *desc;
    char *next;

    if (features == (guint64)FEATURES_ALL) {
        return g_strdup("All features");
    } else {
        desc=g_strdup("");
        for (i=0; i< FEATURE_COUNT; i++) {
            if (features & fdesc[i].feature) {
                next = g_strconcat(desc, fdesc[i].description, " ", NULL);
                g_free(desc);
                desc = next;
            }
        }
        return desc;
    }
}

/*
 * Connect message flags, numbers must match protocol
 * There is only a lossy flag, maybe more will be added later
 */
typedef enum _connect_flags {
    MSG_CONNECT_MIN=1,
    MSG_CONNECT_LOSSY=1,
    MSG_CONNECT_MAX=1
} connect_flags;

static const char* 
connectFlagDescription(guint flag) {
    switch (flag) {
        case MSG_CONNECT_LOSSY: return "Lossy";
    }
    return "No Flags";
}

static void
format_connect_features(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint offset) {
    proto_item* item;
    guint64 features;
    char* desc;
    char linebuf[201];
    guint len, start, end;

    features = tvb_get_letoh64(tvb, offset);
    desc = featureDescription(features);
    len = strlen(desc);
    if (len < 150) {
        /* Short description on one line */
        item = proto_tree_add_uint64_format_value(tree, hf_connect_features, 
            tvb, offset, 8, features, "0x%" G_GINT64_MODIFIER "x (%s)", 
            features, desc);
    } else {

        /* Format as multi-line entry */
        item = proto_tree_add_uint64_format_value(tree, hf_connect_features, 
            tvb, offset, 8, features, "0x%" G_GINT64_MODIFIER "x", features);

        start = 0;
        while (start < len) {
            end = start + (strlen(&desc[start])>200 ? 200 : 
                strlen(&desc[start]));
            while (end>len || desc[end-1] != ' ') 
                end--;
            strncpy(linebuf, &desc[start], end-start);
            linebuf[end-start]=0;
            proto_tree_add_uint64_format_value(tree, hf_connect_features, tvb, 
                offset, 8, features, "%s", linebuf);
            start = end;
        }
    }
    g_free(desc);

    if ((features & (~FEATURES_ALL)) != 0) {
        expert_add_info_format(pinfo, item, PI_UNDECODED, PI_WARN, 
            "Unexpected feature flags seen, dissector may need updating");
    }
}

static gint
dissect_connect_request(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint offset) {

    gint auth_len;
    entity_type host_type;
    guint flags;

    if (tvb_reported_length_remaining (tvb, offset) < SIZE_OF_MSG_CONNECT) 
        return -1;

    host_type = (entity_type)tvb_get_letohl(tvb, offset+8);
    if (!isEntityType(host_type))
        return 0;

    auth_len = tvb_get_letohl(tvb, offset+28);
    if (tvb_reported_length_remaining (tvb, offset) < 
            SIZE_OF_MSG_CONNECT+auth_len) 
        return -1;

    /* All here, just decode */
    setPDUInfo(pinfo, "Connect Request");
    format_connect_features(tvb, pinfo, tree, offset);
    proto_tree_add_uint_format_value(tree, hf_connect_host_type, tvb, offset+8, 
        4, host_type, "%d (%s)", host_type, entityTypeDescription(host_type));
    proto_tree_add_item(tree, hf_connect_global_seq, tvb, offset+12, 4, TRUE);
    proto_tree_add_item(tree, hf_connect_connect_seq, tvb, offset+16, 4, TRUE);
    proto_tree_add_item(tree, hf_connect_protocol_version, tvb, offset+20, 
        4, TRUE);
    proto_tree_add_item(tree, hf_connect_authorizer_protocol, tvb, offset+24, 
        4, TRUE);
    proto_tree_add_item(tree, hf_connect_authorizer_len, tvb, offset+28, 
        4, TRUE);
    flags = tvb_get_guint8(tvb, offset+32);
    proto_tree_add_uint_format_value(tree, hf_connect_flags, tvb, offset+32, 1, 
        flags, "%d (%s)", flags, connectFlagDescription(flags));

    if (auth_len >0)
        proto_tree_add_item(tree, hf_connect_authentication_key, tvb, offset+33, 
                auth_len, TRUE);

    /* Tag it in conv */
    conv_fixup(TRUE, pinfo);
    conv_tag(pinfo, tvb, offset, CONNECT_REQUEST);
    conv_setfrom(pinfo, (entity_type)tvb_get_letohl(tvb, offset+8));

    return SIZE_OF_MSG_CONNECT+auth_len;
}

/*
 * msg_connect_reply - Server->Client connection reply following a request
 *
 struct ceph_msg_connect_reply {
    guint8 tag;
    guint64 features;
    guint32 global_seq;
    guint32 connect_seq;
    guint32 protocol_version;
    guint32 authorizer_len;
    guint8 flags;
}
*/
#define SIZE_OF_MSG_CONNECT_REPLY    (1+8+4+4+4+4+1)   

static gint
dissect_connect_reply(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint offset) {

    gint auth_len;
    guint tag;
    guint flags;

    /* Test we have enough data including auth */
    if (tvb_reported_length_remaining (tvb, offset) < SIZE_OF_MSG_CONNECT_REPLY) 
        return -1;

    /* TODO: This isn't an ideal test but we need something :-( */
    tag = tvb_get_guint8(tvb, offset);
    if (!isMsgrConnectTag(tag))
        return 0;

    auth_len = tvb_get_letohl(tvb, offset+21);
    if (tvb_reported_length_remaining (tvb, offset) < 
            SIZE_OF_MSG_CONNECT_REPLY+auth_len) 
        return -1;

    setPDUInfo(pinfo, "Connect Reply");
    proto_tree_add_uint_format_value(tree, hf_connect_tag, tvb, offset, 1, 
        tag, "%d (%s)", tag, replyTagDescription(tag));
    format_connect_features(tvb, pinfo, tree, offset+1);
    proto_tree_add_item(tree, hf_connect_global_seq, tvb, offset+9, 4, TRUE);
    proto_tree_add_item(tree, hf_connect_connect_seq, tvb, offset+13, 4, TRUE);
    proto_tree_add_item(tree, hf_connect_protocol_version, tvb, offset+17, 
        4, TRUE);
    proto_tree_add_item(tree, hf_connect_authorizer_len, tvb, offset+21, 
        4, TRUE);
    flags = tvb_get_guint8(tvb, offset+25);
    proto_tree_add_uint_format_value(tree, hf_connect_flags, tvb, offset+25, 1, 
        flags, "%d (%s)", flags, connectFlagDescription(flags));

    /* Tag it in conversation */
    conv_fixup(FALSE, pinfo);
    conv_tag(pinfo, tvb, offset, CONNECT_REPLY);
    return SIZE_OF_MSG_CONNECT_REPLY+auth_len;
}

/*
 * msg_ack - Simple ack of previous messages
 *
 struct ceph_msg_ack {
    guint8 tag;
    guint64 seq;    
}
*/
#define SIZE_OF_MSG_ACK    (1+8)   

static guint 
dissect_ack(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, int offset)
{
    if (tvb_reported_length_remaining (tvb, offset) < SIZE_OF_MSG_ACK) 
        return -1;

    if (tree) {
        setPDUInfo(pinfo, "Ack");
        proto_tree_add_item(tree, hf_hdr_tag, tvb, offset, 1, TRUE);
        proto_tree_add_item(tree, hf_hdr_seq_ack, tvb, offset+1, 8, TRUE);
    }
    return SIZE_OF_MSG_ACK;
}

/*
 * entity_name
 *
struct ceph_entity_name {
    guint8 type;      //
    guint64 num;
}
*/
#define SIZE_OF_ENTITY_NAME    (1+8)   

static guint 
dissect_entity_name(tvbuff_t *tvb, proto_tree *tree, gint hf, int offset) {
    proto_item *entity_name_item;
    proto_tree *entity_name_tree;

    if (tvb_reported_length_remaining (tvb, offset) < SIZE_OF_ENTITY_NAME) 
        return -1;

    if (tree) {
        entity_name_item = proto_tree_add_item( tree, hf, tvb, offset, 
            SIZE_OF_ENTITY_NAME, TRUE );
        entity_name_tree = proto_item_add_subtree(entity_name_item, 
            ett_entity_name);

        proto_tree_add_item(entity_name_tree, hf_entity_type, tvb, offset, 
            1, TRUE);
        proto_tree_add_item(entity_name_tree, hf_entity_num, tvb, offset+1, 
            8, TRUE);
    }
    return SIZE_OF_ENTITY_NAME;
}

/*
 * msg_header 
 *
 struct ceph_msg_header {
    guint64 seq;        // message seq# for this session 
    guint64 tid;        // transaction id
    guint16 type;       // message type
    guint16 priority;   // priority.  higher value == higher priority
    guint16 version;    // version of message encoding 

    guint32 front_len;    // bytes in main payload 
    guint32 middle_len;    // bytes in middle payload
    guint32 data_len;    // bytes of data payload
    guint16 data_off;    // sender: include full offset; 
                        // receiver: mask against ~PAGE_MASK 

    struct ceph_entity_name src;
    guint32 reserved;
    guint32 crc;        // header crc32c
}
*/
#define SIZE_OF_MSG_HEADER    (8+8+2+2+2 +4+4+4+2 +SIZE_OF_ENTITY_NAME+4+4)   
#define HEADER_TYPE_OFFSET    (8+8)
#define HEADER_ETYPE_OFFSET    (8+8+2+2+2 +4+4+4+2)   
#define HEADER_CRC_OFFSET    (SIZE_OF_MSG_HEADER-4) 

typedef struct _header_info {
    guint32 type;
    guint32 front_len;
    guint32 middle_len;
    guint32 data_len;
} header_info;

static gint 
dissect_msg_header(tvbuff_t *tvb, proto_tree *tree, int offset, 
        header_info* info) {
    proto_item *header_item;
    proto_tree *header_tree;

    if (tvb_reported_length_remaining (tvb, offset) < SIZE_OF_MSG_HEADER) 
        return -1;

    if (info) {
        info->type = tvb_get_letohs(tvb, offset+16);
        info->front_len = tvb_get_letohl(tvb, offset+22);
        info->middle_len = tvb_get_letohl(tvb, offset+26);
        info->data_len = tvb_get_letohl(tvb, offset+30);
    }

    if (tree) {
        header_item = proto_tree_add_item( tree, hf_header, tvb, offset, 
                SIZE_OF_MSG_HEADER, TRUE );
        header_tree = proto_item_add_subtree(header_item, ett_header);

        proto_tree_add_item(header_tree, hf_hdr_seq, tvb, offset+0, 8, TRUE);
        proto_tree_add_item(header_tree, hf_hdr_tid, tvb, offset+8, 8, TRUE);
        proto_tree_add_item(header_tree, hf_hdr_type, tvb, offset+16, 2, TRUE);
        proto_tree_add_item(header_tree, hf_hdr_priority, tvb, offset+18, 
            2, TRUE);
        proto_tree_add_item(header_tree, hf_hdr_version, tvb, offset+20, 
            2, TRUE);

        proto_tree_add_item(header_tree, hf_hdr_front_len, tvb, offset+22, 
            4, TRUE);
        proto_tree_add_item(header_tree, hf_hdr_middle_len, tvb, offset+26, 
            4, TRUE);
        proto_tree_add_item(header_tree, hf_hdr_data_len, tvb, offset+30, 
            4, TRUE);
        proto_tree_add_item(header_tree, hf_hdr_data_off, tvb, offset+34, 
            2, TRUE);
        
        dissect_entity_name(tvb, header_tree, hf_hdr_src, offset+36);

        proto_tree_add_item(header_tree, hf_hdr_crc, tvb, 
            offset+36+4+SIZE_OF_ENTITY_NAME, 4, TRUE);
    }
    return SIZE_OF_MSG_HEADER;
}

/*
 * msg_footer 
 *
 struct ceph_msg_footer {
    guint32 front_crc;
    guint32 middle_crc;
    guint32 data_crc;
    guint64 sig;
    guint8 flags;
}
*/
#define SIZE_OF_MSG_FOOTER    (4+4+4+8+1)   

static guint32 
dissect_footer(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
    proto_item *footer_item;
    proto_tree *footer_tree;

    if (tvb_reported_length_remaining (tvb, offset) < SIZE_OF_MSG_FOOTER) 
        return -1;

    if (tree) {
        footer_item = proto_tree_add_item( tree, hf_footer, tvb, offset, 
            SIZE_OF_MSG_FOOTER, TRUE );
        footer_tree = proto_item_add_subtree(footer_item, ett_footer);

        proto_tree_add_item(footer_tree, hf_footer_front_crc, tvb, offset, 
            4, TRUE);
        proto_tree_add_item(footer_tree, hf_footer_middle_crc, tvb, offset+4, 
            4, TRUE);
        proto_tree_add_item(footer_tree, hf_footer_data_crc, tvb, offset+8, 
            4, TRUE);
        proto_tree_add_item(footer_tree, hf_footer_sig, tvb, offset+12, 
            8, TRUE);
        proto_tree_add_item(footer_tree, hf_footer_flags, tvb, offset+20, 
            1, TRUE);
    }

    return SIZE_OF_MSG_FOOTER;
}

static guint32 
dissect_fsid(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
    guint32 fsid[4];

    tvb_memcpy(tvb, &fsid, offset, sizeof(fsid));
    proto_tree_add_text(tree, tvb, offset, sizeof(fsid), "fsid: %x-%x-%x-%x", 
        g_ntohl(fsid[0]), g_ntohl(fsid[1]), g_ntohl(fsid[2]), g_ntohl(fsid[3]));
    return sizeof(fsid);
}

/*
 * Message types
 * There is no order to these as yet, maybe better split by source->target type?
 */
typedef enum _msg_ctype {
    MSG_SHUTDOWN=1,
    MSG_PING=2,
    MSG_MON_MAP=4,
    MSG_MON_GET_MAP=5,
    MSG_STATFS=13,
    MSG_STATFS_REPLY=14,
    MSG_MON_SUBSCRIBE=15,
    MSG_MON_SUBSCRIBE_ACK=16,
    MSG_AUTH=17,
    MSG_AUTH_REPLY=18,
    MSG_MON_GET_VERSION=19,
    MSG_MON_GET_VERSION_REPLY=20,
    MSG_MDS_MAP=21,
    MSG_CLIENT_SESSION=22,
    MSG_CLIENT_RECONNECT=23,
    MSG_CLIENT_REQUEST=24,
    MSG_CLIENT_REQUEST_FORWARD=25,
    MSG_CLIENT_REPLY=26,
    MSG_CLIENT_CAPS=0x310,
    MSG_CLIENT_LEASE=0x311,
    MSG_CLIENT_SNAP=0x312,
    MSG_CLIENT_CAPRELEASE=0x313,
    MSG_POOLOP_REPLY=48,
    MSG_POOLOP=49,
    MSG_OSD_MAP=41,
    MSG_OSD_OP=42,
    MSG_OSD_OPREPLY=43,
    MSG_WATCH_NOTIFY=44,
    MSG_MON_ELECTION=65,
    MSG_MON_PAXOS=66,
    MSG_MON_PROBE=67,
    MSG_MON_JOIN=68,
    MSG_MON_SYNC=69,
    MSG_MON_COMMAND=50,
    MSG_MON_COMMAND_ACK=51,
    MSG_LOG=52,
    MSG_LOGACK=53,
    MSG_CLASS=56,
    MSG_CLASS_ACK=57,
    MSG_GETPOOLSTATS=58,
    MSG_GETPOOLSTATSREPLY=59,
    MSG_MON_GLOBAL_ID=60,
    MSG_ROUTE=47,
    MSG_FORWARD=46,
    MSG_PAXOS=40,
    MSG_OSD_PING=70,
    MSG_OSD_BOOT=71,
    MSG_OSD_FAILURE=72,
    MSG_OSD_ALIVE=73,
    MSG_OSD_MARK_ME_DOWN=74,
    MSG_OSD_SUBOP=76,
    MSG_OSD_SUBOPREPLY=77,
    MSG_OSD_PGTEMP=78,
    MSG_OSD_PG_NOTIFY=80,
    MSG_OSD_PG_QUERY=81,
    MSG_OSD_PG_SUMMARY=82,
    MSG_OSD_PG_LOG=83,
    MSG_OSD_PG_REMOVE=84,
    MSG_OSD_PG_INFO=85,
    MSG_OSD_PG_TRIM=86,
    MSG_PGSTATS=87,
    MSG_PGSTATSACK=88,
    MSG_OSD_PG_CREATE=89,
    MSG_REMOVE_SNAPS=90,
    MSG_OSD_SCRUB=91,
    MSG_OSD_PG_MISSING=92,
    MSG_OSD_REP_SCRUB=93,
    MSG_OSD_PG_SCAN=94,
    MSG_OSD_PG_BACKFILL=95,
    MSG_COMMAND=97,
    MSG_COMMAND_REPLY=98,
    MSG_OSD_BACKFILL_RESERVE=99,
    MSG_OSD_RECOVERY_RESERVE=150,
    MSG_MDS_BEACON=100,
    MSG_MDS_SLAVE_REQUEST=101,
    MSG_MDS_TABLE_REQUEST=102,
    MSG_MDS_RESOLVE=0x200,
    MSG_MDS_RESOLVEACK=0x201,
    MSG_MDS_CACHEREJOIN=0x202,
    MSG_MDS_DISCOVER=0x203,
    MSG_MDS_DISCOVERREPLY=0x204,
    MSG_MDS_INODEUPDATE=0x205,
    MSG_MDS_DIRUPDATE=0x206,
    MSG_MDS_CACHEEXPIRE=0x207,
    MSG_MDS_DENTRYUNLINK=0x208,
    MSG_MDS_FRAGMENTNOTIFY=0x209,
    MSG_MDS_OFFLOAD_TARGETS=0x20a,
    MSG_MDS_DENTRYLINK=0x20c,
    MSG_MDS_FINDINO=0x20d,
    MSG_MDS_FINDINOREPLY=0x20e,
    MSG_MDS_OPENINO=0x20f,
    MSG_MDS_OPENINOREPLY=0x210,
    MSG_MDS_LOCK=0x300,
    MSG_MDS_INODEFILECAPS=0x301,
    MSG_MDS_EXPORTDIRDISCOVER=0x449,
    MSG_MDS_EXPORTDIRDISCOVERACK=0x450,
    MSG_MDS_EXPORTDIRCANCEL=0x451,
    MSG_MDS_EXPORTDIRPREP=0x452,
    MSG_MDS_EXPORTDIRPREPACK=0x453,
    MSG_MDS_EXPORTDIRWARNING=0x454,
    MSG_MDS_EXPORTDIRWARNINGACK=0x455,
    MSG_MDS_EXPORTDIR=0x456,
    MSG_MDS_EXPORTDIRACK=0x457,
    MSG_MDS_EXPORTDIRNOTIFY=0x458,
    MSG_MDS_EXPORTDIRNOTIFYACK=0x459,
    MSG_MDS_EXPORTDIRFINISH=0x460,
    MSG_MDS_EXPORTCAPS=0x470,
    MSG_MDS_EXPORTCAPSACK=0x471,
    MSG_MDS_HEARTBEAT=0x500,
    MSG_TIMECHECK=0x600,
    MSG_MON_HEALTH=0x601
} msg_ctype;

static value_string msg_ctype_values[]={
    {MSG_SHUTDOWN, "Shutdown"},
    {MSG_PING, "Ping"},
    {MSG_MON_MAP, "MON Map"},
    {MSG_MON_GET_MAP, "MON Get Map"},
    {MSG_STATFS, "Stat FS"},
    {MSG_STATFS_REPLY, "Stat FS Reply"},
    {MSG_MON_SUBSCRIBE, "MON Subscribe"},
    {MSG_MON_SUBSCRIBE_ACK, "MON Subscribe Ack"},
    {MSG_AUTH, "Auth"},
    {MSG_AUTH_REPLY, "Auth Reply"},
    {MSG_MON_GET_VERSION, "MON Get Version"},
    {MSG_MON_GET_VERSION_REPLY, "MON Get Version Reply"},
    {MSG_MDS_MAP, "MDS Map"},
    {MSG_CLIENT_SESSION, "Client Session"},
    {MSG_CLIENT_RECONNECT, "Client Reconnect"},
    {MSG_CLIENT_REQUEST, "Client Request"},
    {MSG_CLIENT_REQUEST_FORWARD, "Client Request Forward"},
    {MSG_CLIENT_REPLY, "Client Reply"},
    {MSG_PAXOS, "Paxos"},
    {MSG_OSD_MAP, "OSD Map"},
    {MSG_OSD_OP, "OSD Op"},
    {MSG_OSD_OPREPLY, "OSD Op Reply"},
    {MSG_WATCH_NOTIFY, "Watch Notify"},
    {MSG_FORWARD, "Forward"},
    {MSG_ROUTE, "Route"},
    {MSG_POOLOP_REPLY, "Pool Op Reply"},
    {MSG_POOLOP, "Pool Op"},
    {MSG_MON_COMMAND, "MON Command"},
    {MSG_MON_COMMAND_ACK, "MON Command Ack"},
    {MSG_LOG, "Log"},
    {MSG_LOGACK, "Log Ack"},
    {MSG_CLASS, "Class"},
    {MSG_CLASS_ACK, "Class Ack"},
    {MSG_GETPOOLSTATS, "Get Pool Stats"},
    {MSG_GETPOOLSTATSREPLY, "Get Pools Stats Reply"},
    {MSG_MON_GLOBAL_ID, "MON Global Id"},
    {MSG_MON_ELECTION, "MON Election"},
    {MSG_MON_PAXOS, "MON Paxos"},
    {MSG_MON_PROBE, "MON Probe"},
    {MSG_MON_JOIN, "MON Join"},
    {MSG_MON_SYNC, "MON Sync"},
    {MSG_OSD_PING, "OSD Ping"},
    {MSG_OSD_BOOT, "OSD Boot"},
    {MSG_OSD_FAILURE, "OSD Failure"},
    {MSG_OSD_ALIVE, "OSD Alive"},
    {MSG_OSD_MARK_ME_DOWN, "OSD Mark Me Down"},
    {MSG_OSD_SUBOP, "OSD Subop"},
    {MSG_OSD_SUBOPREPLY, "OSD Subop Reply"},
    {MSG_OSD_PGTEMP, "OSD PG Temp"},
    {MSG_OSD_PG_NOTIFY, "OSD PG Notify"},
    {MSG_OSD_PG_QUERY, "OSD PG Query"},
    {MSG_OSD_PG_SUMMARY, "OSD PG Summary"},
    {MSG_OSD_PG_LOG, "OSD PG Log"},
    {MSG_OSD_PG_REMOVE, "OSD PG Remove"},
    {MSG_OSD_PG_INFO, "OSD PG Info"},
    {MSG_OSD_PG_TRIM, "OSD PG Trim"},
    {MSG_PGSTATS, "MPN PG Stats"},
    {MSG_PGSTATSACK, "MON PG Stats Ack"},
    {MSG_OSD_PG_CREATE, "OSD PG Create"},
    {MSG_REMOVE_SNAPS, "Remove Snaps"},
    {MSG_OSD_SCRUB, "OSD Scrub"},
    {MSG_OSD_PG_MISSING, "OSD PG Missing"},
    {MSG_OSD_REP_SCRUB, "OSD REP Scrub"},
    {MSG_OSD_PG_SCAN, "OSD PG Scan"},
    {MSG_OSD_PG_BACKFILL, "OSD PG Backfill"},
    {MSG_COMMAND, "Command"},
    {MSG_COMMAND_REPLY, "Command Reply"},
    {MSG_OSD_BACKFILL_RESERVE, "OSD Backfill Reserve"},
    {MSG_MDS_BEACON, "MDS Beacon"},
    {MSG_MDS_SLAVE_REQUEST, "MDS Slave Request"},
    {MSG_MDS_TABLE_REQUEST, "MDS Table Request"},
    {MSG_OSD_RECOVERY_RESERVE, "OSD Recovery Reserve"},
    {MSG_MDS_RESOLVE, "MDS Resolve"},
    {MSG_MDS_RESOLVEACK, "MDS Resolve Ack"},
    {MSG_MDS_CACHEREJOIN, "MDS Cache Rejoin"},
    {MSG_MDS_DISCOVER, "MDS Discover"},
    {MSG_MDS_DISCOVERREPLY, "MDS Discover Reply"},
    {MSG_MDS_INODEUPDATE, "MDS INode Update"},
    {MSG_MDS_DIRUPDATE, "MDS Dir Update"},
    {MSG_MDS_CACHEEXPIRE, "MDS Cache Expire"},
    {MSG_MDS_DENTRYUNLINK, "MDS DEntry Unlink"},
    {MSG_MDS_FRAGMENTNOTIFY, "MDS Fragment Notify"},
    {MSG_MDS_OFFLOAD_TARGETS, "MDS Offload Targets"},
    {MSG_MDS_DENTRYLINK, "MDS DEntry Link"},
    {MSG_MDS_FINDINO, "MDS Find INO"},
    {MSG_MDS_FINDINOREPLY, "MDS Find INO Reply"},
    {MSG_MDS_OPENINO, "MDS Open INO"},
    {MSG_MDS_OPENINOREPLY, "MDS Open INO Reply"},
    {MSG_MDS_LOCK, "MDS Lock"},
    {MSG_MDS_INODEFILECAPS, "MDS INODE File Caps"},
    {MSG_CLIENT_CAPS, "Client Caps"},
    {MSG_CLIENT_LEASE, "Client Lease"},
    {MSG_CLIENT_SNAP, "Client Snap"},
    {MSG_CLIENT_CAPRELEASE, "Client Cap Release"},
    {MSG_MDS_EXPORTDIRDISCOVER, "MDS Export DIR Discover"},
    {MSG_MDS_EXPORTDIRDISCOVERACK, "MDS Export DIR Discover Ack"},
    {MSG_MDS_EXPORTDIRCANCEL, "MDS Export DIR Cancel"},
    {MSG_MDS_EXPORTDIRPREP, "MDS Export DIR Prep"},
    {MSG_MDS_EXPORTDIRPREPACK, "MDS Export DIR Prepack"},
    {MSG_MDS_EXPORTDIRWARNING, "MDS Export DIR Warning"},
    {MSG_MDS_EXPORTDIRWARNINGACK, "MDS Export DIR Warning Ack"},
    {MSG_MDS_EXPORTDIR, "MDS Export DIR"},
    {MSG_MDS_EXPORTDIRACK, "MDS Export DIR Ack"},
    {MSG_MDS_EXPORTDIRNOTIFY, "MDS Export DIR Notify"},
    {MSG_MDS_EXPORTDIRNOTIFYACK, "MDS Export DIR Notify Ack"},
    {MSG_MDS_EXPORTDIRFINISH, "MDS Export DIR Finish"},
    {MSG_MDS_EXPORTCAPS, "MDS Export Caps"},
    {MSG_MDS_EXPORTCAPSACK, "MDS Export Caps Ack"},
    {MSG_MDS_HEARTBEAT, "MDS Heartbeat"},
    {MSG_TIMECHECK, "Timecheck"},
    {MSG_MON_HEALTH, "MON Health"}
};

static gboolean
isMsgCType(guint32 type) {
    return (try_val_to_str(type, msg_ctype_values)!=NULL);
}

static const char* 
msgCTypeDescription(msg_ctype type) {
	return val_to_str(type, msg_ctype_values, "Unknown Message Type: %d");
}

/*
 * PaxosServiceMessage
 *
struct {
    guint64 version;
    gint16 deprecated_session_mon;
    guint64 deprecated_session_mon_tid;
}
*/
#define SIZE_OF_PAXOS    (8+2+8)   

static guint32 
dissect_paxos(tvbuff_t *tvb, proto_tree *tree, guint32 offset){

    proto_tree_add_item(tree, hf_paxos_version, tvb, offset, 8, TRUE);
    return SIZE_OF_PAXOS;
}

/*
 * feature_set
 *
struct {
    uint64_t mask;
    map <uint64_t,string> names;
}
*/
static guint32 
dissect_featureset(tvbuff_t *tvb, proto_tree *tree, guint32 offset, int hinfo, 
        gint ett) {
    proto_item *litem;
    proto_tree *ltree;
    guint32 i, items;
    guint32 at;

    litem = proto_tree_add_item( tree, hinfo, tvb, offset, 1, TRUE );
    ltree = proto_item_add_subtree(litem, ett);

    at = offset;
    proto_tree_add_item(ltree, hf_featureset_mask, tvb, at, 8, TRUE); at += 8;
    items = tvb_get_letohl(tvb, at); at+=4;
    for (i=0; i<items; i++) {
        proto_tree_add_item(ltree, hf_featureset_id, tvb, at, 8, TRUE);    at += 8;
        proto_tree_add_item(ltree, hf_featureset_name, tvb, at, 4, TRUE);
        at += tvb_get_letohl(tvb, at)+ 4;
    }

    proto_item_set_len(litem, (at-offset));
    return (at-offset);
}

static guint32 
dissect_compatset(tvbuff_t *tvb, proto_tree *tree, guint32 offset) {

    guint32 at;

    at = offset;
    at +=dissect_featureset(tvb, tree, at, hf_compatset_compat, ett_compat);
    at +=dissect_featureset(tvb, tree, at, hf_compatset_rocompat, ett_rocompat);
    at +=dissect_featureset(tvb, tree, at, hf_compatset_incompat, ett_incompat);

    return (at-offset);
}

/*
 * MAuth
 *
 struct {
    paxos paxos;
    guint32 protocol;
    guint32 auth_len;
    guint8 auth[];
    guint32 monmap_epoch;
}
*/
#define SIZE_OF_MSG_AUTH    (SIZE_OF_PAXOS+4+4+0+4)   

static guint32 
dissect_auth(tvbuff_t *tvb, proto_tree *tree, guint32 offset){
    proto_item *auth_item;
    proto_tree *auth_tree;
    gint auth_len;
    guint8* auth_bytes;

    auth_len = tvb_get_letohl(tvb, offset+SIZE_OF_PAXOS+4);

    if (tree) {
        auth_item = proto_tree_add_item( tree, hf_auth, tvb, offset, 
            SIZE_OF_MSG_AUTH+auth_len, TRUE );
        auth_tree = proto_item_add_subtree(auth_item, ett_front);

        offset += dissect_paxos(tvb, auth_tree, offset);
        proto_tree_add_item(auth_tree, hf_auth_protocol, tvb, offset, 4, TRUE);
        proto_tree_add_item(auth_tree, hf_auth_authlen, tvb, offset+4, 4, TRUE);
        auth_bytes = (guint8*)tvb_memdup(tvb, offset+8, auth_len);
        proto_tree_add_bytes(auth_tree, hf_auth_authbytes, tvb, offset+8, 
            auth_len, auth_bytes);
        g_free(auth_bytes);
        proto_tree_add_item(auth_tree, hf_auth_monmapepoch, tvb, 
            offset+8+auth_len, 4, TRUE);
    }
    return SIZE_OF_MSG_AUTH+auth_len;
}

/*
 * msg_auth_reply
 *
 struct ceph_msg_auth_reply {
    guint32 protocol;
    gint32 result;
    guint64 global_id;
    guint32 auth_len;
    guint8 auth[];
    guint32 msg_len;
    char msg[];
}
*/
#define SIZE_OF_MSG_AUTH_REPLY    (4+4+8+4+0+4+0)   

static guint32 
dissect_auth_reply(tvbuff_t *tvb, proto_tree *tree, guint32 offset){
    proto_item *auth_item;
    proto_tree *auth_tree;
    gint auth_len;
    guint8* auth_bytes;
    gint msg_len;
    char* msg_string;

    auth_len = tvb_get_letohl(tvb, offset+16);
    msg_len = tvb_get_letohl(tvb, offset+20+auth_len);

    if (tree) {
        auth_item = proto_tree_add_item( tree, hf_authreply, tvb, offset, 
            SIZE_OF_MSG_AUTH_REPLY+auth_len+msg_len, TRUE );
        auth_tree = proto_item_add_subtree(auth_item, ett_front);

        proto_tree_add_item(auth_tree, hf_authreply_protocol, tvb, offset+0, 
            4, TRUE);
        proto_tree_add_item(auth_tree, hf_authreply_result, tvb, offset+4, 
            4, TRUE);
        proto_tree_add_item(auth_tree, hf_authreply_globalid, tvb, offset+8, 
            8, TRUE);
        proto_tree_add_item(auth_tree, hf_authreply_authlen, tvb, offset+16, 
            4, TRUE);
        
        auth_bytes = (guint8*)tvb_memdup(tvb, offset+20, auth_len);
        proto_tree_add_bytes(auth_tree, hf_authreply_authbytes, tvb, offset+20, 
            auth_len, auth_bytes);
        g_free(auth_bytes);
        
        proto_tree_add_item(auth_tree, hf_authreply_msglen, tvb, 
            offset+20+auth_len, 4, TRUE);
        msg_string = (char*)tvb_memdup(tvb, offset+20+auth_len+4, msg_len);
        proto_tree_add_string(auth_tree, hf_authreply_msgstring, tvb, 
            offset+20+auth_len+4, msg_len, msg_string);
        g_free(msg_string);
    }
    return SIZE_OF_MSG_AUTH_REPLY+auth_len+msg_len;
}

/*
 * msg_mon_map
 *
 struct ceph_msg_monmap {
    guint32 length;
    guint8 version;
    guint8 compat;
    guint32 entry_size;
    ceph_msg_monmap_entry entries[];
 };

 struct ceph_msg_monmap_entry {
    guint8[16] fsid
    guint32 epoch
    map<string,entity_addr_t> mon_addr
    guint64 last_changed
    guint64 created
}
*/

static guint32 
dissect_mon_map(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        guint32 offset) {
    proto_item *litem = NULL;
    proto_tree *ltree = NULL;
    guint32 i, at, name_at;
    guint32 map_len, addr_len, entries_len;
    entity_info einfo;

    map_len = tvb_get_letohl(tvb, offset)+sizeof(map_len);

    litem = proto_tree_add_item( tree, hf_monmap, tvb, offset, map_len, TRUE );
    ltree = proto_item_add_subtree(litem, ett_front);

    proto_tree_add_item(ltree, hf_monmap_version, tvb, offset+4, 1, TRUE);
    proto_tree_add_item(ltree, hf_monmap_compat, tvb, offset+5, 1, TRUE);

    entries_len = tvb_get_letohl(tvb, offset+6);
    offset += 10;
    while (entries_len>0) {
        at = offset;
        at += dissect_fsid(tvb, ltree, at);
        proto_tree_add_item(ltree, hf_monmap_epoch, tvb, at, 4, TRUE); at+=4;

        addr_len = tvb_get_letohl(tvb, at); at+=4;
        for (i=0; i< addr_len; i++) {
            name_at = at;
            proto_tree_add_item(ltree, hf_monmap_name, tvb, at, 4, TRUE); 
            at += tvb_get_letohl(tvb, at)+ 4;
            at += dissect_entity_addr(tvb, pinfo, ltree, at, &einfo);
            conv_set_entity_name(conv_get_entity_frominfo(&einfo),
                tvb, name_at, ENTITY_TYPE_MON);
        }

        proto_tree_add_item(ltree, hf_monmap_lastchanged, tvb, at, 8, TRUE); 
        at+=8;
        proto_tree_add_item(ltree, hf_monmap_created, tvb, at, 8, TRUE); 
        at+=8;

        entries_len -= (at-offset);
        offset = at;
    }
    if (entries_len !=0 )
        expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
            "Length mismatch in decoding mon map entries");

    return map_len;
}

/*
 * msg_mon_subscribe
 *
struct ceph_mon_subscribe_item {
    guint64 start;
    guint8 flags;
};

struct ceph_msg_monsubscribe {
    map<string, ceph_mon_subscribe_item> what
};
*/
static guint32 
dissect_mon_subscribe(tvbuff_t *tvb, proto_tree *tree, guint32 offset, 
        guint32 length) {
    proto_item *litem = NULL;
    proto_tree *ltree = NULL;
    guint32 i, items;
    guint32 at;

    litem = proto_tree_add_item( tree, hf_monsubscribe, tvb, offset, 
        length, TRUE );
    ltree = proto_item_add_subtree(litem, ett_front);

    items = tvb_get_letohl(tvb, offset);
    at = offset +4;
    for (i=0; i< items; i++) {
        proto_tree_add_item(ltree, hf_monsubscribe_name, tvb, at, 4, TRUE); 
        at += tvb_get_letohl(tvb, at)+ 4;

        proto_tree_add_item(ltree, hf_monsubscribe_start, tvb, at, 8, TRUE); 
        at += 8;
        proto_tree_add_item(ltree, hf_monsubscribe_flags, tvb, at, 1, TRUE); 
        at += 1;
    }

    return (at-offset);
}

/*
 * msg_mon_subscribeack
 *
struct ceph_msg_mon_subscribeack {
    guint32 interval;
    guint8 fsid[16];
};
*/
static guint32 
dissect_mon_subscribeack(tvbuff_t *tvb, proto_tree *tree, guint32 offset, 
        guint32 length) {
    proto_item *litem = NULL;
    proto_tree *ltree = NULL;

    litem = proto_tree_add_item( tree, hf_monsubscribeack, tvb, offset, 
        length, TRUE );
    ltree = proto_item_add_subtree(litem, ett_front);

    proto_tree_add_item(ltree, hf_monsubscribeack_interval, tvb, offset, 
        4, TRUE); 
    dissect_fsid(tvb, ltree, offset+4);
    return 20;
}

/*
 * mds_beacon
 *
struct mds_beacon {
    paxos
    guint8[16] fsid;
    guint64 global_id;
    guint32 state;
    guint64 seq;
    string name;
    gint32 standby_for_rank;
    string standby_for_name;
    CompatSet compat;
};
*/
static guint32 
dissect_mdsbeacon(tvbuff_t *tvb, proto_tree *tree, 
        guint32 offset, guint32 length) {
    proto_item *litem = NULL;
    proto_tree *ltree = NULL;
    guint32 at;

    litem = proto_tree_add_item( tree, hf_mdsbeacon, tvb, offset, length, TRUE);
    ltree = proto_item_add_subtree(litem, ett_front);

    at = offset;
    at += dissect_paxos(tvb, ltree, at);
    at += dissect_fsid(tvb, ltree, at);
    proto_tree_add_item(ltree, hf_mdsbeacon_globalid, tvb, at, 8, TRUE); at+=8;
    proto_tree_add_item(ltree, hf_mdsbeacon_state, tvb, at, 4, TRUE); at+=4;
    proto_tree_add_item(ltree, hf_mdsbeacon_seq, tvb, at, 8, TRUE); at+=8;
    proto_tree_add_item(ltree, hf_mdsbeacon_name, tvb, at, 4, TRUE); 
    at += tvb_get_letohl(tvb, at)+ 4;
    proto_tree_add_item(ltree, hf_mdsbeacon_standbyforrank, tvb, at, 4, TRUE);
    at+=4; 
    proto_tree_add_item(ltree, hf_mdsbeacon_standbyforname, tvb, at, 4, TRUE); 
    at += tvb_get_letohl(tvb, at)+ 4;
    at += dissect_compatset(tvb, ltree, at);

    return (at-offset);
}

/*
 * Pull apart a contained message.
 * Dissectors called from here don't need to length check just return how
 * many bytes they actually consumed.
 */
static guint
dissect_msg_ctype(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        int offset, msg_ctype type, guint32 length)
{
    tvbuff_t* ltvb;
    guint32 len=0;

    /* Create a subset to isolate the dissectors 
    so they don't really need to length check */
     
    ltvb = tvb_new_subset_length(tvb, offset, length);

    switch (type) {
        case MSG_MON_MAP:
            len = dissect_mon_map(ltvb, pinfo, tree, 0);
            break;
        case MSG_AUTH:
            len = dissect_auth(ltvb, tree, 0);
            break;
        case MSG_AUTH_REPLY:    
            len = dissect_auth_reply(ltvb, tree, 0);
            break;
        case MSG_MON_SUBSCRIBE:
            len = dissect_mon_subscribe(ltvb, tree, 0, length);
            break;
        case MSG_MON_SUBSCRIBE_ACK:
            len = dissect_mon_subscribeack(ltvb, tree, 0, length);
            break;
        case MSG_MDS_BEACON:
            len = dissect_mdsbeacon(ltvb, tree, 0, length);
            break;
        default:
            break;
    }
    return len;
}

static guint 
dissect_msg(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, int offset) {
    guint start;
    gint len=0;
    header_info info;

    start = offset;
    len = dissect_msg_header(tvb, tree, offset, &info);
    if (len <0 ) return len;
    offset += len;

    if (info.front_len) {
        if ((guint)tvb_reported_length_remaining (tvb, offset) < info.front_len) 
            return -1;

        len = dissect_msg_ctype(tvb, pinfo, tree, offset, 
                (msg_ctype)info.type, info.front_len);
        if (len <0 ) return len;
        else if (len == 0) 
            expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
                "Undecoded: %s", msgCTypeDescription((msg_ctype)info.type));
        else if ((guint32)len != info.front_len)
            expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
                "Header front length (%u) does not match message length (%u)", 
                info.front_len, len);
        offset += info.front_len;
    }

    offset += info.middle_len;
    offset += info.data_len;
    len = dissect_footer(tvb, tree, offset);
    if (len <0 ) return len;

    offset += len;
    setPDUInfo(pinfo, msgCTypeDescription((msg_ctype)info.type));
    return (offset-start);
}

/*
 * Create an expert warning that part of the packet could not be decoded.
 */
static void 
undecodedWarning(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
    guint32 offset, guint32 expecting) {

    /*
     * Post warning message
     */
    setPDUInfo(pinfo, "Undecoded data");
    if ((expecting & SERVER_BANNER) != 0)
        expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
        "Expected server to send banner, ignoring packet");
    if ((expecting & CLIENT_BANNER) != 0)
        expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
        "Expected client to send banner, ignoring packet");
    if ((expecting & SERVER_ADDRESS) != 0)
        expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
        "Expected server to send addresses, ignoring packet");
    if ((expecting & CLIENT_ADDRESS) != 0)
        expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
        "Expected client to send addresses, ignoring packet");
    if ((expecting & CONNECT_REQUEST) != 0)
        expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
        "Expected client to send connection request, ignoring packet");
    if ((expecting & CONNECT_REPLY) != 0)
        expert_add_info_format(pinfo, tree, PI_UNDECODED, PI_WARN, 
        "Expected server to send connection reply, ignoring packet");

    /*
     * Assume we jumped into middle of conversation 
     */
    conv_tag(pinfo, tvb, offset, CONVERSING);
}

static void dissect_ceph(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree){

      proto_item *ceph_item = NULL;
    proto_tree *ceph_tree = NULL;
    guint pduNum;
    guint offset;
    gint32 len;
    guint32 expecting;
    guint32 needMore;
    guint8 type;

    /* Tag as CEPH & prep info */
    if (check_col(pinfo->cinfo, COL_PROTOCOL))
        col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
    if(check_col(pinfo->cinfo,COL_INFO)){
        col_clear(pinfo->cinfo,COL_INFO);
        col_add_fstr(pinfo->cinfo, COL_INFO, "[%s]",conv_display(pinfo));
        col_set_fence(pinfo->cinfo,COL_INFO);
    }
    
    /*
     * Loop reading PDUs
     */
    needMore = FALSE;
    offset=0;
    pduNum=0;
    while (tvb_reported_length_remaining (tvb, offset) != 0) {

        /* Flag if this is first PDU in packet, helps formating info */
        g_firstPdu = (pduNum++ == 0);

        /* Create a new tree for it */
        if (tree) { 
            ceph_item = proto_tree_add_item(tree, g_proto_ceph, tvb, 0, 
                -1, TRUE);
            ceph_tree = proto_item_add_subtree(ceph_item, ett_ceph);
        } else {
            ceph_tree = NULL;
        }
        
        /*
         * Recover what type of message we are expecting to see both for 
         * error checking and guiding the dissector to do the right thing
         */
        expecting = conv_expecting(pinfo, tvb, offset);
        
        /*
         * Try decode something
         */
        if ((expecting & SERVER_BANNER) != 0) {
            len = dissect_banner(tvb, pinfo, ceph_tree, offset, FALSE);
            if (len == -1) needMore = TRUE;
            else if (len>0) {
                conv_tag(pinfo, tvb, offset, SERVER_BANNER);
                offset += len;
                continue;
            }
        } 
        
        if ((expecting & CLIENT_BANNER) != 0) {
            len = dissect_banner(tvb, pinfo, ceph_tree, offset, TRUE);
            if (len == -1) needMore = TRUE;
            else if (len>0) {
                conv_tag(pinfo, tvb, offset, CLIENT_BANNER);
                offset += len;
                continue;
            }
        }

        if ((expecting & CLIENT_ADDRESS) != 0) {
            len = dissect_client_address(tvb, pinfo, ceph_tree, offset);
            if (len == -1) needMore = TRUE;
            else if (len>0) {
                conv_tag(pinfo, tvb, offset, CLIENT_ADDRESS);
                offset += len;
                continue;
            }
        }

        if ((expecting & SERVER_ADDRESS) != 0) {
            len = dissect_server_address(tvb, pinfo, ceph_tree, offset);
            if (len == -1) needMore = TRUE;
            else if (len>0) {
                conv_tag(pinfo, tvb, offset, SERVER_ADDRESS);
                offset += len;
                continue;
            }
        }

        if ((expecting & CONNECT_REQUEST) != 0) {
            len = dissect_connect_request(tvb, pinfo, ceph_tree, offset);
            if (len == -1) needMore = TRUE;
            else if (len>0) {
                conv_tag(pinfo, tvb, offset, CONNECT_REQUEST);
                offset += len;
                continue;
            }
        }

        if ((expecting & CONNECT_REPLY) != 0) {
            len = dissect_connect_reply(tvb, pinfo, ceph_tree, offset);
            if (len == -1) needMore = TRUE;
            else if (len>0) {
                conv_tag(pinfo, tvb, offset, CONNECT_REPLY);
                offset += len;
                continue;
            }
        }

        if ((expecting & CONVERSING) != 0) {
            type = tvb_get_guint8(tvb, offset);

            if(type == MSGR_TAG_CLOSE){
                setPDUInfo(pinfo, "Close");
                conv_tag(pinfo, tvb, offset, CONVERSING);
                offset++;
            }
            else if (type == MSGR_TAG_KEEPALIVE){
                setPDUInfo(pinfo, "Keep Alive");
                conv_tag(pinfo, tvb, offset, CONVERSING);
                offset++;
            } 
            else if (type == MSGR_TAG_ACK) {
                len = dissect_ack(tvb, pinfo, ceph_tree, offset);
                if (len == -1) needMore = TRUE;
                if (len>0) {
                    conv_tag(pinfo, tvb, offset, CONVERSING);
                    offset += len;
                    continue;
                }
            }
            else if (type == MSGR_TAG_MSG) {
                len = dissect_msg(tvb, pinfo, ceph_tree, offset+1);
                if (len == -1) needMore = TRUE;
                if (len>0) {
                    conv_tag(pinfo, tvb, offset+1, CONVERSING);
                    offset += (1+len);
                    continue;
                }
            }
        }

        /* We have fallen through without decoding something */
        if (needMore) {
            /* One of decoders wants to see more data, so request it */
            pinfo->desegment_offset = offset;
            pinfo->desegment_len=DESEGMENT_ONE_MORE_SEGMENT; 
        } else {
            /* Throw data away, we don't know what it is */
            undecodedWarning(tvb, pinfo, ceph_tree, offset, expecting);
        }
        return;
    }
}

/****************************************************************************
 * Initialisation code
 ****************************************************************************/

static gboolean
dissect_ceph_heur(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, 
        void *data) {

    conversation_t* conversation;
    (void)data; /* unused but required */
    
    if (tvb_memeql(tvb,0, "ceph",4)!=0) {
        if (tvb_reported_length_remaining (tvb, 0) >= 1+SIZE_OF_MSG_HEADER) {
            if (!isMsgrTag(tvb_get_guint8(tvb, 0)))
                return FALSE;

            if (!isEntityType(tvb_get_guint8(tvb, 1+HEADER_ETYPE_OFFSET)))
                return FALSE;

            if (!isMsgCType(tvb_get_letohs(tvb, 1+HEADER_TYPE_OFFSET)))
                return FALSE;


        } else {
            return FALSE;
        }
    }

    /* This is for us */
    conversation = find_or_create_conversation(pinfo);
    conversation_set_dissector(conversation, g_ceph_handle);
    dissect_ceph(tvb, pinfo, tree);
    return TRUE;
}

/*
 * Create the dissector, this is called once after proto_register_ceph
 * has been invoked.
 * Note. the dissector is only valid on a range of ports, you can
 * get around this by writing a 'heuristic' detector but that is
 * rather more complex to do.
 */
void proto_reg_handoff_ceph(void)
{
    g_ceph_handle = create_dissector_handle(dissect_ceph, g_proto_ceph);
    heur_dissector_add("tcp", dissect_ceph_heur, g_proto_ceph);
}

/*
 * This is the first part of the two-part registration for the plugin,
 * see proto_reg_handoff_ceph() for part 2. 
 */
void proto_register_ceph (void)
{
    /* A header field is something you can search/filter on.
    * 
    * We create a structure to register our fields. It consists of an
    * array of hf_register_info structures, each of which are of the format
    * {&(field id), {name, abbrev, type, display, strings, bitmask, blurb, HFILL}}.
    */
    static hf_register_info hf[] = {
        { &hf_paxos_version,
            { "version", "ceph.paxosservicemessage.version", FT_UINT64 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_featureset_mask,
            { "mask", "ceph.featureset.mask", FT_UINT64 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_featureset_id,
            { "id", "ceph.featureset.id", FT_UINT64 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_featureset_name,
            { "name", "ceph.featureset.name", FT_UINT_STRING , 
                BASE_NONE, NULL, 0, NULL, HFILL }},
        { &hf_compatset_compat,
            { "Compat", "ceph.compat", FT_NONE , 
                BASE_NONE, NULL, 0, NULL, HFILL }},
        { &hf_compatset_rocompat,
            { "RO Compat", "ceph.rocompat", FT_NONE , 
                BASE_NONE, NULL, 0, NULL, HFILL }},
        { &hf_compatset_incompat,
            { "Incompat", "ceph.incompat", FT_NONE , 
                BASE_NONE, NULL, 0, NULL, HFILL }},
        { &hf_monmap,
            { "MON map", "ceph.monmap", FT_NONE , 
                BASE_NONE, NULL, 0, NULL, HFILL }},
        { &hf_monmap_version,
            { "version", "ceph.monmap.version", FT_UINT8 , 
                BASE_DEC, NULL, 0, NULL, HFILL }},
        { &hf_monmap_compat,
            { "compat", "ceph.monmap.compat", FT_UINT8 , 
                BASE_DEC, NULL, 0, NULL, HFILL }},
        { &hf_monmap_epoch,
            { "epoch", "ceph.monmap.epoch", FT_UINT32 , 
                BASE_DEC, NULL, 0, NULL, HFILL }},
        { &hf_monmap_name,
            { "name", "ceph.monmap.name", FT_UINT_STRING , 
                BASE_NONE, NULL, 0, NULL, HFILL }},
        { &hf_monmap_lastchanged,
            { "last changed", "ceph.monmap.lastchanged", FT_UINT64 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_monmap_created,
            { "created", "ceph.monmap.created", FT_UINT64 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_monsubscribe,
            { "MON subscribe", "ceph.monsubscribe", FT_NONE , 
                BASE_NONE, NULL, 0, NULL, HFILL }},
        { &hf_monsubscribe_name,
            { "name", "ceph.monsubscribe.name", FT_UINT_STRING , 
                BASE_NONE, NULL, 0, NULL, HFILL }},
        { &hf_monsubscribe_start,
            { "start", "ceph.monsubscribe.start", FT_UINT64 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_monsubscribe_flags,
            { "flags", "ceph.monsubscribe.flags", FT_UINT8 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_monsubscribeack,
            { "MON subscribe ack", "ceph.monsubscribeack", FT_NONE , 
                BASE_NONE, NULL, 0,    NULL, HFILL }},
        { &hf_monsubscribeack_interval,
            { "interval", "ceph.monsubscribeack.interval", FT_UINT32 , 
                BASE_DEC, NULL, 0, NULL, HFILL }},
        { &hf_mdsbeacon,
            { "MDS Beacon", "ceph.mdsbeacon", FT_NONE , 
                BASE_NONE, NULL, 0,    NULL, HFILL }},
        { &hf_mdsbeacon_globalid,
            { "global_id", "ceph.mdsbeason.globalid", FT_UINT64 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_mdsbeacon_state,
            { "state", "ceph.mdsbeason.state", FT_UINT32 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_mdsbeacon_seq,
            { "seq", "ceph.mdsbeason.seq", FT_UINT64 , 
                BASE_DEC, NULL, 0, NULL, HFILL }},
        { &hf_mdsbeacon_name,
            { "name", "ceph.mdsbeason.name", FT_UINT_STRING , 
                BASE_NONE, NULL, 0,    NULL, HFILL }},
        { &hf_mdsbeacon_standbyforrank,
            { "standby for rank", "ceph.mdsbeason.standbyforrank", FT_INT32 , 
                BASE_DEC, NULL, 0, NULL, HFILL }},
        { &hf_mdsbeacon_standbyforname,
            { "standby for name", "ceph.mdsbeason.standbyforname", FT_UINT_STRING , 
                BASE_NONE, NULL, 0,    NULL, HFILL }},
        { &hf_auth,
            { "Authentication Request", "ceph.auth.request", FT_NONE , 
                BASE_NONE, NULL, 0,    NULL, HFILL }},
        { &hf_auth_protocol,
            { "protocol", "ceph.auth.protocol", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_auth_authlen,
            { "auth len", "ceph.auth.authlen", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_auth_authbytes,
            { "auth bytes", "ceph.auth.authbytes", FT_BYTES, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_auth_monmapepoch,
            { "monmap epoch", "ceph.auth.monmapepoch", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_authreply,
            { "Authentication reply", "ceph.auth.reply", FT_NONE , 
                BASE_NONE, NULL, 0,    NULL, HFILL }},
        { &hf_authreply_protocol,
            { "protocol", "ceph.authreply.protocol", FT_UINT32 , 
                BASE_DEC, NULL, 0, NULL, HFILL }},
        { &hf_authreply_result,
            { "result", "ceph.authreply.result", FT_INT32 , 
                BASE_DEC, NULL, 0, NULL, HFILL }},
        { &hf_authreply_globalid,
            { "global_id", "ceph.authreply.global_id", FT_UINT64 , 
                BASE_HEX, NULL, 0, NULL, HFILL }},
        { &hf_authreply_authlen,
            { "auth len", "ceph.authreply.authlen", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_authreply_authbytes,
            { "auth bytes", "ceph.authreply.authbytes", FT_BYTES, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_authreply_msglen,
            { "msg len", "ceph.authreply.msglen", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_authreply_msgstring,
            { "msg bytes", "ceph.authreply.msgbytes", FT_STRINGZ, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_header,
            { "Header", "ceph.header", FT_NONE, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_banner,
            { "Banner", "ceph.connect.banner", FT_STRING, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_entity_type,
            { "entity type", "ceph.entity.type", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_entity_num,
            { "entity num", "ceph.entity.num", FT_UINT64, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_entity_addr,
            { "Entity Addr", "ceph.entity.addr", FT_NONE, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_banner_magic,
            { "Banner Magic", "ceph.connect.banner.magic", FT_STRING, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_banner_version,
            { "Banner Version", "ceph.connect.banner.ver", FT_STRING, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_entity_erank,
            { "erank", "ceph.entity.erank", FT_UINT32, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_entity_nonce,
            { "nonce", "ceph.entity.nonce", FT_UINT32, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_sockaddr_in,
            { "sockaddr_in", "ceph.sockaddr_in", FT_NONE, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_sin_family,
            { "sin_family", "ceph.sin_family", FT_UINT16, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_sin_port,
            { "sin_port", "ceph.sin_port", FT_UINT16, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_sin_addr,
            { "ip addr", "ceph.addr", FT_IPv4, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_features,
            { "features", "ceph.connect.features", FT_UINT64, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_host_type,
            { "host_type", "ceph.connect.host_type", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_tag,
            { "tag", "ceph.connect.tag", FT_UINT8, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_global_seq,
            { "global_seq", "ceph.connect.global_seq", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_connect_seq,
            { "connect_seq", "ceph.connect.connect_seq", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_protocol_version,
            { "protocol_version", "ceph.connect.protocol_version", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_authorizer_protocol,
            { "authorizer_protocol", "ceph.connect.authorizer_protocol", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_authorizer_len,
            { "authorizer_len", "ceph.connect.authorizer_len", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_flags,
            { "flags", "ceph.connect.flags", FT_UINT8, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_connect_authentication_key,
            { "authentication_key", "ceph.connect.authentication_key", FT_BYTES, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_tag,
            { "tag", "ceph.tag", FT_UINT8, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_seq_ack,
            { "ack seq", "ceph.ack.seq", FT_UINT64, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_seq,
            { "seq", "ceph.seq", FT_UINT64, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_tid,
            { "tid", "ceph.tid", FT_UINT64, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_type,
            { "type", "ceph.type", FT_UINT16, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_priority,
            { "priority", "ceph.priority", FT_UINT16, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_version,
            { "version", "ceph.version", FT_UINT16, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_mon_protocol,
            { "mon_protocol", "ceph.mon_protocol", FT_UINT16, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_osd_protocol,
            { "osd_protocol", "ceph.osd_protocol", FT_UINT16, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_mds_protocol,
            { "mds_protocol", "ceph.mds_protocol", FT_UINT16, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_client_protocol,
            { "client_protocol", "ceph.client_protocol", FT_UINT16, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_front_len,
            { "front_len", "ceph.front_len", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_middle_len,
            { "middle_len", "ceph.middle_len", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_data_off,
            { "data_off", "ceph.data_off", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_data_len,
            { "data_len", "ceph.data_len", FT_UINT32, 
                BASE_DEC, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_src,
            { "src entity name", "ceph.src", FT_NONE, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_hdr_crc,
            { "crc", "ceph.crc", FT_UINT32, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_footer,
            { "Footer", "ceph.footer", FT_NONE, 
                BASE_NONE, NULL, 0x0, NULL, HFILL }},
        { &hf_footer_front_crc,
            { "front_crc", "ceph.footer.front_crc", FT_UINT32, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_footer_middle_crc,
            { "middle_crc", "ceph.footer.middle_crc", FT_UINT32, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_footer_data_crc,
            { "data_crc", "ceph.footer.data_crc", FT_UINT32, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_footer_sig,
            { "sig", "ceph.footer.sig", FT_UINT64, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
        { &hf_footer_flags,
            { "flags", "ceph.footer.flags", FT_UINT8, 
                BASE_HEX, NULL, 0x0, NULL, HFILL }},
    };
    static gint *ett[] = {
        &ett_ceph,
        &ett_header,
        &ett_banner,
        &ett_entity_addr,
        &ett_front,
        &ett_footer,
        &ett_sockaddr_in,
        &ett_entity_name,
        &ett_compat,
        &ett_rocompat,
        &ett_incompat,
    };

    /*
     * Register the CEPH protocol
     */
    g_proto_ceph = proto_register_protocol ("CEPH Protocol", "CEPH", "ceph");
    proto_register_field_array (g_proto_ceph, hf, array_length (hf));
    proto_register_subtree_array (ett, array_length (ett));
    register_dissector("ceph", dissect_ceph, g_proto_ceph);
}


