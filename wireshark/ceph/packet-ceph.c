/* packet-ceph.c
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
*/

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif


#include <stdio.h>
#include <glib.h>
#include <epan/packet.h>
#include <epan/tvbuff.h>
#include <epan/tvbuff-int.h>

#include <epan/dissectors/packet-tcp.h>

#include "types.h"
#include "crc32c.h"
#include <string.h>

#define PROTO_TAG_CEPH	"CEPH"
#define AUTH_LEN		174

/* Wireshark ID of the CEPH protocol */
static int proto_ceph = -1;



/* These are the handles of our subdissectors */
static dissector_handle_t data_handle=NULL;

static dissector_handle_t ceph_handle;
static void dissect_ceph(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree);

static guint32 global_ceph_min_port = 6789;
static guint32 global_ceph_max_port = 6810;

static guint32 global_ceph_min_mon_port = 6789;
static guint32 global_ceph_max_mon_port = 6799;

#define DEST_PORT_CEPH ((pinfo->destport >= global_ceph_min_port) && (pinfo->destport <= global_ceph_max_port))

#define PORT_IS_MON(port) ((port >= global_ceph_min_mon_port) && (port <= global_ceph_max_mon_port))
#define PORT_IS_MDS(port) ((port >= global_ceph_min_mds_port) && (port <= global_ceph_max_mds_port))
#define PORT_IS_OSD(port) ((port >= global_ceph_min_osd_port) && (port <= global_ceph_max_osd_port))

#define IS_ENTITY(cmp, port1, port2) (cmp(port1) || cmp(port2))

#define IS_MON(pinfo) IS_ENTITY(PORT_IS_MON, pinfo->srcport, pinfo->destport)
#define IS_MDS(pinfo) IS_ENTITY(PORT_IS_MDS, pinfo->srcport, pinfo->destport)
#define IS_OSD(pinfo) IS_ENTITY(PORT_IS_OSD, pinfo->srcport, pinfo->destport)

#define MON_STR "mon"
#define MDS_STR "mds"
#define OSD_STR "osd"

#define FMT_INO	"0x%.16llx"

#define PROTO_ADD_TEXT(type, s, field, modifier)\
	proto_tree_add_text(tree, tvb, offset + offsetof(type, field), sizeof(s->field), "" #field ": " modifier, s->field);

#define PROTO_ADD_SIMPLE_TEXT(tree,getter,format,var)\
	do { \
	    var = getter(tvb,offset); \
	    proto_tree_add_text(tree, tvb, offset, sizeof(var), format, var);\
	    offset += sizeof(var); \
	} while(0)


#define PROTO_ADD_ITEM(tree, type, hf, s, field) \
	proto_tree_add_item(tree, hf, tvb, offset + offsetof(type, field), sizeof(s->field), TRUE);

#define CTIME_BUF_LEN	128

#define PROTO_ADD_TIME(tvb, tree, type, offset, head, field, name) \
	do { \
		time_t time;			\
		time = head->field.tv_sec;	\
		proto_tree_add_text(tree, tvb, offset + offsetof(type, field), \
			sizeof(head->field), "" #name ": %s (%d ns)", ctime(&time), head->field.tv_nsec); \
	} while (0)

static const value_string packettypenames[] = {
	{ 1, "Shutdown" },
	{ 2, "Ping" },
	{ 4, "Mon Map" },
	{ 5, "Mon Get Map" },
	{ 10, "Client Mount" },
	{ 11, "Client Mount Ack" },
	{ 12, "Client Unmount" },
	{ 13, "Statfs" },
	{ 14, "Statfs Reply" },
	{ 15, "Mon Subscribe" },
	{ 16, "Mon Subscribe Ack" },
	{ 17, "Authentication" },
	{ 18, "Authentication reply" },
	{ 20, "MDS Get Map" },
	{ 21, "MDS Map" },
	{ 22, "Client Session" },
	{ 23, "Client Reconnect" },
	{ 24, "Client Request" },
	{ 25, "Client Request Forward" },
	{ 26, "Client Reply" },
	{ 0x310, "Client Caps" },
	{ 0x311, "Client Lease" },
	{ 0x312, "Client Snap" },
	{ 0x313, "Client Cap Release" },
	{ 40, "OSD Get Map" },
	{ 41, "OSD Map" },
	{ 42, "OSD Op" },
	{ 43, "OSD Op Reply" },
	{ 44, "Watch Notify" },
	{ 48, "Poolop Reply" },
	{ 49, "Poolop" },
	{ 0, NULL }
};	

#define ACK_MSG_SIZE			9
#define TVB_MSG_HEADER_POS(x) (1 + offsetof(struct ceph_msg_header, x))
#define TVB_IS_ACK(ofs) (tvb_get_guint8(tvb, ofs) == CEPH_MSGR_TAG_ACK)
#define TVB_MSG_FIELD(func, tvb, ofs, field) func(tvb, ofs + (TVB_IS_ACK(ofs) ? ACK_MSG_SIZE : 0) + TVB_MSG_HEADER_POS(field))

/* The following hf_* variables are used to hold the Wireshark IDs of
* our header fields; they are filled out when we call
* proto_register_field_array() in proto_register_ceph()
*/
/** Kts attempt at defining the protocol */
static gint hf_ceph = -1;
static gint hf_ceph_mds_op = -1;
static gint hf_ceph_header = -1;
static gint hf_ceph_banner = -1;
static gint hf_ceph_entity_addr = -1;
static gint hf_ceph_entity_type = -1;
static gint hf_ceph_entity_num = -1;
static gint hf_ceph_fsid = -1;
static gint hf_ceph_banner_magic = -1;
static gint hf_ceph_banner_version = -1;
static gint hf_ceph_connect_erank = -1;
static gint hf_ceph_connect_nonce = -1;
static gint hf_ceph_sockaddr_in = -1;
static gint hf_ceph_connect_host_type = -1;
static gint hf_ceph_connect_tag = -1;
static gint hf_ceph_connect_global_seq = -1;
static gint hf_ceph_connect_connect_seq = -1;
static gint hf_ceph_connect_flags = -1;
static gint hf_ceph_length = -1;
static gint hf_ceph_type = -1;
static gint hf_ceph_text = -1;
static gint hf_ceph_path = -1;
static gint hf_sin_family = -1;
static gint hf_sin_port = -1;
static gint hf_sin_addr = -1;
static gint hf_ceph_hdr_tag = -1;
static gint hf_ceph_hdr_seq_ack = -1;
static gint hf_ceph_hdr_seq = -1;
static gint hf_ceph_hdr_type = -1;
static gint hf_ceph_hdr_priority = -1;
static gint hf_ceph_hdr_version = -1;
static gint hf_ceph_hdr_mon_protocol = -1;
static gint hf_ceph_hdr_osd_protocol = -1;
static gint hf_ceph_hdr_mds_protocol = -1;
static gint hf_ceph_hdr_client_protocol = -1;
static gint hf_ceph_hdr_front_len = -1;
static gint hf_ceph_hdr_middle_len = -1;
static gint hf_ceph_hdr_data_off = -1;
static gint hf_ceph_hdr_data_len = -1;
static gint hf_ceph_data = -1;
static gint hf_ceph_front = -1;
static gint hf_ceph_hdr_src = -1;
static gint hf_ceph_hdr_orig_src = -1;
static gint hf_ceph_hdr_dst = -1;
static gint hf_ceph_hdr_crc = -1;
static gint hf_ceph_footer = -1;
static gint hf_ceph_footer_flags = -1;
static gint hf_ceph_footer_front_crc = -1;
static gint hf_ceph_footer_middle_crc = -1;
static gint hf_ceph_footer_data_crc = -1;
static gint hf_ceph_monmap = -1;
static gint hf_ceph_mdsmap = -1;
static gint hf_ceph_mdsnode = -1;
static gint hf_ceph_auth_reply = -1;
static gint hf_ceph_pgpools = -1;

/* These are the ids of the subtrees that we may be creating */
static gint ett_ceph = -1;
static gint ett_ceph_header = -1;
static gint ett_ceph_banner = -1;
static gint ett_ceph_entity_addr = -1;
static gint ett_ceph_length = -1;
static gint ett_ceph_type = -1;
static gint ett_ceph_text = -1;
static gint ett_ceph_front = -1;
static gint ett_ceph_data = -1;
static gint ett_ceph_footer = -1;


const char *ceph_cap_op_name(int op)
{
        char* plop;

        switch (op) {
        case CEPH_CAP_OP_GRANT: return "grant";
        case CEPH_CAP_OP_REVOKE: return "revoke";
        case CEPH_CAP_OP_TRUNC: return "trunc";
        case CEPH_CAP_OP_EXPORT: return "export";
        case CEPH_CAP_OP_IMPORT: return "import";
        case CEPH_CAP_OP_UPDATE: return "update";
        case CEPH_CAP_OP_DROP: return "drop";
        case CEPH_CAP_OP_FLUSH: return "flush";
        case CEPH_CAP_OP_FLUSH_ACK: return "flush_ack";
        case CEPH_CAP_OP_FLUSHSNAP: return "flushsnap";
        case CEPH_CAP_OP_FLUSHSNAP_ACK: return "flushsnap_ack";
        case CEPH_CAP_OP_RELEASE: return "release";
        case CEPH_CAP_OP_RENEW: return "renew";
        }

        plop = malloc(16*sizeof(char));
        sprintf(plop,"%i",op);

        return plop;
}

const char *ceph_mds_op_name(int op)
{
	char* plop;

        switch (op) {
        case CEPH_MDS_OP_LOOKUP:  return "lookup";
        case CEPH_MDS_OP_LOOKUPHASH:  return "lookuphash";
        case CEPH_MDS_OP_LOOKUPPARENT:  return "lookupparent";
        case CEPH_MDS_OP_LOOKUPINO:  return "lookupino";
        case CEPH_MDS_OP_LOOKUPNAME:  return "lookupname";
        case CEPH_MDS_OP_GETATTR:  return "getattr";
        case CEPH_MDS_OP_SETXATTR: return "setxattr";
        case CEPH_MDS_OP_SETATTR: return "setattr";
        case CEPH_MDS_OP_RMXATTR: return "rmxattr";
        case CEPH_MDS_OP_SETLAYOUT: return "setlayou";
        case CEPH_MDS_OP_SETDIRLAYOUT: return "setdirlayout";
        case CEPH_MDS_OP_READDIR: return "readdir";
        case CEPH_MDS_OP_MKNOD: return "mknod";
        case CEPH_MDS_OP_LINK: return "link";
        case CEPH_MDS_OP_UNLINK: return "unlink";
        case CEPH_MDS_OP_RENAME: return "rename";
        case CEPH_MDS_OP_MKDIR: return "mkdir";
        case CEPH_MDS_OP_RMDIR: return "rmdir";
        case CEPH_MDS_OP_SYMLINK: return "symlink";
        case CEPH_MDS_OP_CREATE: return "create";
        case CEPH_MDS_OP_OPEN: return "open";
        case CEPH_MDS_OP_LOOKUPSNAP: return "lookupsnap";
        case CEPH_MDS_OP_LSSNAP: return "lssnap";
        case CEPH_MDS_OP_MKSNAP: return "mksnap";
        case CEPH_MDS_OP_RMSNAP: return "rmsnap";
        case CEPH_MDS_OP_SETFILELOCK: return "setfilelock";
        case CEPH_MDS_OP_GETFILELOCK: return "getfilelock";
        }

	plop = malloc(16*sizeof(char));
        printf(plop,"%i",op);

        return plop;
}



void proto_reg_handoff_ceph(void)
{
	static gboolean initialized=FALSE;
	static guint32 port;

	if (!initialized) {
		data_handle = find_dissector("data");
		ceph_handle = create_dissector_handle(dissect_ceph, proto_ceph);
		for (port = global_ceph_min_port; port <= global_ceph_max_port; port++)
			dissector_add("tcp.port", port, ceph_handle);
	}

}

void proto_register_ceph (void)
{
	/* A header field is something you can search/filter on.
	* 
	* We create a structure to register our fields. It consists of an
	* array of hf_register_info structures, each of which are of the format
	* {&(field id), {name, abbrev, type, display, strings, bitmask, blurb, HFILL}}.
	*/
	static hf_register_info hf[] = {
		{ &hf_ceph,
			{ "Data", "ceph.data", FT_NONE , BASE_NONE, NULL, 0,
			"CEPH PDU", HFILL }},
		{ &hf_ceph_pgpools,
			{ "pgpools", "ceph.pgpools", FT_NONE , BASE_NONE, NULL, 0,
			"CEPH PG Pools", HFILL }},
		{ &hf_ceph_monmap,
			{ "MON map", "ceph.monmap", FT_NONE , BASE_NONE, NULL, 0,
			"CEPH MON map", HFILL }},
		{ &hf_ceph_mdsmap,
			{ "MDS map", "ceph.mdsmap", FT_NONE , BASE_NONE, NULL, 0,
			"CEPH MDS map", HFILL }},
		{ &hf_ceph_mdsnode,
			{ "MDS node", "ceph.mdsnode", FT_NONE , BASE_NONE, NULL, 0,
			"CEPH MDS node", HFILL }},
		{ &hf_ceph_auth_reply,
			{ "Authentication reply", "ceph.auth.reply", FT_NONE , BASE_NONE, NULL, 0,
			"CEPH Authentication reply", HFILL }},
		{ &hf_ceph_header,
			{ "Header", "ceph.header", FT_NONE, BASE_NONE, NULL, 0x0,
		 	"CEPH Header", HFILL }},
		{ &hf_ceph_banner,
			{ "Ceph Banner", "ceph.connect.banner", FT_STRING, BASE_NONE, NULL, 0x0,
			"Ceph Banner", HFILL }},
		{ &hf_ceph_entity_type,
			{ "Ceph Entity Type", "ceph.entity.type", FT_UINT32, BASE_DEC, NULL, 0x0,
			"Ceph Entity Type", HFILL }},
		{ &hf_ceph_entity_num,
			{ "Ceph Entity Num", "ceph.entity.num", FT_UINT64, BASE_DEC, NULL, 0x0,
			"Ceph Entity Num", HFILL }},
		{ &hf_ceph_entity_addr,
			{ "Ceph Entity Addr", "ceph.entity.addr", FT_NONE, BASE_NONE, NULL, 0x0,
			"Ceph Entity Addr", HFILL }},
		{ &hf_ceph_fsid,
			{ "Ceph FSID", "ceph.fsid", FT_NONE, BASE_NONE, NULL, 0x0,
			"Ceph FSID", HFILL }},
		{ &hf_ceph_banner_magic,
			{ "Ceph Banner Magic", "ceph.connect.banner.magic", FT_STRING, BASE_NONE, NULL, 0x0,
			"Ceph Banner Magic", HFILL }},
		{ &hf_ceph_banner_version,
			{ "Ceph Banner Version", "ceph.connect.banner.ver", FT_STRING, BASE_NONE, NULL, 0x0,
			"Ceph Banner", HFILL }},
		{ &hf_ceph_connect_erank,
			{ "erank", "ceph.connect.erank", FT_UINT32, BASE_HEX, NULL, 0x0,
			"connect: erank", HFILL }},
		{ &hf_ceph_connect_nonce,
			{ "nonce", "ceph.connect.nonce", FT_UINT32, BASE_HEX, NULL, 0x0,
			"connect: nonce", HFILL }},
		{ &hf_ceph_sockaddr_in,
			{ "sockaddr_in", "ceph.sockaddr_in", FT_NONE, BASE_NONE, NULL, 0x0,
		 	"sockaddr_in", HFILL }},
		{ &hf_sin_family,
			{ "sin_family", "ceph.sin_family", FT_UINT16, BASE_HEX, NULL, 0x0,
		 	"sockaddr_in: sin_family", HFILL }},
		{ &hf_sin_port,
			{ "sin_port", "ceph.sin_port", FT_UINT16, BASE_DEC, NULL, 0x0,
		 	"sockaddr_in: sin_port", HFILL }},
		{ &hf_sin_addr,
			{ "ip addr", "ceph.addr", FT_IPv4, BASE_NONE, NULL, 0x0,
		 	"sockaddr_in: ip addr", HFILL }},
		{ &hf_ceph_connect_host_type,
			{ "host_type", "ceph.connect.host_type", FT_UINT32, BASE_DEC, NULL, 0x0,
		 	"connect: host_type", HFILL }},
		{ &hf_ceph_connect_tag,
			{ "tag", "ceph.connect.tag", FT_UINT8, BASE_DEC, NULL, 0x0,
		 	"connect: tag", HFILL }},
		{ &hf_ceph_mds_op,
			{ "mds op", "ceph.mds.op", FT_UINT32, BASE_HEX, NULL, 0x0,
		 	"ceph: mds op", HFILL }},
		{ &hf_ceph_connect_global_seq,
			{ "global_seq", "ceph.connect.global_seq", FT_UINT32, BASE_DEC, NULL, 0x0,
		 	"connect: global_seq", HFILL }},
		{ &hf_ceph_connect_connect_seq,
			{ "connect_seq", "ceph.connect.connect_seq", FT_UINT32, BASE_DEC, NULL, 0x0,
		 	"connect: connect_seq", HFILL }},
		{ &hf_ceph_connect_flags,
			{ "flags", "ceph.connect.flags", FT_UINT8, BASE_HEX, NULL, 0x0,
		 	"connect: flags", HFILL }},
		{ &hf_ceph_length,
			{ "Package Length", "ceph.len", FT_UINT32, BASE_DEC, NULL, 0x0,
			"Package Length", HFILL }},
		{ &hf_ceph_type,
			{ "Type", "ceph.type", FT_UINT8, BASE_DEC, VALS(packettypenames), 0x0,
		 	"Package Type", HFILL }},
		{ &hf_ceph_text,
			{ "Text", "ceph.text", FT_STRING, BASE_NONE, NULL, 0x0,
		 	"Text", HFILL }},
		{ &hf_ceph_path,
			{ "path", "ceph.path", FT_STRING, BASE_NONE, NULL, 0x0,
		 	"path", HFILL }},
		{ &hf_ceph_hdr_tag,
			{ "tag", "ceph.tag", FT_UINT8, BASE_DEC, NULL, 0x0,
		 	"hdr: tag", HFILL }},
		{ &hf_ceph_hdr_seq_ack,
			{ "ack seq", "ceph.ack.seq", FT_UINT64, BASE_DEC, NULL, 0x0,
		 	"ack: seq", HFILL }},
		{ &hf_ceph_hdr_seq,
			{ "seq", "ceph.seq", FT_UINT64, BASE_DEC, NULL, 0x0,
		 	"hdr: seq", HFILL }},
		{ &hf_ceph_hdr_type,
			{ "type", "ceph.type", FT_UINT16, BASE_HEX, NULL, 0x0,
		 	"hdr: type", HFILL }},
		{ &hf_ceph_hdr_priority,
			{ "priority", "ceph.priority", FT_UINT16, BASE_DEC, NULL, 0x0,
		 	"hdr: priority", HFILL }},
		{ &hf_ceph_hdr_version,
			{ "version", "ceph.version", FT_UINT16, BASE_DEC, NULL, 0x0,
		 	"hdr: version", HFILL }},
		{ &hf_ceph_hdr_mon_protocol,
			{ "mon_protocol", "ceph.mon_protocol", FT_UINT16, BASE_DEC, NULL, 0x0,
		 	"hdr: mon_protocol", HFILL }},
		{ &hf_ceph_hdr_osd_protocol,
			{ "osd_protocol", "ceph.osd_protocol", FT_UINT16, BASE_DEC, NULL, 0x0,
		 	"hdr: osd_protocol", HFILL }},
		{ &hf_ceph_hdr_mds_protocol,
			{ "mds_protocol", "ceph.mds_protocol", FT_UINT16, BASE_DEC, NULL, 0x0,
		 	"hdr: mds_protocol", HFILL }},
		{ &hf_ceph_hdr_client_protocol,
			{ "client_protocol", "ceph.client_protocol", FT_UINT16, BASE_DEC, NULL, 0x0,
		 	"hdr: client_protocol", HFILL }},
		{ &hf_ceph_hdr_front_len,
			{ "front_len", "ceph.front_len", FT_UINT32, BASE_DEC, NULL, 0x0,
		 	"hdr: front_len", HFILL }},
		{ &hf_ceph_hdr_middle_len,
			{ "middle_len", "ceph.middle_len", FT_UINT32, BASE_DEC, NULL, 0x0,
		 	"hdr: middle_len", HFILL }},
		{ &hf_ceph_hdr_data_off,
			{ "data_off", "ceph.data_off", FT_UINT32, BASE_DEC, NULL, 0x0,
		 	"hdr: data_off", HFILL }},
		{ &hf_ceph_hdr_data_len,
			{ "data_len", "ceph.data_len", FT_UINT32, BASE_DEC, NULL, 0x0,
		 	"hdr: data_len", HFILL }},
		{ &hf_ceph_hdr_src,
			{ "src", "ceph.src", FT_NONE, BASE_NONE, NULL, 0x0,
		 	"hdr: src", HFILL }},
		{ &hf_ceph_hdr_orig_src,
			{ "orig_src", "ceph.orig_src", FT_NONE, BASE_NONE, NULL, 0x0,
		 	"hdr: orig_src", HFILL }},
		{ &hf_ceph_hdr_dst,
			{ "dst", "ceph.dst", FT_NONE, BASE_NONE, NULL, 0x0,
		 	"hdr: dst", HFILL }},
		{ &hf_ceph_hdr_crc,
			{ "crc", "ceph.crc", FT_UINT32, BASE_HEX, NULL, 0x0,
		 	"hdr: crc", HFILL }},
		{ &hf_ceph_front,
			{ "Front", "ceph.front", FT_NONE, BASE_NONE, NULL, 0x0,
		 	"Ceph Front", HFILL }},
		{ &hf_ceph_data,
			{ "Data", "ceph.data", FT_NONE, BASE_NONE, NULL, 0x0,
		 	"Ceph Data", HFILL }},
		{ &hf_ceph_footer,
			{ "Footer", "ceph.footer", FT_NONE, BASE_NONE, NULL, 0x0,
		 	"Ceph Footer", HFILL }},
		{ &hf_ceph_footer_flags,
			{ "flags", "ceph.footer.flags", FT_UINT32, BASE_HEX, NULL, 0x0,
		 	"footer: flags", HFILL }},
		{ &hf_ceph_footer_front_crc,
			{ "front_crc", "ceph.footer.front_crc", FT_UINT32, BASE_HEX, NULL, 0x0,
		 	"footer: front_crc", HFILL }},
		{ &hf_ceph_footer_middle_crc,
			{ "middle_crc", "ceph.footer.middle_crc", FT_UINT32, BASE_HEX, NULL, 0x0,
		 	"footer: middle_crc", HFILL }},
		{ &hf_ceph_footer_data_crc,
			{ "data_crc", "ceph.footer.data_crc", FT_UINT32, BASE_HEX, NULL, 0x0,
		 	"footer: data_crc", HFILL }},
	};
	static gint *ett[] = {
		&ett_ceph,
		&ett_ceph_header,
		&ett_ceph_banner,
		&ett_ceph_length,
		&ett_ceph_entity_addr,
		&ett_ceph_type,
		&ett_ceph_text,
		&ett_ceph_data,
		&ett_ceph_front,
		&ett_ceph_footer,
	};
	//if (proto_ceph == -1) { /* execute protocol initialization only once */
	proto_ceph = proto_register_protocol ("CEPH Protocol", "CEPH", "ceph");

	proto_register_field_array (proto_ceph, hf, array_length (hf));
	proto_register_subtree_array (ett, array_length (ett));
	register_dissector("ceph", dissect_ceph, proto_ceph);
	//}
}
	
static guint32 dissect_sockaddr_in(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	proto_tree *ceph_sockaddr_tree;
	proto_item *ceph_sub_item = NULL;
	proto_item *ceph_item = proto_tree_get_parent(tree);

	ceph_sockaddr_tree = proto_item_add_subtree(ceph_item, ett_ceph);

	ceph_sub_item = proto_tree_add_item( tree, hf_ceph_sockaddr_in, tvb, offset, 16, TRUE );
	ceph_sockaddr_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);

	proto_tree_add_item(ceph_sockaddr_tree, hf_sin_family, tvb, offset, 2, TRUE);
	proto_tree_add_item(ceph_sockaddr_tree, hf_sin_port, tvb, offset+2, 2, TRUE);
	proto_tree_add_item(ceph_sockaddr_tree, hf_sin_addr, tvb, offset+4, 4, FALSE);
	offset += 16;
	return offset;
}

static guint32 dissect_ceph_banner(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	proto_tree *ceph_banner_tree = NULL;
	proto_item *ceph_sub_item = NULL;

	ceph_sub_item = proto_tree_add_item( tree, hf_ceph_banner, tvb, offset, 9, TRUE );
	ceph_banner_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);

	proto_tree_add_item(ceph_banner_tree, hf_ceph_banner_magic, tvb, offset, 4, TRUE);
	proto_tree_add_item(ceph_banner_tree, hf_ceph_banner_version, tvb, offset+5, 4, TRUE);

	return offset+9;
}

static guint32 dissect_ceph_entity_addr(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	proto_tree *ceph_entity_tree = NULL;
	proto_item *ceph_sub_item = NULL;

	ceph_sub_item = proto_tree_add_item( tree, hf_ceph_entity_addr, tvb, offset, sizeof(struct ceph_entity_addr), TRUE );
	ceph_entity_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);
	proto_tree_add_item(ceph_entity_tree, hf_ceph_connect_erank, tvb, offset, 4, TRUE);
	proto_tree_add_item(ceph_entity_tree, hf_ceph_connect_nonce, tvb, offset+4, 4, TRUE);
	dissect_sockaddr_in(tvb, ceph_entity_tree, offset+8);
#if 0
	proto_tree_add_item(ceph_entity_tree, hf_ceph_connect_host_type, tvb, offset, 4, TRUE);
	offset += 4;
#endif

	return offset + sizeof(struct ceph_entity_addr);
}

static guint32 dissect_ceph_fsid(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	struct ceph_fsid fsid;
	guint32 * fsid_dec = NULL;
	fsid_dec = malloc(4*sizeof(guint32));
	fsid = *(struct ceph_fsid *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_fsid));
	memcpy(fsid_dec,fsid.fsid,4*sizeof(guint32));
	proto_tree_add_text(tree, tvb, offset, sizeof(struct ceph_fsid), "fsid: %x-%x-%x-%x",
			ntohl(fsid_dec[0]),
			ntohl(fsid_dec[1]),
			ntohl(fsid_dec[2]),
			ntohl(fsid_dec[3])
			);
	offset += sizeof(struct ceph_fsid);
	free (fsid_dec);
	return offset;
}

guint32 dissect_ceph_entity_name(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	proto_tree_add_item(tree, hf_ceph_entity_type, tvb, offset, 1, TRUE);
	proto_tree_add_item(tree, hf_ceph_entity_num, tvb, offset+1, 8, TRUE);
	offset += sizeof(struct ceph_entity_name);
	return offset;
}


static guint32 dissect_ceph_footer(tvbuff_t *tvb, proto_tree *tree, guint32 offset, guint32 data_crc)
{
	proto_tree *ceph_footer_tree = NULL;
	proto_item *ceph_sub_item = NULL;
	proto_item *data_crc_item = NULL;

	ceph_sub_item = proto_tree_add_item( tree, hf_ceph_footer, tvb, offset, sizeof(struct ceph_msg_footer), TRUE );
	ceph_footer_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);
	proto_tree_add_item(ceph_footer_tree, hf_ceph_footer_front_crc, tvb, offset, 4, TRUE);
	proto_tree_add_item(ceph_footer_tree, hf_ceph_footer_middle_crc, tvb, offset+4, 4, TRUE);
	data_crc_item = proto_tree_add_item(ceph_footer_tree, hf_ceph_footer_data_crc, tvb, offset+8, 4, TRUE);
	proto_item_append_text(data_crc_item, " (calculated %x)", data_crc);
	proto_tree_add_item(ceph_footer_tree, hf_ceph_footer_flags, tvb, offset+12, 1, TRUE);

	offset += 13;
	return offset;
}

static guint32 dissect_ceph_client_connect(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	proto_tree *ceph_header_tree;
	proto_item *ceph_sub_item = NULL;
	proto_item *ceph_item = proto_tree_get_parent(tree);
	struct ceph_msg_connect *msg;
	guint32 auth_len;

	offset = dissect_ceph_banner(tvb, tree, offset);

	ceph_header_tree = proto_item_add_subtree(ceph_item, ett_ceph);

	ceph_sub_item = proto_tree_add_item( tree, hf_ceph_header, tvb, offset, -1, TRUE );
	ceph_header_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);

	offset = dissect_ceph_entity_addr(tvb, ceph_header_tree, offset);
#if 0
	proto_tree_add_item(ceph_header_tree, hf_ceph_connect_host_type, tvb, offset, 4, TRUE);
	offset += 4;
	proto_tree_add_item(ceph_header_tree, hf_ceph_connect_global_seq, tvb, offset, 4, TRUE);
	proto_tree_add_item(ceph_header_tree, hf_ceph_connect_connect_seq, tvb, offset+4, 4, TRUE);
	proto_tree_add_item(ceph_header_tree, hf_ceph_connect_flags, tvb, offset+8, 1, TRUE);
	offset += 8;
#endif

	msg = (struct ceph_msg_connect *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_msg_connect));
	PROTO_ADD_TEXT(struct ceph_msg_connect, msg, features, "%lld");
	PROTO_ADD_TEXT(struct ceph_msg_connect, msg, host_type, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect, msg, connect_seq, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect, msg, global_seq, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect, msg, protocol_version, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect, msg, flags, "%x");
	PROTO_ADD_TEXT(struct ceph_msg_connect, msg, authorizer_protocol, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect, msg, authorizer_len, "%d");
	offset += sizeof(struct ceph_msg_connect);
	auth_len = msg->authorizer_len;
	auth_len = 174;
	proto_tree_add_text(tree,tvb, offset, auth_len, "Authentication key");
	offset += auth_len;
	return offset;
}

static guint32 dissect_ceph_server_connect(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	proto_tree *ceph_header_tree;
	proto_item *ceph_sub_item = NULL;
	proto_item *ceph_item = proto_tree_get_parent(tree);
	struct ceph_msg_connect_reply *msg;

	offset = dissect_ceph_banner(tvb, tree, offset);

	ceph_header_tree = proto_item_add_subtree(ceph_item, ett_ceph);

	ceph_sub_item = proto_tree_add_item( tree, hf_ceph_header, tvb, offset, -1, TRUE );
	ceph_header_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);

	offset = dissect_ceph_entity_addr(tvb, ceph_header_tree, offset);
	offset = dissect_ceph_entity_addr(tvb, ceph_header_tree, offset);
	msg = (struct ceph_msg_connect_reply *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_msg_connect_reply));
	PROTO_ADD_TEXT(struct ceph_msg_connect_reply, msg, tag, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect_reply, msg, features, "%lld");
	PROTO_ADD_TEXT(struct ceph_msg_connect_reply, msg, global_seq, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect_reply, msg, connect_seq, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect_reply, msg, protocol_version, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect_reply, msg, authorizer_len, "%d");
	PROTO_ADD_TEXT(struct ceph_msg_connect_reply, msg, flags, "%x");

	return offset;
}

static guint32 dissect_ceph_file_layout(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	guint32 orig_ofs = offset;
	struct ceph_file_layout *lo;

	lo = (struct ceph_file_layout *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_file_layout));
	
	PROTO_ADD_TEXT(struct ceph_file_layout, lo, fl_stripe_unit, "%d");
	PROTO_ADD_TEXT(struct ceph_file_layout, lo, fl_stripe_count, "%d");
	PROTO_ADD_TEXT(struct ceph_file_layout, lo, fl_object_size, "%d");
	PROTO_ADD_TEXT(struct ceph_file_layout, lo, fl_cas_hash, "%d");
	PROTO_ADD_TEXT(struct ceph_file_layout, lo, fl_object_stripe_unit, "%d");
	PROTO_ADD_TEXT(struct ceph_file_layout, lo, fl_pg_preferred, "%d");
	PROTO_ADD_TEXT(struct ceph_file_layout, lo, fl_pg_pool, "%u");

	return orig_ofs + sizeof(struct ceph_mds_reply_head);
}
#if 0
static int dissect_ceph_filepath(tvbuff_t *tvb, proto_tree *tree, guint32 offset, char **path, guint64 *ino)
{
	guint32 len;
	const char *p = NULL;

	*ino = tvb_get_letoh64(tvb, offset);
	proto_tree_add_text(tree, tvb, offset, sizeof(*ino), "inode: " FMT_INO, *ino);
	offset += sizeof(*ino);
	len = tvb_get_letohl(tvb, offset);
	proto_tree_add_text(tree, tvb, offset, sizeof(len), "len: %d", len);
	offset += sizeof(len);

	if (len) {
		p = tvb_get_ptr(tvb, offset, len);
		*path = malloc(len+1);
		if (*path) {
			memcpy(*path, p, len);
			(*path)[len] = '\0';
			proto_tree_add_item(tree, hf_ceph_path, tvb, offset, len, TRUE);
		}
	}

	offset += len;

	return offset;
}
#endif
static guint32 dissect_ceph_mon_statfs(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	struct ceph_mon_statfs *req;

	req = (struct ceph_mon_statfs *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_mon_statfs));
	
	dissect_ceph_fsid(tvb, tree, offset + offsetof(struct ceph_mon_statfs, fsid));
	PROTO_ADD_TEXT(struct ceph_mon_statfs, req, monhdr.session_mon_tid, "%lld");

	return offset + sizeof(struct ceph_mon_statfs);
}

static guint32 dissect_ceph_mon_statfs_reply(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	struct ceph_mon_statfs_reply *req;

	req = (struct ceph_mon_statfs_reply *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_mon_statfs_reply));
	
	dissect_ceph_fsid(tvb, tree, offset + offsetof(struct ceph_mon_statfs_reply, fsid));
	PROTO_ADD_TEXT(struct ceph_mon_statfs_reply, req, version, "%lld");

	PROTO_ADD_TEXT(struct ceph_mon_statfs_reply, req, st.kb, "%lld");
	PROTO_ADD_TEXT(struct ceph_mon_statfs_reply, req, st.kb_used, "%lld");
	PROTO_ADD_TEXT(struct ceph_mon_statfs_reply, req, st.kb_avail, "%lld");
	PROTO_ADD_TEXT(struct ceph_mon_statfs_reply, req, st.num_objects, "%lld");

	return offset + sizeof(struct ceph_mon_statfs_reply);
}
#if 0
static guint32 dissect_ceph_client_osd_getmap(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	struct ceph_osd_getmap *req;

	req = (struct ceph_osd_getmap *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_osd_getmap));
	
	dissect_ceph_fsid(tvb, tree, offset + offsetof(struct ceph_osd_getmap, fsid));
	PROTO_ADD_TEXT(struct ceph_osd_getmap, req, start, "%d");

	return offset + sizeof(struct ceph_osd_getmap);
}

static guint32 dissect_ceph_client_mds_getmap(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	struct ceph_mds_getmap *req;

	req = (struct ceph_mds_getmap *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_mds_getmap));
	
	PROTO_ADD_TEXT(struct ceph_mds_getmap, req, monhdr.have_version, "%lld");
	dissect_ceph_fsid(tvb, tree, offset + offsetof(struct ceph_mds_getmap, fsid));

	return offset + sizeof(struct ceph_mds_getmap);
}
#endif
static guint32 dissect_ceph_client_mds_request(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, guint32 offset)
{
	struct ceph_mds_request_head *head;
	proto_item *item;

	head = (struct ceph_mds_request_head *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_mds_request_head));
	
	//PROTO_ADD_TEXT(struct ceph_mds_request_head, head, tid, "%lld");
	PROTO_ADD_TEXT(struct ceph_mds_request_head, head, oldest_client_tid, "%lld");
	PROTO_ADD_TEXT(struct ceph_mds_request_head, head, mdsmap_epoch, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_request_head, head, num_retry, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_request_head, head, num_fwd, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_request_head, head, num_releases, "%d");

	item = proto_tree_add_item(tree, hf_ceph_mds_op, tvb, offset+offsetof(struct ceph_mds_request_head, op), sizeof(head->op), TRUE);
	proto_item_append_text(item, " (%s)", ceph_mds_op_name(head->op));

	PROTO_ADD_TEXT(struct ceph_mds_request_head, head, caller_uid, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_request_head, head, caller_gid, "%d");

	if (check_col(pinfo->cinfo, COL_INFO)) {
		col_append_fstr(pinfo->cinfo, COL_INFO, " (%s)", ceph_mds_op_name(head->op));
	}
	
	/* FIXME */
	switch (head->op) {
	case CEPH_MDS_OP_LOOKUP:
//		PROTO_ADD_TEXT(struct ceph_mds_request_head, head, args.lookup.mask, "0x%.4x");
		break;
	case CEPH_MDS_OP_SETXATTR:
		break;
	case CEPH_MDS_OP_SETLAYOUT:
		dissect_ceph_file_layout(tvb, tree, offset + offsetof(struct ceph_mds_request_head, args.setlayout.layout));
		break;
	case CEPH_MDS_OP_SETATTR:
		break;
	case CEPH_MDS_OP_MKNOD:
		PROTO_ADD_TEXT(struct ceph_mds_request_head, head, args.mknod.mode, "0%.5o");
		PROTO_ADD_TEXT(struct ceph_mds_request_head, head, args.mknod.rdev, "%d");
		break;
	case CEPH_MDS_OP_OPEN:
		PROTO_ADD_TEXT(struct ceph_mds_request_head, head, args.open.flags, "%x");
		PROTO_ADD_TEXT(struct ceph_mds_request_head, head, args.open.mode, "0%.5o");
		break;
	case CEPH_MDS_OP_MKDIR:
		PROTO_ADD_TEXT(struct ceph_mds_request_head, head, args.mkdir.mode, "0%.5o");
		break;
	case CEPH_MDS_OP_RMXATTR:
	case CEPH_MDS_OP_LINK:
	case CEPH_MDS_OP_UNLINK:
	case CEPH_MDS_OP_RENAME:
	case CEPH_MDS_OP_RMDIR:
	case CEPH_MDS_OP_SYMLINK:
	case CEPH_MDS_OP_LSSNAP:
	case CEPH_MDS_OP_MKSNAP:
	case CEPH_MDS_OP_RMSNAP:
	case CEPH_MDS_OP_CREATE:
		break;
	}

	offset += sizeof(struct ceph_mds_request_head);
#if 0
	if (head->op == CEPH_MDS_OP_FINDINODE) {
	
	} else {
		guint64 ino1, ino2;
		char *s1 = NULL, *s2 = NULL;

		offset = dissect_ceph_filepath(tvb, tree, offset, &s1, &ino1);
		offset = dissect_ceph_filepath(tvb, tree, offset, &s2, &ino2);

		if (check_col(pinfo->cinfo, COL_INFO)) {
			if (s1)
				col_append_fstr(pinfo->cinfo, COL_INFO, " %s", s1);
			if (s2)
				col_append_fstr(pinfo->cinfo, COL_INFO, " -> %s", s2);
		}


		
	}
#endif
	return offset;
}

static guint32 dissect_ceph_client_mds_reply(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, guint32 offset)
{
	guint32 orig_ofs = offset;
	struct ceph_mds_reply_head *head;

	head = (struct ceph_mds_reply_head *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_mds_reply_head));
	
	//PROTO_ADD_TEXT(struct ceph_mds_reply_head, head, tid, "%lld");

	proto_tree_add_text(tree, tvb, offsetof(struct ceph_mds_reply_head, op), 
				sizeof(head->op), "op: %d (%s)", head->op, ceph_mds_op_name(head->op));

	PROTO_ADD_TEXT(struct ceph_mds_reply_head, head, result, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_reply_head, head, mdsmap_epoch, "%d");


	if (check_col(pinfo->cinfo, COL_INFO)) {
		col_append_fstr(pinfo->cinfo, COL_INFO, " (%s)", ceph_mds_op_name(head->op));
	}

	return orig_ofs + sizeof(struct ceph_mds_reply_head);
}

static guint32 dissect_ceph_client_mds_lease_request(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, guint32 offset)
{
	guint32 orig_ofs = offset;
	struct ceph_mds_lease *head;
	static char *lease_action[] = { "", "revoke", "release", "renew" };

	head = (struct ceph_mds_lease *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_mds_lease));
	
	PROTO_ADD_TEXT(struct ceph_mds_lease, head, action, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_lease, head, mask, "%.4x");
	PROTO_ADD_TEXT(struct ceph_mds_lease, head, ino, FMT_INO);
	PROTO_ADD_TEXT(struct ceph_mds_lease, head, first, "%lld");
	PROTO_ADD_TEXT(struct ceph_mds_lease, head, last, "%lld");

	if (check_col(pinfo->cinfo, COL_INFO)) {
		if (head->action < 4) {
			col_append_fstr(pinfo->cinfo, COL_INFO, " (%s)", lease_action[head->action]);
		}
	}

	return orig_ofs + sizeof(struct ceph_mds_lease);
}

static guint32 dissect_ceph_client_mds_caps_request(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, guint32 offset)
{
	guint32 orig_ofs = offset;
	struct ceph_mds_caps *head;

	head = (struct ceph_mds_caps *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_mds_caps));
	
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, op, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, ino, FMT_INO);
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, seq, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, caps, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, wanted, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, size, "%llu");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, max_size, "%llu");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, truncate_seq, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, migrate_seq, "%d");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, time_warp_seq, "%u");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, snap_follows, "%llu");
	PROTO_ADD_TEXT(struct ceph_mds_caps, head, snap_trace_len, "%d");

#define CAPS_REQ_ADD_TIME(field) PROTO_ADD_TIME(tvb, tree, struct ceph_mds_caps, offset, head, field, field)
	CAPS_REQ_ADD_TIME(mtime);
	CAPS_REQ_ADD_TIME(atime);
	CAPS_REQ_ADD_TIME(ctime);

	if (check_col(pinfo->cinfo, COL_INFO)) {
		col_append_fstr(pinfo->cinfo, COL_INFO, " (%s)", ceph_cap_op_name(head->op));
	}

	return orig_ofs + sizeof(struct ceph_mds_caps);
}

static guint32 dissect_ceph_auth_reply(tvbuff_t *tvb, proto_tree *tree, guint32 offset){
	guint32 protocol, result, payload_len,str_len;
	guint64 global_id;
	proto_item *ceph_sub_item = NULL;
	proto_item *ceph_item = proto_tree_get_parent(tree);
	proto_tree *ceph_auth_reply_tree;

	ceph_auth_reply_tree = proto_item_add_subtree(ceph_item, ett_ceph);
	ceph_sub_item = proto_tree_add_item( tree, hf_ceph_auth_reply, tvb, offset, 16, TRUE );
	ceph_auth_reply_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);

	PROTO_ADD_SIMPLE_TEXT(ceph_auth_reply_tree,tvb_get_letohl,"protocol: %d",protocol);
	PROTO_ADD_SIMPLE_TEXT(ceph_auth_reply_tree,tvb_get_letohl,"result: %d",result);
	PROTO_ADD_SIMPLE_TEXT(ceph_auth_reply_tree,tvb_get_letoh64,"global_id: %" G_GUINT64_FORMAT, global_id);
	PROTO_ADD_SIMPLE_TEXT(ceph_auth_reply_tree,tvb_get_letohl,"payload_len: %d",payload_len);

	str_len = tvb_get_letohl(tvb,offset+payload_len);
	proto_tree_add_text(ceph_auth_reply_tree, tvb, offset+payload_len, str_len,"%s",tvb_get_const_stringz(tvb,offset,&str_len));
	return offset;
}

static guint32 dissect_ceph_pgpools(tvbuff_t *tvb, proto_tree *tree, guint32 offset){
	guint32 n, i, pool;
	proto_item *ceph_item = proto_tree_get_parent(tree);
	proto_tree *ceph_pgpools_tree;
	proto_item *ceph_sub_item = NULL;
	ceph_pgpools_tree = proto_item_add_subtree(ceph_item, ett_ceph);
	ceph_sub_item = proto_tree_add_item(tree, hf_ceph_pgpools, tvb, offset, -1, TRUE );
	ceph_pgpools_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);
	PROTO_ADD_SIMPLE_TEXT(ceph_pgpools_tree,tvb_get_letohl," %d pool(s)",n);
	for ( i = 0; i<n; i++){
		PROTO_ADD_SIMPLE_TEXT(ceph_pgpools_tree,tvb_get_letohl,"data_pg_pool: %d",pool);
		PROTO_ADD_SIMPLE_TEXT(ceph_pgpools_tree,tvb_get_letohl,"cas_pg_pool: %d",pool);
	}
	return offset;
}
static guint32 dissect_ceph_mdsmap_node(tvbuff_t *tvb, proto_tree *tree, guint32 offset){
	guint8 infoversion;
	guint32 field, num_export_targets, i;
	guint64 global_id, field64;
	proto_item *ceph_item = proto_tree_get_parent(tree);
	proto_tree *ceph_mdsnode_tree;
	proto_item *ceph_sub_item = NULL;
	struct ceph_timespec *cephtime = NULL;
	time_t time;
	ceph_mdsnode_tree = proto_item_add_subtree(ceph_item, ett_ceph);
	ceph_sub_item = proto_tree_add_item(tree, hf_ceph_mdsnode, tvb, offset, -1, TRUE );
	ceph_mdsnode_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);

	PROTO_ADD_SIMPLE_TEXT(ceph_mdsnode_tree,tvb_get_letoh64,"Global ID: %" G_GUINT64_FORMAT, global_id);
	infoversion = *tvb_get_ptr(tvb,offset,sizeof(infoversion));
	offset++;
	offset+=sizeof(guint64);
	proto_tree_add_text(ceph_mdsnode_tree, tvb, offset, sizeof(infoversion), "version: %d", infoversion);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsnode_tree,tvb_get_letoh64,"Name length: %d",field);
	proto_tree_add_text(ceph_mdsnode_tree, tvb, offset, field,"MDS name: %s",tvb_get_const_stringz(tvb,offset,&field));
	offset+=field-1;
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsnode_tree,tvb_get_letohl,"mds: %d",field);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsnode_tree,tvb_get_letohl,"inc: %d",field);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsnode_tree,tvb_get_letohl,"state: %d",field);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsnode_tree,tvb_get_letoh64,"state seq: %" G_GUINT64_FORMAT, field64);
	offset = dissect_ceph_entity_addr(tvb, ceph_mdsnode_tree, offset);
	cephtime = (struct ceph_timespec *) tvb_get_ptr(tvb, offset, sizeof(struct ceph_timespec));
	time = cephtime->tv_sec;
	proto_tree_add_text(ceph_mdsnode_tree, tvb, offset, sizeof(time), "Time: %s (%d ns)", ctime(&time), cephtime->tv_nsec);
	offset += sizeof(struct ceph_timespec);
	offset += sizeof(guint32);
	offset += tvb_get_letohl(tvb,offset);
	offset += sizeof(guint32);
	//offset ++;
	if (infoversion >= 2 ){
		PROTO_ADD_SIMPLE_TEXT(ceph_mdsnode_tree,tvb_get_letohl,"num_export_targets: %d",num_export_targets);
	}
	else{
		num_export_targets = 0;
	}
	for ( i = 0; i < num_export_targets ; i++){
		PROTO_ADD_SIMPLE_TEXT(ceph_mdsnode_tree,tvb_get_letohl,"export_target: %d",field);
	}

	return offset;
}

static guint32 dissect_ceph_mon_map(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	guint32 epoch, num_mon, len, i;
	guint16 version;
	proto_item *ceph_item = proto_tree_get_parent(tree);
	proto_tree *ceph_monmap_tree;
	proto_item *ceph_sub_item = NULL;
	ceph_monmap_tree = proto_item_add_subtree(ceph_item, ett_ceph);
	len = tvb_get_letohl(tvb,offset);
	ceph_sub_item = proto_tree_add_item(tree, hf_ceph_monmap, tvb, offset, len, TRUE );
	ceph_monmap_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);

	PROTO_ADD_SIMPLE_TEXT(ceph_monmap_tree,tvb_get_letohl,"length: %i",len);
	PROTO_ADD_SIMPLE_TEXT(ceph_monmap_tree,tvb_get_letohs,"version: %i",version);
	offset = dissect_ceph_fsid(tvb, ceph_monmap_tree, offset);
	PROTO_ADD_SIMPLE_TEXT(ceph_monmap_tree,tvb_get_letohl,"epoch: %i",epoch);
	PROTO_ADD_SIMPLE_TEXT(ceph_monmap_tree,tvb_get_letohl,"%i monitor",num_mon);
	for (i = 0; i < num_mon; i++)
		offset = dissect_ceph_entity_name(tvb, ceph_monmap_tree, offset);
		offset = dissect_ceph_entity_addr(tvb, ceph_monmap_tree, offset);

	return offset;
}

static guint32 dissect_ceph_mds_map(tvbuff_t *tvb, proto_tree *tree, guint32 offset)
{
	guint16 version;
	guint32 epoch, map_len, len, plop, i;
	guint64 max_filesize;
	proto_item *ceph_item = proto_tree_get_parent(tree);
	proto_tree *ceph_mdsmap_tree;
	proto_item *ceph_sub_item = NULL;
	ceph_mdsmap_tree = proto_item_add_subtree(ceph_item, ett_ceph);
	len = tvb_get_letohl(tvb,offset);
	ceph_sub_item = proto_tree_add_item(tree, hf_ceph_mdsmap, tvb, offset, len, TRUE );
	ceph_mdsmap_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);
	offset = dissect_ceph_fsid(tvb, ceph_mdsmap_tree, offset);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"epoch: %i",epoch);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"map length: %i",map_len);

	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"version: %i",version);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"m_epoch: %i",plop);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"m_client_epoch: %i",plop);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"m_last_failure: %i",plop);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"m_root: %i",plop);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"m_session_timeout: %i",plop);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"m_session_autoclose: %i",plop);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letoh64,"m_maxfile_size: %" G_GUINT64_FORMAT, max_filesize);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"m_max_mds: %i",plop);
	PROTO_ADD_SIMPLE_TEXT(ceph_mdsmap_tree,tvb_get_letohl,"n: %i",len);


	for (i = 0; i < len; i++){
		 offset = dissect_ceph_mdsmap_node(tvb, ceph_mdsmap_tree, offset);
	}
	offset = dissect_ceph_pgpools(tvb, ceph_mdsmap_tree, offset);
	return offset;
}

static guint32 dissect_ceph_front(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, guint32 offset, guint16 type)
{
	switch (type) {
/*	case CEPH_MSG_AUTH:
		offset = dissect_ceph_auth();
		break;*/
	case CEPH_MSG_AUTH_REPLY:
		offset = dissect_ceph_auth_reply(tvb, tree, offset);
		break;
	case CEPH_MSG_MON_MAP:
		offset = dissect_ceph_mon_map(tvb, tree, offset);
		break;
	case CEPH_MSG_MDS_MAP:
		offset = dissect_ceph_mds_map(tvb, tree, offset);
		break;
	case CEPH_MSG_STATFS:
		offset = dissect_ceph_mon_statfs(tvb, tree, offset);
		break;
	case CEPH_MSG_STATFS_REPLY:
		offset = dissect_ceph_mon_statfs_reply(tvb, tree, offset);
		break;
	case CEPH_MSG_CLIENT_REQUEST: /* mds request */
		offset = dissect_ceph_client_mds_request(tvb, pinfo, tree, offset);
		break;
	case CEPH_MSG_CLIENT_REPLY:
		offset = dissect_ceph_client_mds_reply(tvb, pinfo, tree, offset);
		break;
	case CEPH_MSG_CLIENT_LEASE:
		offset = dissect_ceph_client_mds_lease_request(tvb, pinfo, tree, offset);
		break;
	case CEPH_MSG_CLIENT_CAPS:
		offset = dissect_ceph_client_mds_caps_request(tvb, pinfo, tree, offset);
		break;
	default:
		break;
	}
	return offset;
}

static guint32 dissect_ceph_generic(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree, guint32 offset)
{
	proto_tree *ceph_header_tree;
	proto_item *ceph_sub_item = NULL;
	proto_item *ceph_item = proto_tree_get_parent(tree);
	guint32 front_len, middle_len, data_len;
	guint8 tag;
	guint32 hlen;
	guint32 orig_ofs = offset;
	guint16 type;
	guint64 seq;
	struct ceph_msg_header *header;
	unsigned int data_crc = 0;

	tag = tvb_get_guint8(tvb, offset);
	hlen = ( tag == CEPH_MSGR_TAG_ACK ) ? ACK_MSG_SIZE:0;
	hlen += sizeof(struct ceph_msg_header);
	hlen++;

	ceph_header_tree = proto_item_add_subtree(ceph_item, ett_ceph);

	ceph_sub_item = proto_tree_add_item( tree, hf_ceph_header, tvb, offset, hlen, TRUE );
	ceph_header_tree = proto_item_add_subtree(ceph_sub_item, ett_ceph);


	if (tag == CEPH_MSGR_TAG_ACK) {
		proto_tree_add_item(ceph_header_tree, hf_ceph_hdr_tag, tvb, offset, 1, TRUE);
		proto_tree_add_item(ceph_header_tree, hf_ceph_hdr_seq_ack, tvb, offset+1, 8, TRUE);
		offset += ACK_MSG_SIZE;
	}

	proto_tree_add_item(ceph_header_tree, hf_ceph_hdr_tag, tvb, offset, 1, TRUE);
	offset++;

	header = (struct ceph_msg_header *)tvb_get_ptr(tvb, offset, sizeof(struct ceph_msg_header));

	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_seq, header, seq);
	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_type, header, type);
	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_priority, header, priority);
	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_version, header, version);
	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_front_len, header, front_len);
	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_middle_len, header, middle_len);
	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_data_off, header, data_off);
	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_data_len, header, data_len);
	PROTO_ADD_ITEM(ceph_header_tree, struct ceph_msg_header, hf_ceph_hdr_crc, header, crc);

	// Bad location
	dissect_ceph_entity_name(tvb, ceph_header_tree, offset + offsetof(struct ceph_msg_header, src));
	//dissect_ceph_entity_inst(tvb, ceph_header_tree, offset + offsetof(struct ceph_msg_header, orig_src));

	type = TVB_MSG_FIELD(tvb_get_letohl, tvb, orig_ofs, type);
	seq = TVB_MSG_FIELD(tvb_get_letoh64, tvb, orig_ofs, seq);
	front_len = TVB_MSG_FIELD(tvb_get_letohs, tvb, orig_ofs, front_len);
	middle_len = TVB_MSG_FIELD(tvb_get_letohs, tvb, orig_ofs, middle_len);
	data_len = TVB_MSG_FIELD(tvb_get_letohl, tvb, orig_ofs, data_len);

	offset += sizeof(struct ceph_msg_header);

	if (front_len) {
		dissect_ceph_front(tvb, pinfo, tree, offset, type);
		offset += front_len;
	}

	if (middle_len) {
		 offset += middle_len;
	}

	if (data_len) {
		char *data;
		ceph_sub_item = proto_tree_add_item( tree, hf_ceph_data, tvb, offset, data_len, TRUE );
	    data = (char *)tvb_get_ptr(tvb, offset, data_len);
		data_crc = ceph_crc32c_le(0, data, data_len);
		offset += data_len;
	}
	offset = dissect_ceph_footer(tvb, tree, offset, data_crc);
	return offset;
}


static const char *ceph_protocol_to_text(guint32 proto){
	if ( proto == CEPH_OSD_PROTOCOL )
		return "osd";
	if ( proto == CEPH_MDS_PROTOCOL )
		return "mds";
	if ( proto == CEPH_MON_PROTOCOL )
		return "mon";
	if ( proto == CEPH_OSDC_PROTOCOL )
		return "osd";
	if ( proto == CEPH_MDSC_PROTOCOL )
		return "mds";
	if ( proto == CEPH_MONC_PROTOCOL )
		return "mon";
	else{
		return "???";
	}
}

static const char *entity_name_by_type(int type)
{
	if (type < 4)
		return "???";

	if (type < 20)
		return "mon";

	if (type < 30)
		return "mds";

	if (type < 50)
		return "osd";

	if (type < 0x300)
		return "???";

	if (type < 0x400)
		return "mds";

	return "???";
}

static void
dissect_ceph_client(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree)
{

	proto_item *ceph_item = NULL;
	proto_tree *ceph_tree = NULL;
	guint16 type = 0;
	guint32 proto = 0;
	const guchar *ptr;
	guint32 pos = 0;
	int have_banner = 0;
	
	if (check_col(pinfo->cinfo, COL_PROTOCOL))
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
	/* Clear out stuff in the info column */
	if(check_col(pinfo->cinfo,COL_INFO)){
		col_clear(pinfo->cinfo,COL_INFO);
	}

	ptr = tvb_get_ptr(tvb, pos, 9);
	if (ptr && memcmp(ptr, "ceph", 4) == 0) {
		have_banner = 1;
		pos += 9;
	}

	// This is not a good way of dissecting packets.  The tvb length should
	// be sanity checked so we aren't going past the actual size of the buffer.
	type = tvb_get_guint8(tvb,4); // Get the type byte


	if (check_col(pinfo->cinfo, COL_INFO)) {
		const char *entity_str = NULL;
			
		if (have_banner) {
			if (IS_MON(pinfo))
				entity_str = MON_STR;
			else{
				// Guess communication type from "struct ceph_msg_connect"
				proto = tvb_get_letohl(tvb,9 + sizeof(struct ceph_entity_addr) + 20);
				entity_str = ceph_protocol_to_text(proto);
			}
			col_add_fstr(pinfo->cinfo, COL_INFO, "[%s] Connect Request", entity_str);
		} else {
			type = TVB_MSG_FIELD(tvb_get_letohl, tvb, 0, type);
			entity_str = entity_name_by_type(type);
			col_add_fstr(pinfo->cinfo, COL_INFO, "[%s] %s",
			entity_str,
			val_to_str(type, packettypenames, "Unknown Type:0x%02x"));
		}
	}

	if (tree) { /* we are being asked for details */
		guint32 offset = 0;

		ceph_item = proto_tree_add_item(tree, proto_ceph, tvb, 0, -1, TRUE);
		ceph_tree = proto_item_add_subtree(ceph_item, ett_ceph);
		if (have_banner) { /* this is a connect message */
			offset = dissect_ceph_client_connect(tvb, ceph_tree, offset);
		} else {
			offset = dissect_ceph_generic(tvb, pinfo, ceph_tree, offset);
		}
	}
}

static void
dissect_ceph_server(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree)
{

	proto_item *ceph_item = NULL;
	proto_tree *ceph_tree = NULL;
	guint16 type = 0;
	const guchar *ptr;
	guint32 pos = 0;
	int have_banner = 0;

	if (check_col(pinfo->cinfo, COL_PROTOCOL))
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
	/* Clear out stuff in the info column */
	if(check_col(pinfo->cinfo,COL_INFO)){
		col_clear(pinfo->cinfo,COL_INFO);
	}

	ptr = tvb_get_ptr(tvb, pos, 9);
	if (ptr && memcmp(ptr, "ceph", 4) == 0) {
		have_banner = 1;
		pos += 9;
	}

	// This is not a good way of dissecting packets.  The tvb length should
	// be sanity checked so we aren't going past the actual size of the buffer.
	type = tvb_get_guint8( tvb, 4 ); // Get the type byte

	if (check_col(pinfo->cinfo, COL_INFO)) {
		const char *entity_str;
			if (IS_MON(pinfo))
				entity_str = MON_STR;
			else{
				entity_str = "???";
			}
		if (have_banner) {
			col_add_fstr(pinfo->cinfo, COL_INFO, "[%s] Connect Response", entity_str);
		} else {
			type = TVB_MSG_FIELD(tvb_get_letohl, tvb, 0, type);
			entity_str = entity_name_by_type(type);
			col_add_fstr(pinfo->cinfo, COL_INFO, "[%s] %s",
			entity_str,
			val_to_str(type, packettypenames, "Unknown Type:0x%02x"));
		}
	}

	if (tree) { /* we are being asked for details */
		guint32 offset = 0;

		ceph_item = proto_tree_add_item(tree, proto_ceph, tvb, 0, -1, TRUE);
		ceph_tree = proto_item_add_subtree(ceph_item, ett_ceph);

		if (have_banner) {
			offset = dissect_ceph_server_connect(tvb, ceph_tree, offset);
		} else {
			offset = dissect_ceph_generic(tvb, pinfo, ceph_tree, offset);
		}
	}
}

static void
dissect_ceph_message(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree)
{
	if (DEST_PORT_CEPH)
		dissect_ceph_client(tvb, pinfo, tree);
	else
		dissect_ceph_server(tvb, pinfo, tree);
}		

static guint dissect_ceph_acks(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree)
{
	guint32 offset = 0;

	if (check_col(pinfo->cinfo, COL_PROTOCOL))
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
	/* Clear out stuff in the info column */
	if(check_col(pinfo->cinfo,COL_INFO)){
		col_clear(pinfo->cinfo,COL_INFO);
		col_add_fstr(pinfo->cinfo, COL_INFO, "Ack");
	}
	if (tree) {
		proto_tree_add_item(tree, proto_ceph, tvb, 0, 5, TRUE);
		proto_tree_add_item(tree, hf_ceph_hdr_tag, tvb, offset, 1, TRUE);
		proto_tree_add_item(tree, hf_ceph_hdr_seq_ack, tvb, offset+1, 8, TRUE);
		offset += 9;
	}

	return offset;
}

/* determine PDU length of protocol ceph */
static guint get_ceph_message_len(packet_info *pinfo, tvbuff_t *tvb, int offset)
{
	const char *ptr;
	guint32 len;
	guint32 pos = 0;
	guint32 tlen = (int) tvb->length;
	ptr = tvb_get_ptr(tvb, offset, tlen - offset);
	//ptr = tvb_get_ptr(tvb, offset, /* sizeof(CEPH_BANNER) */ (tvb->length)-offset);
	if (ptr && memcmp(ptr, "ceph", 4) == 0) {
		if (DEST_PORT_CEPH) {
			len = sizeof(CEPH_BANNER) - 1 +
				sizeof(struct ceph_entity_addr) +
				sizeof(struct ceph_msg_connect) + AUTH_LEN;
		} else
			len = sizeof(CEPH_BANNER) - 1 +
				sizeof(struct ceph_entity_addr) +
				sizeof(struct ceph_entity_addr) +
				sizeof(struct ceph_msg_connect_reply);
		return len;
	}

	if (*ptr == CEPH_MSGR_TAG_ACK)
		pos = ACK_MSG_SIZE;

	len = pos + (guint)1 + sizeof(struct ceph_msg_header) +
		TVB_MSG_FIELD(tvb_get_letohl, tvb, offset, front_len) + 
		TVB_MSG_FIELD(tvb_get_letohl, tvb, offset, data_len) + 
		sizeof(struct ceph_msg_footer);
	
	if (!*ptr)
		return 0;
	return len;
}


static void dissect_ceph(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree){
	const char *ptr;
	ptr = tvb_get_ptr(tvb, 0, 6);
	if(*ptr == CEPH_MSGR_TAG_RESETSESSION){
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
		col_add_fstr(pinfo->cinfo, COL_INFO, "Ceph reset session");
	}
	else if(*ptr == CEPH_MSGR_TAG_WAIT){
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
		col_add_fstr(pinfo->cinfo, COL_INFO, "Ceph I am waiting");
	}
	else if(*ptr == CEPH_MSGR_TAG_RETRY_SESSION){
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
		col_add_fstr(pinfo->cinfo, COL_INFO, "Ceph retry session");
	}
	else if(*ptr == CEPH_MSGR_TAG_RETRY_GLOBAL){
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
		col_add_fstr(pinfo->cinfo, COL_INFO, "Ceph retry global");
	}
	else if(*ptr == CEPH_MSGR_TAG_CLOSE){
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
		col_add_fstr(pinfo->cinfo, COL_INFO, "Ceph close");
	}
	else if(*ptr == CEPH_MSGR_TAG_KEEPALIVE){
		col_set_str(pinfo->cinfo, COL_PROTOCOL, PROTO_TAG_CEPH);
		col_add_fstr(pinfo->cinfo, COL_INFO, "Ceph keep alive");
	}
	else if ((*ptr == CEPH_MSGR_TAG_MSG) ||
			(memcmp(ptr, CEPH_BANNER, 4) == 0) ||
			((ptr[0] == CEPH_MSGR_TAG_ACK) && (ptr[9] == CEPH_MSGR_TAG_MSG))
	){
		tcp_dissect_pdus(tvb, pinfo, tree, TRUE, TVB_MSG_HEADER_POS(src),
				get_ceph_message_len, dissect_ceph_message);
	} else {
		dissect_ceph_acks(tvb, pinfo, tree);
	}
}

