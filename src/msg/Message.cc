// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifdef ENCODE_DUMP
# include <typeinfo>
# include <cxxabi.h>
#endif

#include <iostream>
using namespace std;

#include "include/types.h"

#include "global/global_context.h"

#include "Message.h"

#include "messages/MPGStats.h"

#include "messages/MGenericMessage.h"

#include "messages/MPGStatsAck.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"


#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"

#include "messages/PaxosServiceMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"
#include "messages/MMonPaxos.h"

#include "messages/MMonProbe.h"
#include "messages/MMonJoin.h"
#include "messages/MMonElection.h"
#include "messages/MMonSync.h"
#include "messages/MMonScrub.h"

#include "messages/MLog.h"
#include "messages/MLogAck.h"

#include "messages/MPing.h"

#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"

#include "messages/MRoute.h"
#include "messages/MForward.h"

#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDMap.h"
#include "messages/MMonGetOSDMap.h"

#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGMissing.h"
#include "messages/MOSDScrub.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"

#include "messages/MRemoveSnaps.h"

#include "messages/MMonMap.h"
#include "messages/MMonGetMap.h"
#include "messages/MMonGetVersion.h"
#include "messages/MMonGetVersionReply.h"
#include "messages/MMonHealth.h"
#include "messages/MMonMetadata.h"
#include "messages/MDataPing.h"
#include "messages/MAuth.h"
#include "messages/MAuthReply.h"
#include "messages/MMonSubscribe.h"
#include "messages/MMonSubscribeAck.h"
#include "messages/MMonGlobalID.h"
#include "messages/MClientSession.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientReply.h"
#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientLease.h"
#include "messages/MClientSnap.h"
#include "messages/MClientQuota.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMDSResolve.h"
#include "messages/MMDSResolveAck.h"
#include "messages/MMDSCacheRejoin.h"
#include "messages/MMDSFindIno.h"
#include "messages/MMDSFindInoReply.h"
#include "messages/MMDSOpenIno.h"
#include "messages/MMDSOpenInoReply.h"

#include "messages/MDirUpdate.h"
#include "messages/MDiscover.h"
#include "messages/MDiscoverReply.h"

#include "messages/MMDSFragmentNotify.h"

#include "messages/MExportDirDiscover.h"
#include "messages/MExportDirDiscoverAck.h"
#include "messages/MExportDirCancel.h"
#include "messages/MExportDirPrep.h"
#include "messages/MExportDirPrepAck.h"
#include "messages/MExportDir.h"
#include "messages/MExportDirAck.h"
#include "messages/MExportDirNotify.h"
#include "messages/MExportDirNotifyAck.h"
#include "messages/MExportDirFinish.h"

#include "messages/MExportCaps.h"
#include "messages/MExportCapsAck.h"
#include "messages/MGatherCaps.h"


#include "messages/MDentryUnlink.h"
#include "messages/MDentryLink.h"

#include "messages/MHeartbeat.h"

#include "messages/MMDSTableRequest.h"

//#include "messages/MInodeUpdate.h"
#include "messages/MCacheExpire.h"
#include "messages/MInodeFileCaps.h"

#include "messages/MLock.h"

#include "messages/MWatchNotify.h"
#include "messages/MTimeCheck.h"

#include "common/config.h"

#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGPull.h"

#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"

#include "messages/MOSDPGUpdateLogMissing.h"
#include "messages/MOSDPGUpdateLogMissingReply.h"

#define DEBUGLVL  10    // debug level of output

#define dout_subsys ceph_subsys_ms

void Message::encode(uint64_t features, int crcflags)
{
  // encode and copy out of *m
  if (empty_payload()) {
    encode_payload(features);

    // if the encoder didn't specify past compatibility, we assume it
    // is incompatible.
    if (header.compat_version == 0)
      header.compat_version = header.version;
  }
  if (crcflags & MSG_CRC_HEADER)
    calc_front_crc();

  // update envelope
  header.front_len = get_payload().length();
  header.middle_len = get_middle().length();
  header.data_len = get_data().length();
  if (crcflags & MSG_CRC_HEADER)
    calc_header_crc();

  footer.flags = CEPH_MSG_FOOTER_COMPLETE;

  if (crcflags & MSG_CRC_DATA) {
    calc_data_crc();

#ifdef ENCODE_DUMP
    bufferlist bl;
    ::encode(get_header(), bl);

    // dump the old footer format
    ceph_msg_footer_old old_footer;
    old_footer.front_crc = footer.front_crc;
    old_footer.middle_crc = footer.middle_crc;
    old_footer.data_crc = footer.data_crc;
    old_footer.flags = footer.flags;
    ::encode(old_footer, bl);

    ::encode(get_payload(), bl);
    ::encode(get_middle(), bl);
    ::encode(get_data(), bl);

    // this is almost an exponential backoff, except because we count
    // bits we tend to sample things we encode later, which should be
    // more representative.
    static int i = 0;
    i++;
    int bits = 0;
    for (unsigned t = i; t; bits++)
      t &= t - 1;
    if (bits <= 2) {
      char fn[200];
      int status;
      snprintf(fn, sizeof(fn), ENCODE_STRINGIFY(ENCODE_DUMP) "/%s__%d.%x",
	       abi::__cxa_demangle(typeid(*this).name(), 0, 0, &status),
	       getpid(), i++);
      int fd = ::open(fn, O_WRONLY|O_TRUNC|O_CREAT, 0644);
      if (fd >= 0) {
	bl.write_fd(fd);
	::close(fd);
      }
    }
#endif
  } else {
    footer.flags = (unsigned)footer.flags | CEPH_MSG_FOOTER_NOCRC;
  }
}

void Message::dump(Formatter *f) const
{
  stringstream ss;
  print(ss);
  f->dump_string("summary", ss.str());
}

Message *decode_message(CephContext *cct, int crcflags,
			ceph_msg_header& header,
			ceph_msg_footer& footer,
			bufferlist& front, bufferlist& middle,
			bufferlist& data)
{
  // verify crc
  if (crcflags & MSG_CRC_HEADER) {
    __u32 front_crc = front.crc32c(0);
    __u32 middle_crc = middle.crc32c(0);

    if (front_crc != footer.front_crc) {
      if (cct) {
	ldout(cct, 0) << "bad crc in front " << front_crc << " != exp " << footer.front_crc << dendl;
	ldout(cct, 20) << " ";
	front.hexdump(*_dout);
	*_dout << dendl;
      }
      return 0;
    }
    if (middle_crc != footer.middle_crc) {
      if (cct) {
	ldout(cct, 0) << "bad crc in middle " << middle_crc << " != exp " << footer.middle_crc << dendl;
	ldout(cct, 20) << " ";
	middle.hexdump(*_dout);
	*_dout << dendl;
      }
      return 0;
    }
  }
  if (crcflags & MSG_CRC_DATA) {
    if ((footer.flags & CEPH_MSG_FOOTER_NOCRC) == 0) {
      __u32 data_crc = data.crc32c(0);
      if (data_crc != footer.data_crc) {
	if (cct) {
	  ldout(cct, 0) << "bad crc in data " << data_crc << " != exp " << footer.data_crc << dendl;
	  ldout(cct, 20) << " ";
	  data.hexdump(*_dout);
	  *_dout << dendl;
	}
	return 0;
      }
    }
  }

  // make message
  int type = header.type;
  Message *m = MessageFactory::get().produce(type);
  if(m == nullptr){
    if (cct) {
      ldout(cct, 0) << "can't decode unknown message type " << type << " MSG_AUTH=" << CEPH_MSG_AUTH << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
        assert(0);
    }
    return 0;
  }

  m->set_cct(cct);

  // m->header.version, if non-zero, should be populated with the
  // newest version of the encoding the code supports.  If set, check
  // it against compat_version.
  if (m->get_header().version &&
      m->get_header().version < header.compat_version) {
    if (cct) {
      ldout(cct, 0) << "will not decode message of type " << type
		    << " version " << header.version
		    << " because compat_version " << header.compat_version
		    << " > supported version " << m->get_header().version << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	assert(0);
    }
    m->put();
    return 0;
  }

  m->set_header(header);
  m->set_footer(footer);
  m->set_payload(front);
  m->set_middle(middle);
  m->set_data(data);

  try {
    m->decode_payload();
  }
  catch (const buffer::error &e) {
    if (cct) {
      lderr(cct) << "failed to decode message of type " << type
		 << " v" << header.version
		 << ": " << e.what() << dendl;
      ldout(cct, cct->_conf->ms_dump_corrupt_message_level) << "dump: \n";
      m->get_payload().hexdump(*_dout);
      *_dout << dendl;
      if (cct->_conf->ms_die_on_bad_msg)
	assert(0);
    }
    m->put();
    return 0;
  }

  // done!
  return m;
}


// This routine is not used for ordinary messages, but only when encapsulating a message
// for forwarding and routing.  It's also used in a backward compatibility test, which only
// effectively tests backward compability for those functions.  To avoid backward compatibility
// problems, we currently always encode and decode using the old footer format that doesn't
// allow for message authentication.  Eventually we should fix that.  PLR

void encode_message(Message *msg, uint64_t features, bufferlist& payload)
{
  bufferlist front, middle, data;
  ceph_msg_footer_old old_footer;
  ceph_msg_footer footer;
  msg->encode(features, MSG_CRC_ALL);
  ::encode(msg->get_header(), payload);

  // Here's where we switch to the old footer format.  PLR

  footer = msg->get_footer();
  old_footer.front_crc = footer.front_crc;   
  old_footer.middle_crc = footer.middle_crc;   
  old_footer.data_crc = footer.data_crc;   
  old_footer.flags = footer.flags;   
  ::encode(old_footer, payload);

  ::encode(msg->get_payload(), payload);
  ::encode(msg->get_middle(), payload);
  ::encode(msg->get_data(), payload);
}

// See above for somewhat bogus use of the old message footer.  We switch to the current footer
// after decoding the old one so the other form of decode_message() doesn't have to change.
// We've slipped in a 0 signature at this point, so any signature checking after this will
// fail.  PLR

Message *decode_message(CephContext *cct, int crcflags, bufferlist::iterator& p)
{
  ceph_msg_header h;
  ceph_msg_footer_old fo;
  ceph_msg_footer f;
  bufferlist fr, mi, da;
  ::decode(h, p);
  ::decode(fo, p);
  f.front_crc = fo.front_crc;
  f.middle_crc = fo.middle_crc;
  f.data_crc = fo.data_crc;
  f.flags = fo.flags;
  f.sig = 0;
  ::decode(fr, p);
  ::decode(mi, p);
  ::decode(da, p);
  return decode_message(cct, crcflags, h, f, fr, mi, da);
}

