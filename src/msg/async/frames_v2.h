#ifndef _MSG_ASYNC_FRAMES_V2_
#define _MSG_ASYNC_FRAMES_V2_

#include "include/types.h"
#include "common/Clock.h"
#include "crypto_onwire.h"
#include <array>
#include <utility>

#include <boost/container/static_vector.hpp>

/**
 * Protocol V2 Frame Structures
 * 
 * Documentation in: doc/dev/msgr2.rst
 **/

namespace ceph::msgr::v2 {

// We require these features from any peer, period, in order to encode
// a entity_addrvec_t.
const uint64_t msgr2_required = CEPH_FEATUREMASK_MSG_ADDR2;

// We additionally assume the peer has the below features *purely for
// the purpose of encoding the frames themselves*.  The only complex
// types in the frames are entity_addr_t and entity_addrvec_t, and we
// specifically want the peer to understand the (new in nautilus)
// TYPE_ANY.  We treat narrow this assumption to frames because we
// expect there may be future clients (the kernel) that understand
// msgr v2 and understand this encoding but don't necessarily have
// everything else that SERVER_NAUTILUS implies.  Yes, a fresh feature
// bit would be a cleaner approach, but those are scarce these days.
const uint64_t msgr2_frame_assumed =
		   msgr2_required |
		   CEPH_FEATUREMASK_SERVER_NAUTILUS;

enum class Tag : __u8 {
  HELLO = 1,
  AUTH_REQUEST,
  AUTH_BAD_METHOD,
  AUTH_REPLY_MORE,
  AUTH_REQUEST_MORE,
  AUTH_DONE,
  AUTH_SIGNATURE,
  CLIENT_IDENT,
  SERVER_IDENT,
  IDENT_MISSING_FEATURES,
  SESSION_RECONNECT,
  SESSION_RESET,
  SESSION_RETRY,
  SESSION_RETRY_GLOBAL,
  SESSION_RECONNECT_OK,
  WAIT,
  MESSAGE,
  KEEPALIVE2,
  KEEPALIVE2_ACK,
  ACK
};

struct segment_t {
  // TODO: this will be dropped with support for `allocation policies`.
  // We need them because of the rx_buffers zero-copy optimization.
  static constexpr __u16 PAGE_SIZE_ALIGNMENT = 4096;

  static constexpr __u16 DEFAULT_ALIGNMENT = sizeof(void *);

  ceph_le32 length;
  ceph_le16 alignment;
} __attribute__((packed));

struct SegmentIndex {
  struct Msg {
    static constexpr std::size_t HEADER = 0;
    static constexpr std::size_t FRONT = 1;
    static constexpr std::size_t MIDDLE = 2;
    static constexpr std::size_t DATA = 3;
  };

  struct Control {
    static constexpr std::size_t PAYLOAD = 0;
  };
};

static constexpr uint8_t CRYPTO_BLOCK_SIZE { 16 };

static constexpr std::size_t MAX_NUM_SEGMENTS = 4;

// V2 preamble consists of one or more preamble blocks depending on
// the number of segments a particular frame needs. Each block holds
// up to MAX_NUM_SEGMENTS segments and has its own CRC.
//
// XXX: currently the multi-segment facility is NOT implemented.
struct preamble_block_t {  
  // Tag. For multi-segmented frames the value is the same
  // between subsequent preamble blocks.
  __u8 tag;

  // Number of segments to go in entire frame. First preable block has
  // set this to just #segments, second #segments - MAX_NUM_SEGMENTS,
  // third to #segments - MAX_NUM_SEGMENTS and so on.
  __u8 num_segments;

  segment_t segments[MAX_NUM_SEGMENTS];
  __u8 _reserved[2];

  // CRC32 for this single preamble block.
  ceph_le32 crc;
} __attribute__((packed));
static_assert(sizeof(preamble_block_t) % CRYPTO_BLOCK_SIZE == 0);
static_assert(std::is_standard_layout<preamble_block_t>::value);

// Each Frame has an epilogue for integrity or authenticity validation.
// For plain mode it's quite straightforward - the structure stores up
// to MAX_NUM_SEGMENTS crc32 checksums, one per each segment.
// For secure mode things become very different. The fundamental thing
// is that epilogue format is **an implementation detail of particular
// cipher**. ProtocolV2 only knows:
//   * where the data is placed (always at the end of ciphertext),
//   * how long it is. RxHandler provides get_extra_size_at_final() but
//     ProtocolV2 has NO WAY to alter this.
//
// The intention behind the contract is to provide flexibility of cipher
// selection. Currently AES in GCM mode is used and epilogue conveys its
// *auth tag* (following OpenSSL's terminology). However, it would be OK
// to switch to e.g. AES128-CBC + HMAC-SHA512 without affecting protocol
// (expect the cipher negotiation, of course).
//
// In addition to integrity/authenticity data each variant of epilogue
// conveys late_flags. The initial user of this field will be the late
// frame abortion facility.
struct epilogue_plain_block_t {
  __u8 late_flags;
  ceph_le32 crc_values[MAX_NUM_SEGMENTS];
} __attribute__((packed));
static_assert(std::is_standard_layout<epilogue_plain_block_t>::value);

struct epilogue_secure_block_t {
  __u8 late_flags;
  __u8 padding[CRYPTO_BLOCK_SIZE - sizeof(late_flags)];

  __u8 ciphers_private_data[];
} __attribute__((packed));
static_assert(sizeof(epilogue_secure_block_t) % CRYPTO_BLOCK_SIZE == 0);
static_assert(std::is_standard_layout<epilogue_secure_block_t>::value);


static constexpr uint32_t FRAME_PREAMBLE_SIZE = sizeof(preamble_block_t);
static constexpr uint32_t FRAME_PLAIN_EPILOGUE_SIZE =
    sizeof(epilogue_plain_block_t);
static constexpr uint32_t FRAME_SECURE_EPILOGUE_SIZE =
    sizeof(epilogue_secure_block_t);

#define FRAME_FLAGS_LATEABRT      (1<<0)   /* frame was aborted after txing data */

static uint32_t segment_onwire_size(const uint32_t logical_size)
{
  return p2roundup<uint32_t>(logical_size, CRYPTO_BLOCK_SIZE);
}

static inline ceph::bufferlist segment_onwire_bufferlist(ceph::bufferlist&& bl)
{
  const auto padding_size = segment_onwire_size(bl.length()) - bl.length();
  if (padding_size) {
    bl.append_zero(padding_size);
  }
  return std::move(bl);
}

template <class T, uint16_t... SegmentAlignmentVs>
struct Frame {
  static constexpr size_t SegmentsNumV = sizeof...(SegmentAlignmentVs);
  static_assert(SegmentsNumV > 0 && SegmentsNumV <= MAX_NUM_SEGMENTS);
protected:
  std::array<ceph::bufferlist, SegmentsNumV> segments;

private:
  static constexpr std::array<uint16_t, SegmentsNumV> alignments {
    SegmentAlignmentVs...
  };
  ceph::bufferlist::contiguous_filler preamble_filler;

  __u8 calc_num_segments(const segment_t segments[])
  {
    for (__u8 num = SegmentsNumV; num > 0; num--) {
      if (segments[num-1].length) {
        return num;
      }
    }
    // frame always has at least one segment.
    return 1;
  }

  // craft the main preamble. It's always present regardless of the number
  // of segments message is composed from.
  void fill_preamble() {
    ceph_assert(std::size(segments) <= MAX_NUM_SEGMENTS);

    preamble_block_t main_preamble;
    // FIPS zeroization audit 20191115: this memset is not security related.
    ::memset(&main_preamble, 0, sizeof(main_preamble));

    main_preamble.tag = static_cast<__u8>(T::tag);
    ceph_assert(main_preamble.tag != 0);

    // implementation detail: the first bufferlist of Frame::segments carries
    // space for preamble. This glueing isn't a part of the onwire format but
    // just our private detail.
    main_preamble.segments[0].length =
        segments[0].length() - FRAME_PREAMBLE_SIZE;
    main_preamble.segments[0].alignment = alignments[0];

    // there is no business in issuing frame without at least one segment
    // filled.
    if constexpr(SegmentsNumV > 1) {
      for (__u8 idx = 1; idx < SegmentsNumV; idx++) {
        main_preamble.segments[idx].length = segments[idx].length();
        main_preamble.segments[idx].alignment = alignments[idx];
      }
    }
    // calculate the number of non-empty segments.
    // TODO: reorder segments to get DATA first
    main_preamble.num_segments = calc_num_segments(main_preamble.segments);

    main_preamble.crc =
        ceph_crc32c(0, reinterpret_cast<unsigned char *>(&main_preamble),
                    sizeof(main_preamble) - sizeof(main_preamble.crc));

    preamble_filler.copy_in(sizeof(main_preamble),
                            reinterpret_cast<const char *>(&main_preamble));
  }

  template <size_t... Is>
  void reset_tx_handler(
    ceph::crypto::onwire::rxtx_t &session_stream_handlers,
    std::index_sequence<Is...>)
  {
    session_stream_handlers.tx->reset_tx_handler({ segments[Is].length()...,
                                                   sizeof(epilogue_secure_block_t) });
  }

public:
  ceph::bufferlist get_buffer(
    ceph::crypto::onwire::rxtx_t &session_stream_handlers)
  {
    fill_preamble();
    if (session_stream_handlers.tx) {
      // we're padding segments to biggest cipher's block size. Although
      // AES-GCM can live without that as it's a stream cipher, we don't
      // to be fixed to stream ciphers only.
      for (auto& segment : segments) {
        segment = segment_onwire_bufferlist(std::move(segment));
      }

      // let's cipher allocate one huge buffer for entire ciphertext.
      reset_tx_handler(
          session_stream_handlers, std::make_index_sequence<SegmentsNumV>());

      for (auto& segment : segments) {
        if (segment.length()) {
          session_stream_handlers.tx->authenticated_encrypt_update(
            std::move(segment));
        }
      }

      // in secure mode we craft only the late_flags. Signature (for AES-GCM
      // called auth tag) will be added by the cipher.
      {
        epilogue_secure_block_t epilogue;
        // FIPS zeroization audit 20191115: this memset is not security
        // related.
        ::memset(&epilogue, 0, sizeof(epilogue));
        ceph::bufferlist epilogue_bl;
        epilogue_bl.append(reinterpret_cast<const char*>(&epilogue),
                           sizeof(epilogue));
        session_stream_handlers.tx->authenticated_encrypt_update(epilogue_bl);
      }
      return session_stream_handlers.tx->authenticated_encrypt_final();
    } else {
      // plain mode
      epilogue_plain_block_t epilogue;
      // FIPS zeroization audit 20191115: this memset is not security related.
      ::memset(&epilogue, 0, sizeof(epilogue));

      ceph::bufferlist::const_iterator hdriter(&segments.front(),
                                               FRAME_PREAMBLE_SIZE);
      epilogue.crc_values[SegmentIndex::Control::PAYLOAD] =
          hdriter.crc32c(hdriter.get_remaining(), -1);
      if constexpr(SegmentsNumV > 1) {
        for (__u8 idx = 1; idx < SegmentsNumV; idx++) {
          epilogue.crc_values[idx] = segments[idx].crc32c(-1);
        }
      }

      ceph::bufferlist ret;
      for (auto& segment : segments) {
        ret.claim_append(segment);
      }
      ret.append(reinterpret_cast<const char*>(&epilogue), sizeof(epilogue));
      return ret;
    }
  }

  Frame()
    : preamble_filler(segments.front().append_hole(FRAME_PREAMBLE_SIZE)) {
  }

public:
};


// ControlFrames are used to manage transceiver state (like connections) and
// orchestrate transfers of MessageFrames. They use only single segment with
// marshalling facilities -- derived classes specify frame structure through
// Args pack while ControlFrame provides common encode/decode machinery.
template <class C, typename... Args>
class ControlFrame : public Frame<C, segment_t::DEFAULT_ALIGNMENT /* single segment */> {
protected:
  ceph::bufferlist &get_payload_segment() {
    return this->segments[SegmentIndex::Control::PAYLOAD];
  }

  // this tuple is only used when decoding values from a payload segment
  std::tuple<Args...> _values;

  // FIXME: for now, we assume specific features for the purpoess of encoding
  // the frames themselves (*not* messages in message frames!).
  uint64_t features = msgr2_frame_assumed;

  template <typename T>
  inline void _encode_payload_each(T &t) {
    if constexpr (std::is_same<T, std::vector<uint32_t> const>()) {
      encode((uint32_t)t.size(), this->get_payload_segment(), features);
      for (const auto &elem : t) {
        encode(elem, this->get_payload_segment(), features);
      }
    } else {
      encode(t, this->get_payload_segment(), features);
    }
  }

  template <typename T>
  inline void _decode_payload_each(T &t, bufferlist::const_iterator &ti) const {
    if constexpr (std::is_same<T, std::vector<uint32_t>>()) {
      uint32_t size;
      decode(size, ti);
      t.resize(size);
      for (uint32_t i = 0; i < size; ++i) {
        decode(t[i], ti);
      }
    } else {
      decode(t, ti);
    }
  }

  template <std::size_t... Is>
  inline void _decode_payload(bufferlist::const_iterator &ti,
                              std::index_sequence<Is...>) const {
    (_decode_payload_each((Args &)std::get<Is>(_values), ti), ...);
  }

  template <std::size_t N>
  inline decltype(auto) get_val() {
    return std::get<N>(_values);
  }

  ControlFrame()
    : Frame<C, segment_t::DEFAULT_ALIGNMENT /* single segment */>() {
  }

  void _encode(const Args &... args) {
    (_encode_payload_each(args), ...);
  }

  void _decode(const ceph::bufferlist &bl) {
    auto ti = bl.cbegin();
    _decode_payload(ti, std::index_sequence_for<Args...>());
  }

public:
  static C Encode(const Args &... args) {
    C c;
    c._encode(args...);
    return c;
  }

  static C Decode(const ceph::bufferlist &payload) {
    C c;
    c._decode(payload);
    return c;
  }
};

struct HelloFrame : public ControlFrame<HelloFrame,
                                        uint8_t,          // entity type
                                        entity_addr_t> {  // peer address
  static const Tag tag = Tag::HELLO;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint8_t &entity_type() { return get_val<0>(); }
  inline entity_addr_t &peer_addr() { return get_val<1>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct AuthRequestFrame : public ControlFrame<AuthRequestFrame,
                                              uint32_t, // auth method
                                              std::vector<uint32_t>, // preferred modes
                                              bufferlist> { // auth payload
  static const Tag tag = Tag::AUTH_REQUEST;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint32_t &method() { return get_val<0>(); }
  inline std::vector<uint32_t> &preferred_modes() { return get_val<1>(); }
  inline bufferlist &auth_payload() { return get_val<2>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct AuthBadMethodFrame : public ControlFrame<AuthBadMethodFrame,
                                                uint32_t, // method
                                                int32_t,  // result
                                                std::vector<uint32_t>,   // allowed methods
                                                std::vector<uint32_t>> { // allowed modes
  static const Tag tag = Tag::AUTH_BAD_METHOD;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint32_t &method() { return get_val<0>(); }
  inline int32_t &result() { return get_val<1>(); }
  inline std::vector<uint32_t> &allowed_methods() { return get_val<2>(); }
  inline std::vector<uint32_t> &allowed_modes() { return get_val<3>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct AuthReplyMoreFrame : public ControlFrame<AuthReplyMoreFrame,
                                                bufferlist> { // auth payload
  static const Tag tag = Tag::AUTH_REPLY_MORE;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline bufferlist &auth_payload() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct AuthRequestMoreFrame : public ControlFrame<AuthRequestMoreFrame,
                                                  bufferlist> { // auth payload
  static const Tag tag = Tag::AUTH_REQUEST_MORE;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline bufferlist &auth_payload() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct AuthDoneFrame : public ControlFrame<AuthDoneFrame,
                                           uint64_t, // global id
                                           uint32_t, // connection mode
                                           bufferlist> { // auth method payload
  static const Tag tag = Tag::AUTH_DONE;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint64_t &global_id() { return get_val<0>(); }
  inline uint32_t &con_mode() { return get_val<1>(); }
  inline bufferlist &auth_payload() { return get_val<2>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct AuthSignatureFrame
    : public ControlFrame<AuthSignatureFrame,
                          sha256_digest_t> {
  static const Tag tag = Tag::AUTH_SIGNATURE;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline sha256_digest_t &signature() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct ClientIdentFrame
    : public ControlFrame<ClientIdentFrame,
                          entity_addrvec_t,  // my addresses
                          entity_addr_t,  // target address
                          int64_t,  // global_id
                          uint64_t,  // global seq
                          uint64_t,  // supported features
                          uint64_t,  // required features
                          uint64_t,  // flags
                          uint64_t> {  // client cookie
  static const Tag tag = Tag::CLIENT_IDENT;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline entity_addr_t &target_addr() { return get_val<1>(); }
  inline int64_t &gid() { return get_val<2>(); }
  inline uint64_t &global_seq() { return get_val<3>(); }
  inline uint64_t &supported_features() { return get_val<4>(); }
  inline uint64_t &required_features() { return get_val<5>(); }
  inline uint64_t &flags() { return get_val<6>(); }
  inline uint64_t &cookie() { return get_val<7>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct ServerIdentFrame
    : public ControlFrame<ServerIdentFrame,
                          entity_addrvec_t,  // my addresses
                          int64_t,  // global_id
                          uint64_t,  // global seq
                          uint64_t,  // supported features
                          uint64_t,  // required features
                          uint64_t,  // flags
                          uint64_t> {  // server cookie
  static const Tag tag = Tag::SERVER_IDENT;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline int64_t &gid() { return get_val<1>(); }
  inline uint64_t &global_seq() { return get_val<2>(); }
  inline uint64_t &supported_features() { return get_val<3>(); }
  inline uint64_t &required_features() { return get_val<4>(); }
  inline uint64_t &flags() { return get_val<5>(); }
  inline uint64_t &cookie() { return get_val<6>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct ReconnectFrame
    : public ControlFrame<ReconnectFrame,
                          entity_addrvec_t,  // my addresses
                          uint64_t,  // client cookie
                          uint64_t,  // server cookie
                          uint64_t,  // global sequence
                          uint64_t,  // connect sequence
                          uint64_t> { // message sequence
  static const Tag tag = Tag::SESSION_RECONNECT;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline uint64_t &client_cookie() { return get_val<1>(); }
  inline uint64_t &server_cookie() { return get_val<2>(); }
  inline uint64_t &global_seq() { return get_val<3>(); }
  inline uint64_t &connect_seq() { return get_val<4>(); }
  inline uint64_t &msg_seq() { return get_val<5>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct ResetFrame : public ControlFrame<ResetFrame,
                                        bool> {  // full reset
  static const Tag tag = Tag::SESSION_RESET;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline bool &full() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct RetryFrame : public ControlFrame<RetryFrame,
                                        uint64_t> {  // connection seq
  static const Tag tag = Tag::SESSION_RETRY;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint64_t &connect_seq() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct RetryGlobalFrame : public ControlFrame<RetryGlobalFrame,
                                              uint64_t> { // global seq
  static const Tag tag = Tag::SESSION_RETRY_GLOBAL;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint64_t &global_seq() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct WaitFrame : public ControlFrame<WaitFrame> {
  static const Tag tag = Tag::WAIT;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

protected:
  using ControlFrame::ControlFrame;
};

struct ReconnectOkFrame : public ControlFrame<ReconnectOkFrame,
                                              uint64_t> { // message seq
  static const Tag tag = Tag::SESSION_RECONNECT_OK;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint64_t &msg_seq() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct IdentMissingFeaturesFrame 
    : public ControlFrame<IdentMissingFeaturesFrame,
                          uint64_t> { // missing features mask
  static const Tag tag = Tag::IDENT_MISSING_FEATURES;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint64_t &features() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct KeepAliveFrame : public ControlFrame<KeepAliveFrame,
                                            utime_t> {  // timestamp
  static const Tag tag = Tag::KEEPALIVE2;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  static KeepAliveFrame Encode() {
    return KeepAliveFrame::Encode(ceph_clock_now());
  }

  inline utime_t &timestamp() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct KeepAliveFrameAck : public ControlFrame<KeepAliveFrameAck,
                                               utime_t> { // ack timestamp
  static const Tag tag = Tag::KEEPALIVE2_ACK;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline utime_t &timestamp() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

struct AckFrame : public ControlFrame<AckFrame,
                                      uint64_t> { // message sequence
  static const Tag tag = Tag::ACK;
  using ControlFrame::Encode;
  using ControlFrame::Decode;

  inline uint64_t &seq() { return get_val<0>(); }

protected:
  using ControlFrame::ControlFrame;
};

using segment_bls_t =
    boost::container::static_vector<bufferlist, MAX_NUM_SEGMENTS>;

// This class is used for encoding/decoding header of the message frame.
// Body is processed almost independently with the sole junction point
// being the `extra_payload_len` passed to get_buffer().
struct MessageFrame : public Frame<MessageFrame,
                                   /* four segments */
                                   segment_t::DEFAULT_ALIGNMENT,
                                   segment_t::DEFAULT_ALIGNMENT,
                                   segment_t::DEFAULT_ALIGNMENT,
                                   segment_t::PAGE_SIZE_ALIGNMENT> {
  static const Tag tag = Tag::MESSAGE;

  static MessageFrame Encode(const ceph_msg_header2 &msg_header,
                             const ceph::bufferlist &front,
                             const ceph::bufferlist &middle,
                             const ceph::bufferlist &data) {
    MessageFrame f;
    f.segments[SegmentIndex::Msg::HEADER].append(
        reinterpret_cast<const char*>(&msg_header), sizeof(msg_header));

    f.segments[SegmentIndex::Msg::FRONT] = front;
    f.segments[SegmentIndex::Msg::MIDDLE] = middle;
    f.segments[SegmentIndex::Msg::DATA] = data;

    return f;
  }

  static MessageFrame Decode(segment_bls_t& recv_segments) {
    MessageFrame f;
    // transfer segments' bufferlists. If a MessageFrame contains less
    // SegmentsNumV segments, the missing ones will be seen as zeroed.
    for (__u8 idx = 0; idx < std::size(recv_segments); idx++) {
      f.segments[idx] = std::move(recv_segments[idx]);
    }
    return f;
  }

  inline const ceph_msg_header2 &header() {
    auto& hdrbl = segments[SegmentIndex::Msg::HEADER];
    return reinterpret_cast<const ceph_msg_header2&>(*hdrbl.c_str());
  }

  ceph::bufferlist &front() {
    return segments[SegmentIndex::Msg::FRONT];
  }

  ceph::bufferlist &middle() {
    return segments[SegmentIndex::Msg::MIDDLE];
  }

  ceph::bufferlist &data() {
    return segments[SegmentIndex::Msg::DATA];
  }

  uint32_t front_len() const {
    return segments[SegmentIndex::Msg::FRONT].length();
  }

  uint32_t middle_len() const {
    return segments[SegmentIndex::Msg::MIDDLE].length();
  }

  uint32_t data_len() const {
    return segments[SegmentIndex::Msg::DATA].length();
  }

protected:
  using Frame::Frame;
};

} // namespace ceph::msgr::v2

#endif // _MSG_ASYNC_FRAMES_V2_
