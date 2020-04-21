#ifndef _MSG_ASYNC_FRAMES_V2_
#define _MSG_ASYNC_FRAMES_V2_

#include "include/types.h"
#include "common/Clock.h"
#include "crypto_onwire.h"
#include <array>
#include <iosfwd>
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

struct epilogue_crc_rev0_block_t {
  __u8 late_flags;  // FRAME_LATE_FLAG_ABORTED
  ceph_le32 crc_values[MAX_NUM_SEGMENTS];
} __attribute__((packed));
static_assert(std::is_standard_layout_v<epilogue_crc_rev0_block_t>);

struct epilogue_crc_rev1_block_t {
  __u8 late_status;  // FRAME_LATE_STATUS_*
  ceph_le32 crc_values[MAX_NUM_SEGMENTS - 1];
} __attribute__((packed));
static_assert(std::is_standard_layout_v<epilogue_crc_rev1_block_t>);

struct epilogue_secure_rev0_block_t {
  __u8 late_flags;  // FRAME_LATE_FLAG_ABORTED
  __u8 padding[CRYPTO_BLOCK_SIZE - sizeof(late_flags)];
} __attribute__((packed));
static_assert(sizeof(epilogue_secure_rev0_block_t) % CRYPTO_BLOCK_SIZE == 0);
static_assert(std::is_standard_layout_v<epilogue_secure_rev0_block_t>);

// epilogue_secure_rev0_block_t with late_flags changed to late_status
struct epilogue_secure_rev1_block_t {
  __u8 late_status;  // FRAME_LATE_STATUS_*
  __u8 padding[CRYPTO_BLOCK_SIZE - sizeof(late_status)];
} __attribute__((packed));
static_assert(sizeof(epilogue_secure_rev1_block_t) % CRYPTO_BLOCK_SIZE == 0);
static_assert(std::is_standard_layout_v<epilogue_secure_rev1_block_t>);

static constexpr uint32_t FRAME_CRC_SIZE = 4;
static constexpr uint32_t FRAME_PREAMBLE_INLINE_SIZE = 48;
static_assert(FRAME_PREAMBLE_INLINE_SIZE % CRYPTO_BLOCK_SIZE == 0);
// just for performance, nothing should break otherwise
static_assert(sizeof(ceph_msg_header2) <= FRAME_PREAMBLE_INLINE_SIZE);
static constexpr uint32_t FRAME_PREAMBLE_WITH_INLINE_SIZE =
    sizeof(preamble_block_t) + FRAME_PREAMBLE_INLINE_SIZE;

// A frame can be aborted by the sender after transmitting the
// preamble and the first segment.  The remainder of the frame
// is filled with zeros, up until the epilogue.
//
// This flag is for msgr2.0.  Note that in crc mode, late_flags
// is not covered by any crc -- a single bit flip can result in
// a completed frame being dropped or in an aborted frame with
// garbage segment payloads being dispatched.
#define FRAME_LATE_FLAG_ABORTED           (1<<0)

// For msgr2.1, FRAME_LATE_STATUS_ABORTED has the same meaning
// as FRAME_LATE_FLAG_ABORTED and late_status replaces late_flags.
// Bit error detection in crc mode is achieved by using a 4-bit
// nibble per flag with two code words that are far apart in terms
// of Hamming Distance (HD=4, same as provided by CRC32-C for
// input lengths over ~5K).
#define FRAME_LATE_STATUS_ABORTED         0x1
#define FRAME_LATE_STATUS_COMPLETE        0xe
#define FRAME_LATE_STATUS_ABORTED_MASK    0xf

#define FRAME_LATE_STATUS_RESERVED_TRUE   0x10
#define FRAME_LATE_STATUS_RESERVED_FALSE  0xe0
#define FRAME_LATE_STATUS_RESERVED_MASK   0xf0

struct FrameError : std::runtime_error {
  using runtime_error::runtime_error;
};

class FrameAssembler {
public:
  // crypto must be non-null
  FrameAssembler(const ceph::crypto::onwire::rxtx_t* crypto, bool is_rev1)
      : m_crypto(crypto), m_is_rev1(is_rev1) {}

  void set_is_rev1(bool is_rev1) {
    m_descs.clear();
    m_is_rev1 = is_rev1;
  }

  size_t get_num_segments() const {
    ceph_assert(!m_descs.empty());
    return m_descs.size();
  }

  uint32_t get_segment_logical_len(size_t seg_idx) const {
    ceph_assert(seg_idx < m_descs.size());
    return m_descs[seg_idx].logical_len;
  }

  uint16_t get_segment_align(size_t seg_idx) const {
    ceph_assert(seg_idx < m_descs.size());
    return m_descs[seg_idx].align;
  }

  // Preamble:
  //
  //   preamble_block_t
  //   [preamble inline buffer + auth tag -- only in msgr2.1 secure mode]
  //
  // The preamble is generated unconditionally.
  //
  // In msgr2.1 secure mode, the first segment is inlined into the
  // preamble inline buffer, either fully or partially.
  uint32_t get_preamble_onwire_len() const {
    if (m_is_rev1 && m_crypto->rx) {
      return FRAME_PREAMBLE_WITH_INLINE_SIZE + get_auth_tag_len();
    }
    return sizeof(preamble_block_t);
  }

  // Segment:
  //
  //   segment payload
  //   [zero padding -- only in secure mode]
  //   [crc or auth tag -- only in msgr2.1, only for the first segment]
  //
  // For an empty segment, nothing is generated.  In msgr2.1 secure
  // mode, if the first segment gets fully inlined into the preamble
  // inline buffer, it is considered empty.
  uint32_t get_segment_onwire_len(size_t seg_idx) const {
    ceph_assert(seg_idx < m_descs.size());
    if (m_crypto->rx) {
      uint32_t padded_len = get_segment_padded_len(seg_idx);
      if (m_is_rev1 && seg_idx == 0) {
        if (padded_len > FRAME_PREAMBLE_INLINE_SIZE) {
          return padded_len + get_auth_tag_len() - FRAME_PREAMBLE_INLINE_SIZE;
        }
        return 0;
      }
      return padded_len;
    }
    if (m_is_rev1 && seg_idx == 0 && m_descs[0].logical_len > 0) {
      return m_descs[0].logical_len + FRAME_CRC_SIZE;
    }
    return m_descs[seg_idx].logical_len;
  }

  // Epilogue:
  //
  //   epilogue_*_block_t
  //   [auth tag -- only in secure mode]
  //
  // For msgr2.0, the epilogue is generated unconditionally.  In
  // crc mode, it stores crcs for all segments; the preamble is
  // covered by its own crc.  In secure mode, the epilogue auth tag
  // covers the whole frame.
  //
  // For msgr2.1, the epilogue is generated only if the frame has
  // more than one segment (i.e. at least one of second to fourth
  // segments is not empty).  In crc mode, it stores crcs for
  // second to fourh segments; the preamble and the first segment
  // are covered by their own crcs.  In secure mode, the epilogue
  // auth tag covers second to fourth segments; the preamble and the
  // first segment (if not fully inlined into the preamble inline
  // buffer) are covered by their own auth tags.
  //
  // Note that the auth tag format is an implementation detail of a
  // particular cipher.  FrameAssembler is concerned only with where
  // the auth tag is placed (at the end of the ciphertext) and how
  // long it is (RxHandler::get_extra_size_at_final()).  This is to
  // provide room for other encryption algorithms: currently we use
  // AES-128-GCM with 16-byte tags, but it is possible to switch to
  // e.g. AES-128-CBC + HMAC-SHA512 without affecting the protocol
  // (except for the cipher negotiation, of course).
  //
  // Additionally, each variant of the epilogue contains either
  // late_flags or late_status field that directs handling of frames
  // with more than one segment.
  uint32_t get_epilogue_onwire_len() const {
    ceph_assert(!m_descs.empty());
    if (m_is_rev1 && m_descs.size() == 1) {
      return 0;
    }
    if (m_crypto->rx) {
      return (m_is_rev1 ? sizeof(epilogue_secure_rev1_block_t) :
                  sizeof(epilogue_secure_rev0_block_t)) + get_auth_tag_len();
    }
    return m_is_rev1 ? sizeof(epilogue_crc_rev1_block_t) :
                       sizeof(epilogue_crc_rev0_block_t);
  }

  uint64_t get_frame_logical_len() const;
  uint64_t get_frame_onwire_len() const;

  bufferlist assemble_frame(Tag tag, bufferlist segment_bls[],
                            const uint16_t segment_aligns[],
                            size_t segment_count);

  Tag disassemble_preamble(bufferlist& preamble_bl);

  // Like msgr1, and unlike msgr2.0, msgr2.1 allows interpreting the
  // first segment before reading in the rest of the frame.
  //
  // For msgr2.1 (set_is_rev1(true)), you may:
  //
  // - read in the first segment
  // - call disassemble_first_segment()
  // - use the contents of the first segment, for example to
  //   look up user-provided buffers based on ceph_msg_header2::tid
  // - read in the remaining segments, possibly directly into
  //   user-provided buffers
  // - read in epilogue
  // - call disassemble_remaining_segments()
  //
  // For msgr2.0 (set_is_rev1(false)), disassemble_first_segment() is
  // a noop.  To accomodate, disassemble_remaining_segments() always
  // takes all segments and skips over the first segment in msgr2.1
  // case.  You must:
  //
  // - read in all segments
  // - read in epilogue
  // - call disassemble_remaining_segments()
  //
  // disassemble_remaining_segments() returns true if the frame is
  // ready for dispatching, or false if it was aborted by the sender
  // and must be dropped.
  void disassemble_first_segment(bufferlist& preamble_bl,
                                 bufferlist& segment_bl) const;
  bool disassemble_remaining_segments(bufferlist segment_bls[],
                                      bufferlist& epilogue_bl) const;

private:
  struct segment_desc_t {
    uint32_t logical_len;
    uint16_t align;
  };

  uint32_t get_segment_padded_len(size_t seg_idx) const {
    return p2roundup<uint32_t>(m_descs[seg_idx].logical_len,
                               CRYPTO_BLOCK_SIZE);
  }

  uint32_t get_auth_tag_len() const {
    return m_crypto->rx->get_extra_size_at_final();
  }

  bufferlist asm_crc_rev0(const preamble_block_t& preamble,
                          bufferlist segment_bls[]) const;
  bufferlist asm_secure_rev0(const preamble_block_t& preamble,
                             bufferlist segment_bls[]) const;
  bufferlist asm_crc_rev1(const preamble_block_t& preamble,
                          bufferlist segment_bls[]) const;
  bufferlist asm_secure_rev1(const preamble_block_t& preamble,
                             bufferlist segment_bls[]) const;

  bool disasm_all_crc_rev0(bufferlist segment_bls[],
                           bufferlist& epilogue_bl) const;
  bool disasm_all_secure_rev0(bufferlist segment_bls[],
                              bufferlist& epilogue_bl) const;
  void disasm_first_crc_rev1(bufferlist& preamble_bl,
                             bufferlist& segment_bl) const;
  bool disasm_remaining_crc_rev1(bufferlist segment_bls[],
                                 bufferlist& epilogue_bl) const;
  void disasm_first_secure_rev1(bufferlist& preamble_bl,
                                bufferlist& segment_bl) const;
  bool disasm_remaining_secure_rev1(bufferlist segment_bls[],
                                    bufferlist& epilogue_bl) const;

  void fill_preamble(Tag tag, preamble_block_t& preamble) const;
  friend std::ostream& operator<<(std::ostream& os,
                                  const FrameAssembler& frame_asm);

  boost::container::static_vector<segment_desc_t, MAX_NUM_SEGMENTS> m_descs;
  const ceph::crypto::onwire::rxtx_t* m_crypto;
  bool m_is_rev1;  // msgr2.1?
};

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

public:
  ceph::bufferlist get_buffer(FrameAssembler& tx_frame_asm) {
    auto bl = tx_frame_asm.assemble_frame(T::tag, segments.data(),
                                          alignments.data(), SegmentsNumV);
    ceph_assert(bl.length() == tx_frame_asm.get_frame_onwire_len());
    return bl;
  }
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
