#ifndef _MSG_ASYNC_FRAMES_V2_
#define _MSG_ASYNC_FRAMES_V2_

#include "include/types.h"
#include "common/Clock.h"
#include "crypto_onwire.h"
#include <array>

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
  static constexpr __le16 PAGE_SIZE_ALIGNMENT{4096};

  static constexpr __le16 DEFAULT_ALIGNMENT = sizeof(void *);

  __le32 length;
  __le16 alignment;
} __attribute__((packed));

struct SegmentIndex {
  struct Msg {
    static constexpr std::size_t HEADER = 0;
    static constexpr std::size_t FRONT = 1;
    static constexpr std::size_t MIDDLE = 2;
    static constexpr std::size_t DATA = 3;
  };

  struct Frame {
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

  std::array<segment_t, MAX_NUM_SEGMENTS> segments;
  __u8 _reserved[2];

  // CRC32 for this single preamble block.
  __le32 crc;
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
  std::array<__le32, MAX_NUM_SEGMENTS> crc_values;
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

template <class T>
struct Frame {
protected:
  ceph::bufferlist payload;
  ceph::bufferlist::contiguous_filler preamble_filler;

  void fill_preamble(const std::initializer_list<segment_t> main_segments) {
    ceph_assert(std::size(main_segments) <= MAX_NUM_SEGMENTS);

    // Craft the main preamble. It's always present regardless of the number
    // of segments message is composed from. This doesn't apply to extra one
    // as it's optional -- if there is up to 2 segments, we'll never transmit
    // preamble_extra_t;
    {
      preamble_block_t main_preamble;
      // TODO: we might fill/pad with pseudo-random data.
      ::memset(&main_preamble, 0, sizeof(main_preamble));

      main_preamble.num_segments = std::size(main_segments);
      main_preamble.tag = static_cast<__u8>(T::tag);
      ceph_assert(main_preamble.tag != 0);

      std::copy(std::cbegin(main_segments), std::cend(main_segments),
                std::begin(main_preamble.segments));

      main_preamble.crc =
          ceph_crc32c(0, reinterpret_cast<unsigned char *>(&main_preamble),
                      sizeof(main_preamble) - sizeof(main_preamble.crc));

      preamble_filler.copy_in(sizeof(main_preamble),
                              reinterpret_cast<const char *>(&main_preamble));
    }
  }

  Frame() : preamble_filler(payload.append_hole(FRAME_PREAMBLE_SIZE)) {}

  void decode_frame(const ceph::bufferlist &bl) {
    auto ti = bl.cbegin();
    static_cast<T *>(this)->decode_payload(ti);
  }

  void decode_payload(bufferlist::const_iterator &ti) {}

public:
  ceph::bufferlist &get_buffer() {
    fill_preamble({segment_t{payload.length() - FRAME_PREAMBLE_SIZE,
                   segment_t::DEFAULT_ALIGNMENT}});

    epilogue_plain_block_t epilogue;
    ::memset(&epilogue, 0, sizeof(epilogue));

    ceph::bufferlist::const_iterator hdriter(&this->payload, FRAME_PREAMBLE_SIZE);
    epilogue.crc_values[SegmentIndex::Frame::PAYLOAD] =
        hdriter.crc32c(hdriter.get_remaining(), -1);
    this->payload.append(reinterpret_cast<const char*>(&epilogue), sizeof(epilogue));

    return payload;
  }
};

template <class C, typename... Args>
class PayloadFrame : public Frame<C> {
protected:
  // this tuple is only used when decoding values from a payload buffer
  std::tuple<Args...> _values;

  // FIXME: for now, we assume specific features for the purpoess of encoding
  // the frames themselves (*not* messages in message frames!).
  uint64_t features = msgr2_frame_assumed;

  template <typename T>
  inline void _encode_payload_each(T &t) {
    if constexpr (std::is_same<T, bufferlist const>()) {
      this->payload.claim_append((bufferlist &)t);
    } else if constexpr (std::is_same<T, std::vector<uint32_t> const>()) {
      encode((uint32_t)t.size(), this->payload, features);
      for (const auto &elem : t) {
        encode(elem, this->payload, features);
      }
    } else if constexpr (std::is_same<T, ceph_msg_header2 const>()) {
      this->payload.append((char *)&t, sizeof(t));
    } else {
      encode(t, this->payload, features);
    }
  }

  template <typename T>
  inline void _decode_payload_each(T &t, bufferlist::const_iterator &ti) const {
    if constexpr (std::is_same<T, bufferlist>()) {
      if (ti.get_remaining()) {
        t.append(ti.get_current_ptr());
      }
    } else if constexpr (std::is_same<T, std::vector<uint32_t>>()) {
      uint32_t size;
      decode(size, ti);
      t.resize(size);
      for (uint32_t i = 0; i < size; ++i) {
        decode(t[i], ti);
      }
    } else if constexpr (std::is_same<T, ceph_msg_header2>()) {
      auto ptr = ti.get_current_ptr();
      ti.advance(sizeof(T));
      t = *(T *)ptr.raw_c_str();
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

  PayloadFrame() : Frame<C>() {}

  void _encode(const Args &... args) {
    (_encode_payload_each(args), ...);
  }

public:
  static C Encode(const Args &... args) {
    C c;
    c._encode(args...);
    return c;
  }

  static C Decode(const ceph::bufferlist &payload) {
    C c;
    c.decode_frame(payload);
    return c;
  }

  void decode_payload(bufferlist::const_iterator &ti) {
    _decode_payload(ti, std::index_sequence_for<Args...>());
  }
};

struct HelloFrame : public PayloadFrame<HelloFrame,
                                        uint8_t,          // entity type
                                        entity_addr_t> {  // peer address
  static const Tag tag = Tag::HELLO;
  using PayloadFrame::Encode;
  using PayloadFrame::Decode;

  inline uint8_t &entity_type() { return get_val<0>(); }
  inline entity_addr_t &peer_addr() { return get_val<1>(); }

protected:
  using PayloadFrame::PayloadFrame;
};

struct AuthRequestFrame : public PayloadFrame<AuthRequestFrame,
			                                        uint32_t, // auth method
                                              vector<uint32_t>, // preferred modes
                                              bufferlist> { // auth payload
  static const Tag tag = Tag::AUTH_REQUEST;
  using PayloadFrame::Encode;
  using PayloadFrame::Decode;

  inline uint32_t &method() { return get_val<0>(); }
  inline vector<uint32_t> &preferred_modes() { return get_val<1>(); }
  inline bufferlist &auth_payload() { return get_val<2>(); }

protected:
  using PayloadFrame::PayloadFrame;
};

struct AuthBadMethodFrame : public PayloadFrame<AuthBadMethodFrame,
                                                uint32_t, // method
                                                int32_t,  // result
                                                std::vector<uint32_t>,   // allowed methods
                                                std::vector<uint32_t>> { // allowed modes
  static const Tag tag = Tag::AUTH_BAD_METHOD;
  using PayloadFrame::Encode;
  using PayloadFrame::Decode;

  inline uint32_t &method() { return get_val<0>(); }
  inline int32_t &result() { return get_val<1>(); }
  inline std::vector<uint32_t> &allowed_methods() { return get_val<2>(); }
  inline std::vector<uint32_t> &allowed_modes() { return get_val<3>(); }

protected:
  using PayloadFrame::PayloadFrame;
};

struct AuthReplyMoreFrame : public PayloadFrame<AuthReplyMoreFrame,
                                                bufferlist> { // auth payload
  static const Tag tag = Tag::AUTH_REPLY_MORE;
  using PayloadFrame::Encode;
  using PayloadFrame::Decode;

  inline bufferlist &auth_payload() { return get_val<0>(); }

protected:
  using PayloadFrame::PayloadFrame;
};

struct AuthRequestMoreFrame : public PayloadFrame<AuthRequestMoreFrame,
                                                  bufferlist> { // auth payload
  static const Tag tag = Tag::AUTH_REQUEST_MORE;
  using PayloadFrame::Encode;
  using PayloadFrame::Decode;

  inline bufferlist &auth_payload() { return get_val<0>(); }

protected:
  using PayloadFrame::PayloadFrame;
};

struct AuthDoneFrame : public PayloadFrame<AuthDoneFrame,
                                           uint64_t, // global id
                                           uint32_t, // connection mode
                                           bufferlist> { // auth method payload
  static const Tag tag = Tag::AUTH_DONE;
  using PayloadFrame::Encode;
  using PayloadFrame::Decode;

  inline uint64_t &global_id() { return get_val<0>(); }
  inline uint32_t &con_mode() { return get_val<1>(); }
  inline bufferlist &auth_payload() { return get_val<2>(); }

protected:
  using PayloadFrame::PayloadFrame;
};

template <class T, typename... Args>
struct SignedEncryptedFrame : public PayloadFrame<T, Args...> {
  ceph::bufferlist &get_buffer() {
    // In contrast to Frame::get_buffer() we don't fill preamble here.
    return this->payload;
  }

  static T Encode(ceph::crypto::onwire::rxtx_t &session_stream_handlers,
                  const Args &... args) {
    T c = PayloadFrame<T, Args...>::Encode(args...);
    // FIXME: plainsize -> ciphersize; for AES-GCM they are equall apart
    // from auth tag size
    c.fill_preamble({segment_t{c.payload.length() - FRAME_PREAMBLE_SIZE,
                     segment_t::DEFAULT_ALIGNMENT}});

    if (session_stream_handlers.tx) {
      epilogue_secure_block_t epilogue;
      ::memset(&epilogue, 0, sizeof(epilogue));
      c.payload.append(reinterpret_cast<const char*>(&epilogue), sizeof(epilogue));

      ceph_assert(session_stream_handlers.tx);
      session_stream_handlers.tx->reset_tx_handler({c.payload.length()});

      session_stream_handlers.tx->authenticated_encrypt_update(
          std::move(c.payload));
      c.payload = session_stream_handlers.tx->authenticated_encrypt_final();
    } else {
      epilogue_plain_block_t epilogue;
      ::memset(&epilogue, 0, sizeof(epilogue));

      ceph::bufferlist::const_iterator hdriter(&c.payload, FRAME_PREAMBLE_SIZE);
      epilogue.crc_values[SegmentIndex::Frame::PAYLOAD] =
          hdriter.crc32c(hdriter.get_remaining(), -1);
      c.payload.append(reinterpret_cast<const char*>(&epilogue), sizeof(epilogue));
    }
    return c;
  }

  static T Decode(ceph::crypto::onwire::rxtx_t &session_stream_handlers,
                  ceph::bufferlist &payload) {
    return PayloadFrame<T, Args...>::Decode(payload);
  }

protected:
  SignedEncryptedFrame() : PayloadFrame<T, Args...>() {}
};

struct ClientIdentFrame
    : public SignedEncryptedFrame<ClientIdentFrame, 
                                  entity_addrvec_t,  // my addresses
                                  entity_addr_t,  // target address
                                  int64_t,  // global_id
                                  uint64_t,  // global seq
                                  uint64_t,  // supported features
                                  uint64_t,  // required features
                                  uint64_t,  // flags
                                  uint64_t> {  // client cookie
  static const Tag tag = Tag::CLIENT_IDENT;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline entity_addr_t &target_addr() { return get_val<1>(); }
  inline int64_t &gid() { return get_val<2>(); }
  inline uint64_t &global_seq() { return get_val<3>(); }
  inline uint64_t &supported_features() { return get_val<4>(); }
  inline uint64_t &required_features() { return get_val<5>(); }
  inline uint64_t &flags() { return get_val<6>(); }
  inline uint64_t &cookie() { return get_val<7>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct ServerIdentFrame
    : public SignedEncryptedFrame<ServerIdentFrame,
                                  entity_addrvec_t,  // my addresses
                                  int64_t,  // global_id
                                  uint64_t,  // global seq
                                  uint64_t,  // supported features
                                  uint64_t,  // required features
                                  uint64_t,  // flags
                                  uint64_t> {  // server cookie
  static const Tag tag = Tag::SERVER_IDENT;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline int64_t &gid() { return get_val<1>(); }
  inline uint64_t &global_seq() { return get_val<2>(); }
  inline uint64_t &supported_features() { return get_val<3>(); }
  inline uint64_t &required_features() { return get_val<4>(); }
  inline uint64_t &flags() { return get_val<5>(); }
  inline uint64_t &cookie() { return get_val<6>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct ReconnectFrame
    : public SignedEncryptedFrame<ReconnectFrame, 
                                  entity_addrvec_t,  // my addresses
                                  uint64_t,  // client cookie
                                  uint64_t,  // server cookie
                                  uint64_t,  // global sequence
                                  uint64_t,  // connect sequence
                                  uint64_t> { // message sequence
  static const Tag tag = Tag::SESSION_RECONNECT;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline entity_addrvec_t &addrs() { return get_val<0>(); }
  inline uint64_t &client_cookie() { return get_val<1>(); }
  inline uint64_t &server_cookie() { return get_val<2>(); }
  inline uint64_t &global_seq() { return get_val<3>(); }
  inline uint64_t &connect_seq() { return get_val<4>(); }
  inline uint64_t &msg_seq() { return get_val<5>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct ResetFrame : public SignedEncryptedFrame<ResetFrame,
                                                bool> {  // full reset
  static const Tag tag = Tag::SESSION_RESET;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline bool &full() { return get_val<0>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct RetryFrame : public SignedEncryptedFrame<RetryFrame,
                                                uint64_t> {  // connection seq
  static const Tag tag = Tag::SESSION_RETRY;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline uint64_t &connect_seq() { return get_val<0>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct RetryGlobalFrame : public SignedEncryptedFrame<RetryGlobalFrame,
                                                      uint64_t> { // global seq
  static const Tag tag = Tag::SESSION_RETRY_GLOBAL;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline uint64_t &global_seq() { return get_val<0>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct WaitFrame : public SignedEncryptedFrame<WaitFrame> {
  static const Tag tag = Tag::WAIT;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct ReconnectOkFrame : public SignedEncryptedFrame<ReconnectOkFrame,
                                                      uint64_t> { // message seq
  static const Tag tag = Tag::SESSION_RECONNECT_OK;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline uint64_t &msg_seq() { return get_val<0>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct IdentMissingFeaturesFrame 
    : public SignedEncryptedFrame<IdentMissingFeaturesFrame,
                                  uint64_t> { // missing features mask
  static const Tag tag = Tag::IDENT_MISSING_FEATURES;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline uint64_t &features() { return get_val<0>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct KeepAliveFrame : public SignedEncryptedFrame<KeepAliveFrame,
                                                    utime_t> {  // timestamp
  static const Tag tag = Tag::KEEPALIVE2;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  static KeepAliveFrame Encode(
      ceph::crypto::onwire::rxtx_t &session_stream_handlers) {
    return KeepAliveFrame::Encode(session_stream_handlers, ceph_clock_now());
  }

  inline utime_t &timestamp() { return get_val<0>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct KeepAliveFrameAck : public SignedEncryptedFrame<KeepAliveFrameAck,
                                                       utime_t> { // ack timestamp
  static const Tag tag = Tag::KEEPALIVE2_ACK;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline utime_t &timestamp() { return get_val<0>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

struct AckFrame : public SignedEncryptedFrame<AckFrame,
                                              uint64_t> { // message sequence
  static const Tag tag = Tag::ACK;
  using SignedEncryptedFrame::Encode;
  using SignedEncryptedFrame::Decode;

  inline uint64_t &seq() { return get_val<0>(); }

protected:
  using SignedEncryptedFrame::SignedEncryptedFrame;
};

// This class is used for encoding/decoding header of the message frame.
// Body is processed almost independently with the sole junction point
// being the `extra_payload_len` passed to get_buffer().
struct MessageHeaderFrame
    : public PayloadFrame<MessageHeaderFrame, ceph_msg_header2> {
  static const Tag tag = Tag::MESSAGE;

  ceph::bufferlist &get_buffer() {
    // In contrast to Frame::get_buffer() we don't fill preamble here.
    return this->payload;
  }

  static MessageHeaderFrame Encode(ceph::crypto::onwire::rxtx_t &session_stream_handlers,
                                   const ceph_msg_header2 &msg_header,
                                   const ceph::bufferlist& front,
                                   const ceph::bufferlist& middle,
                                   const ceph::bufferlist& data) {
    MessageHeaderFrame f =
        PayloadFrame<MessageHeaderFrame, ceph_msg_header2>::Encode(msg_header);
    // FIXME: plainsize -> ciphersize; for AES-GCM they are equall apart from auth tag size
    f.fill_preamble({
      segment_t{ f.payload.length() - FRAME_PREAMBLE_SIZE,
		 segment_t::DEFAULT_ALIGNMENT },
      segment_t{ front.length(), segment_t::DEFAULT_ALIGNMENT },
      segment_t{ middle.length(), segment_t::DEFAULT_ALIGNMENT },
      segment_t{ data.length(), segment_t::PAGE_SIZE_ALIGNMENT },
    });

    // FIXME: plainsize -> ciphersize; for AES-GCM they are equall apart from auth tag size
    if (session_stream_handlers.tx) {
      // let's cipher allocate one huge buffer for entire ciphertext.
      // NOTE: ultimately we'll align these sizes to cipher's block size.
      // AES-GCM can live without that as it's basically stream cipher.
      session_stream_handlers.tx->reset_tx_handler({
        f.payload.length(),
        front.length(),
        middle.length(),
        data.length()
      });

      ceph_assert(f.payload.length());
      session_stream_handlers.tx->authenticated_encrypt_update(
        std::move(f.payload));

      // TODO: switch TxHandler from `bl&&` to `const bl&`.
      if (front.length()) {
        session_stream_handlers.tx->authenticated_encrypt_update(front);
      }
      if (middle.length()) {
        session_stream_handlers.tx->authenticated_encrypt_update(middle);
      }
      if (data.length()) {
        session_stream_handlers.tx->authenticated_encrypt_update(data);
      }

      // craft the secure's mode epilogue
      {
        epilogue_secure_block_t epilogue;
        ::memset(&epilogue, 0, sizeof(epilogue));
        ceph::bufferlist epilogue_bl;
        epilogue_bl.append(reinterpret_cast<const char*>(&epilogue),
                                   sizeof(epilogue));
        session_stream_handlers.tx->authenticated_encrypt_update(epilogue_bl);
      }

      // auth tag will be appended at the end
      f.payload = session_stream_handlers.tx->authenticated_encrypt_final();
    } else {
      epilogue_plain_block_t epilogue;
      ::memset(&epilogue, 0, sizeof(epilogue));

      ceph::bufferlist::const_iterator hdriter(&f.payload, FRAME_PREAMBLE_SIZE);
      epilogue.crc_values[SegmentIndex::Msg::HEADER] =
          hdriter.crc32c(hdriter.get_remaining(), -1);
      epilogue.crc_values[SegmentIndex::Msg::FRONT] = front.crc32c(-1);
      epilogue.crc_values[SegmentIndex::Msg::MIDDLE] = middle.crc32c(-1);
      epilogue.crc_values[SegmentIndex::Msg::DATA] = data.crc32c(-1);

      f.payload.append(front);
      f.payload.append(middle);
      f.payload.append(data);
      f.payload.append(reinterpret_cast<const char*>(&epilogue), sizeof(epilogue));
    }

    return f;
  }

  static MessageHeaderFrame Decode(ceph::bufferlist&& text) {
    MessageHeaderFrame f;
    f.decode_frame(text);
    return f;
  }

  inline ceph_msg_header2 &header() { return get_val<0>(); }

protected:
  using PayloadFrame::PayloadFrame;
};

} // namespace ceph::msgr::v2

#endif // _MSG_ASYNC_FRAMES_V2_
