#ifndef JWT_CPP_BASE_H
#define JWT_CPP_BASE_H

#include <algorithm>
#include <array>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef __has_cpp_attribute
#if __has_cpp_attribute(fallthrough)
#define JWT_FALLTHROUGH [[fallthrough]]
#endif
#endif

#ifndef JWT_FALLTHROUGH
#define JWT_FALLTHROUGH
#endif

namespace jwt {
	/**
	 * \brief character maps when encoding and decoding
	 */
	namespace alphabet {
		/**
		 * \brief valid list of character when working with [Base64](https://datatracker.ietf.org/doc/html/rfc4648#section-4)
		 *
		 * As directed in [X.509 Parameter](https://datatracker.ietf.org/doc/html/rfc7517#section-4.7) certificate chains are
		 * base64-encoded as per [Section 4 of RFC4648](https://datatracker.ietf.org/doc/html/rfc4648#section-4)
		 */
		struct base64 {
			static const std::array<char, 64>& data() {
				static constexpr std::array<char, 64> data{
					{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
					 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
					 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
					 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'}};
				return data;
			}
			static const std::array<int8_t, 256>& rdata() {
				static constexpr std::array<int8_t, 256> rdata{{
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63,
					52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,	1,	2,	3,	4,	5,	6,
					7,	8,	9,	10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, -1,
					-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
					49, 50, 51, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				}};
				return rdata;
			}
			static const std::string& fill() {
				static const std::string fill{"="};
				return fill;
			}
		};
		/**
		 * \brief valid list of character when working with [Base64URL](https://tools.ietf.org/html/rfc4648#section-5)
		 *
		 * As directed by [RFC 7519 Terminology](https://datatracker.ietf.org/doc/html/rfc7519#section-2) set the definition of Base64URL
		 * encoding as that in [RFC 7515](https://datatracker.ietf.org/doc/html/rfc7515#section-2) that states:
		 *
		 * > Base64 encoding using the URL- and filename-safe character set defined in
		 * > [Section 5 of RFC 4648 RFC4648](https://tools.ietf.org/html/rfc4648#section-5), with all trailing '=' characters omitted
		 */
		struct base64url {
			static const std::array<char, 64>& data() {
				static constexpr std::array<char, 64> data{
					{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
					 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
					 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
					 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'}};
				return data;
			}
			static const std::array<int8_t, 256>& rdata() {
				static constexpr std::array<int8_t, 256> rdata{{
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1,
					52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,	1,	2,	3,	4,	5,	6,
					7,	8,	9,	10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, 63,
					-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
					49, 50, 51, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
				}};
				return rdata;
			}
			static const std::string& fill() {
				static const std::string fill{"%3d"};
				return fill;
			}
		};
		namespace helper {
			/**
			 * \brief A General purpose base64url alphabet respecting the
			 * [URI Case Normalization](https://datatracker.ietf.org/doc/html/rfc3986#section-6.2.2.1)
			 *
			 * This is useful in situations outside of JWT encoding/decoding and is provided as a helper
			 */
			struct base64url_percent_encoding {
				static const std::array<char, 64>& data() {
					static constexpr std::array<char, 64> data{
						{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
						 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
						 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
						 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'}};
					return data;
				}
				static const std::array<int8_t, 256>& rdata() {
					static constexpr std::array<int8_t, 256> rdata{{
						-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
						-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1,
						52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,	1,	2,	3,	4,	5,	6,
						7,	8,	9,	10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, -1, -1, -1, -1, 63,
						-1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
						49, 50, 51, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
						-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
						-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
						-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
						-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
						-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
					}};
					return rdata;
				}
				static const std::vector<std::string>& fill() {
					static const std::vector<std::string> fill{"%3D", "%3d"};
					return fill;
				}
			};
		} // namespace helper

		inline uint32_t index(const std::array<int8_t, 256>& rdata, char symbol) {
			auto index = rdata[static_cast<unsigned char>(symbol)];
			if (index <= -1) { throw std::runtime_error("Invalid input: not within alphabet"); }
			return static_cast<uint32_t>(index);
		}
	} // namespace alphabet

	/**
	 * \brief A collection of fellable functions for working with base64 and base64url
	 */
	namespace base {
		namespace details {
			struct padding {
				size_t count = 0;
				size_t length = 0;

				padding() = default;
				padding(size_t count, size_t length) : count(count), length(length) {}

				padding operator+(const padding& p) { return padding(count + p.count, length + p.length); }

				friend bool operator==(const padding& lhs, const padding& rhs) {
					return lhs.count == rhs.count && lhs.length == rhs.length;
				}
			};

			inline padding count_padding(const std::string& base, const std::vector<std::string>& fills) {
				for (const auto& fill : fills) {
					if (base.size() < fill.size()) continue;
					// Does the end of the input exactly match the fill pattern?
					if (base.substr(base.size() - fill.size()) == fill) {
						return padding{1, fill.length()} +
							   count_padding(base.substr(0, base.size() - fill.size()), fills);
					}
				}

				return {};
			}

			inline std::string encode(const std::string& bin, const std::array<char, 64>& alphabet,
									  const std::string& fill) {
				size_t size = bin.size();
				std::string res;

				// clear incomplete bytes
				size_t fast_size = size - size % 3;
				for (size_t i = 0; i < fast_size;) {
					uint32_t octet_a = static_cast<unsigned char>(bin[i++]);
					uint32_t octet_b = static_cast<unsigned char>(bin[i++]);
					uint32_t octet_c = static_cast<unsigned char>(bin[i++]);

					uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

					res += alphabet[(triple >> 3 * 6) & 0x3F];
					res += alphabet[(triple >> 2 * 6) & 0x3F];
					res += alphabet[(triple >> 1 * 6) & 0x3F];
					res += alphabet[(triple >> 0 * 6) & 0x3F];
				}

				if (fast_size == size) return res;

				size_t mod = size % 3;

				uint32_t octet_a = fast_size < size ? static_cast<unsigned char>(bin[fast_size++]) : 0;
				uint32_t octet_b = fast_size < size ? static_cast<unsigned char>(bin[fast_size++]) : 0;
				uint32_t octet_c = fast_size < size ? static_cast<unsigned char>(bin[fast_size++]) : 0;

				uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

				switch (mod) {
				case 1:
					res += alphabet[(triple >> 3 * 6) & 0x3F];
					res += alphabet[(triple >> 2 * 6) & 0x3F];
					res += fill;
					res += fill;
					break;
				case 2:
					res += alphabet[(triple >> 3 * 6) & 0x3F];
					res += alphabet[(triple >> 2 * 6) & 0x3F];
					res += alphabet[(triple >> 1 * 6) & 0x3F];
					res += fill;
					break;
				default: break;
				}

				return res;
			}

			inline std::string decode(const std::string& base, const std::array<int8_t, 256>& rdata,
									  const std::vector<std::string>& fill) {
				const auto pad = count_padding(base, fill);
				if (pad.count > 2) throw std::runtime_error("Invalid input: too much fill");

				const size_t size = base.size() - pad.length;
				if ((size + pad.count) % 4 != 0) throw std::runtime_error("Invalid input: incorrect total size");

				size_t out_size = size / 4 * 3;
				std::string res;
				res.reserve(out_size);

				auto get_sextet = [&](size_t offset) { return alphabet::index(rdata, base[offset]); };

				size_t fast_size = size - size % 4;
				for (size_t i = 0; i < fast_size;) {
					uint32_t sextet_a = get_sextet(i++);
					uint32_t sextet_b = get_sextet(i++);
					uint32_t sextet_c = get_sextet(i++);
					uint32_t sextet_d = get_sextet(i++);

					uint32_t triple =
						(sextet_a << 3 * 6) + (sextet_b << 2 * 6) + (sextet_c << 1 * 6) + (sextet_d << 0 * 6);

					res += static_cast<char>((triple >> 2 * 8) & 0xFFU);
					res += static_cast<char>((triple >> 1 * 8) & 0xFFU);
					res += static_cast<char>((triple >> 0 * 8) & 0xFFU);
				}

				if (pad.count == 0) return res;

				uint32_t triple = (get_sextet(fast_size) << 3 * 6) + (get_sextet(fast_size + 1) << 2 * 6);

				switch (pad.count) {
				case 1:
					triple |= (get_sextet(fast_size + 2) << 1 * 6);
					res += static_cast<char>((triple >> 2 * 8) & 0xFFU);
					res += static_cast<char>((triple >> 1 * 8) & 0xFFU);
					break;
				case 2: res += static_cast<char>((triple >> 2 * 8) & 0xFFU); break;
				default: break;
				}

				return res;
			}

			inline std::string decode(const std::string& base, const std::array<int8_t, 256>& rdata,
									  const std::string& fill) {
				return decode(base, rdata, std::vector<std::string>{fill});
			}

			inline std::string pad(const std::string& base, const std::string& fill) {
				std::string padding;
				switch (base.size() % 4) {
				case 1: padding += fill; JWT_FALLTHROUGH;
				case 2: padding += fill; JWT_FALLTHROUGH;
				case 3: padding += fill; JWT_FALLTHROUGH;
				default: break;
				}

				return base + padding;
			}

			inline std::string trim(const std::string& base, const std::string& fill) {
				auto pos = base.find(fill);
				return base.substr(0, pos);
			}
		} // namespace details

		/**
		 * \brief Generic base64 encoding
		 * 
		 * A Generic base64 encode function that supports any "alphabet"
		 * such as jwt::alphabet::base64 
		 * 
		 * \code
		 * const auto b64 = jwt::base::encode<jwt::alphabet::base64>("example_data")
		 * \endcode
		 */
		template<typename T>
		std::string encode(const std::string& bin) {
			return details::encode(bin, T::data(), T::fill());
		}
		/**
		 * \brief Generic base64 decoding
		 * 
		 * A Generic base64 decoding function that supports any "alphabet"
		 * such as jwt::alphabet::base64 
		 * 
		 * \code
		 * const auto b64 = jwt::base::decode<jwt::alphabet::base64>("ZXhhbXBsZV9kYXRh")
		 * \endcode
		 */
		template<typename T>
		std::string decode(const std::string& base) {
			return details::decode(base, T::rdata(), T::fill());
		}
		/**
		 * \brief Generic base64 padding
		 * 
		 * A Generic base64 pad function that supports any "alphabet"
		 * such as jwt::alphabet::base64 
		 * 
		 * \code
		 * const auto b64 = jwt::base::pad<jwt::alphabet::base64>("ZXhhbXBsZV9kYQ")
		 * \endcode
		 */
		template<typename T>
		std::string pad(const std::string& base) {
			return details::pad(base, T::fill());
		}
		/**
		 * \brief Generic base64 trimming
		 * 
		 * A Generic base64 trim function that supports any "alphabet"
		 * such as jwt::alphabet::base64 
		 * 
		 * \code
		 * const auto b64 = jwt::base::trim<jwt::alphabet::base64>("ZXhhbXBsZV9kYQ==")
		 * \endcode
		 */
		template<typename T>
		std::string trim(const std::string& base) {
			return details::trim(base, T::fill());
		}
	} // namespace base
} // namespace jwt

#endif
