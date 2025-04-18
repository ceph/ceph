#ifndef JWT_CPP_OPEN_SOURCE_PARSERS_JSONCPP_DEFAULTS_H
#define JWT_CPP_OPEN_SOURCE_PARSERS_JSONCPP_DEFAULTS_H

#ifndef JWT_DISABLE_PICOJSON
#define JWT_DISABLE_PICOJSON
#endif

#include "traits.h"

namespace jwt {
	/**
	 * \brief a class to store a generic [jsoncpp](https://github.com/open-source-parsers/jsoncpp) value as claim
	 *
	 * This type is the specialization of the \ref basic_claim class which
	 * uses the standard template types.
	 */
	using claim = basic_claim<traits::open_source_parsers_jsoncpp>;

	/**
	 * Create a verifier using the default clock
	 * \return verifier instance
	 */
	inline verifier<default_clock, traits::open_source_parsers_jsoncpp> verify() {
		return verify<default_clock, traits::open_source_parsers_jsoncpp>(default_clock{});
	}

	/**
	 * Create a builder using the default clock
	 * \return builder instance to create a new token
	 */
	inline builder<default_clock, traits::open_source_parsers_jsoncpp> create() {
		return builder<default_clock, traits::open_source_parsers_jsoncpp>(default_clock{});
	}

#ifndef JWT_DISABLE_BASE64
	/**
	 * Decode a token
	 * \param token Token to decode
	 * \return Decoded token
	 * \throw std::invalid_argument Token is not in correct format
	 * \throw std::runtime_error Base64 decoding failed or invalid json
	 */
	inline decoded_jwt<traits::open_source_parsers_jsoncpp> decode(const std::string& token) {
		return decoded_jwt<traits::open_source_parsers_jsoncpp>(token);
	}
#endif

	/**
	 * Decode a token
	 * \tparam Decode is callable, taking a string_type and returns a string_type.
	 * It should ensure the padding of the input and then base64url decode and
	 * return the results.
	 * \param token Token to decode
	 * \param decode The token to parse
	 * \return Decoded token
	 * \throw std::invalid_argument Token is not in correct format
	 * \throw std::runtime_error Base64 decoding failed or invalid json
	 */
	template<typename Decode>
	decoded_jwt<traits::open_source_parsers_jsoncpp> decode(const std::string& token, Decode decode) {
		return decoded_jwt<traits::open_source_parsers_jsoncpp>(token, decode);
	}

	/**
	 * Parse a jwk
	 * \param token JWK Token to parse
	 * \return Parsed JWK
	 * \throw std::runtime_error Token is not in correct format
	 */
	inline jwk<traits::open_source_parsers_jsoncpp>
	parse_jwk(const traits::open_source_parsers_jsoncpp::string_type& token) {
		return jwk<traits::open_source_parsers_jsoncpp>(token);
	}

	/**
	 * Parse a jwks
	 * \param token JWKs Token to parse
	 * \return Parsed JWKs
	 * \throw std::runtime_error Token is not in correct format
	 */
	inline jwks<traits::open_source_parsers_jsoncpp>
	parse_jwks(const traits::open_source_parsers_jsoncpp::string_type& token) {
		return jwks<traits::open_source_parsers_jsoncpp>(token);
	}

	/**
	 * This type is the specialization of the \ref verify_ops::verify_context class which
	 * uses the standard template types.
	 */
	using verify_context = verify_ops::verify_context<traits::open_source_parsers_jsoncpp>;
} // namespace jwt

#endif // JWT_CPP_OPEN_SOURCE_PARSERS_JSONCPP_DEFAULTS_H
