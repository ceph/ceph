#ifndef JWT_CPP_PICOJSON_TRAITS_H
#define JWT_CPP_PICOJSON_TRAITS_H

#ifndef PICOJSON_USE_INT64
#define PICOJSON_USE_INT64
#endif
#include "picojson/picojson.h"

#ifndef JWT_DISABLE_PICOJSON
#define JWT_DISABLE_PICOJSON
#endif
#include "jwt-cpp/jwt.h"

namespace jwt {
	/**
	 * \brief Namespace containing all the json_trait implementations for a jwt::basic_claim.
	*/
	namespace traits {
		/// basic_claim's JSON trait implementation for picojson
		struct kazuho_picojson {
			using value_type = picojson::value;
			using object_type = picojson::object;
			using array_type = picojson::array;
			using string_type = std::string;
			using number_type = double;
			using integer_type = int64_t;
			using boolean_type = bool;

			static json::type get_type(const picojson::value& val) {
				using json::type;
				if (val.is<bool>()) return type::boolean;
				if (val.is<int64_t>()) return type::integer;
				if (val.is<double>()) return type::number;
				if (val.is<std::string>()) return type::string;
				if (val.is<picojson::array>()) return type::array;
				if (val.is<picojson::object>()) return type::object;

				throw std::logic_error("invalid type");
			}

			static picojson::object as_object(const picojson::value& val) {
				if (!val.is<picojson::object>()) throw std::bad_cast();
				return val.get<picojson::object>();
			}

			static std::string as_string(const picojson::value& val) {
				if (!val.is<std::string>()) throw std::bad_cast();
				return val.get<std::string>();
			}

			static picojson::array as_array(const picojson::value& val) {
				if (!val.is<picojson::array>()) throw std::bad_cast();
				return val.get<picojson::array>();
			}

			static int64_t as_integer(const picojson::value& val) {
				if (!val.is<int64_t>()) throw std::bad_cast();
				return val.get<int64_t>();
			}

			static bool as_boolean(const picojson::value& val) {
				if (!val.is<bool>()) throw std::bad_cast();
				return val.get<bool>();
			}

			static double as_number(const picojson::value& val) {
				if (!val.is<double>()) throw std::bad_cast();
				return val.get<double>();
			}

			static bool parse(picojson::value& val, const std::string& str) {
				return picojson::parse(val, str).empty();
			}

			static std::string serialize(const picojson::value& val) { return val.serialize(); }
		};
	} // namespace traits
} // namespace jwt

#endif
