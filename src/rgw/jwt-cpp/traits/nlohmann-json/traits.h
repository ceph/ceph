#ifndef JWT_CPP_NLOHMANN_JSON_TRAITS_H
#define JWT_CPP_NLOHMANN_JSON_TRAITS_H

#include "jwt-cpp/jwt.h"
#include "nlohmann/json.hpp"

namespace jwt {
	/**
	 * \brief Namespace containing all the json_trait implementations for a jwt::basic_claim.
	*/
	namespace traits {
		/// basic_claim's JSON trait implementation for Modern C++ JSON
		struct nlohmann_json {
			using json = nlohmann::json;
			using value_type = json;
			using object_type = json::object_t;
			using array_type = json::array_t;
			using string_type = std::string; // current limitation of traits implementation
			using number_type = json::number_float_t;
			using integer_type = json::number_integer_t;
			using boolean_type = json::boolean_t;

			static jwt::json::type get_type(const json& val) {
				using jwt::json::type;

				if (val.type() == json::value_t::boolean) return type::boolean;
				// nlohmann internally tracks two types of integers
				if (val.type() == json::value_t::number_integer) return type::integer;
				if (val.type() == json::value_t::number_unsigned) return type::integer;
				if (val.type() == json::value_t::number_float) return type::number;
				if (val.type() == json::value_t::string) return type::string;
				if (val.type() == json::value_t::array) return type::array;
				if (val.type() == json::value_t::object) return type::object;

				throw std::logic_error("invalid type");
			}

			static json::object_t as_object(const json& val) {
				if (val.type() != json::value_t::object) throw std::bad_cast();
				return val.get<json::object_t>();
			}

			static std::string as_string(const json& val) {
				if (val.type() != json::value_t::string) throw std::bad_cast();
				return val.get<std::string>();
			}

			static json::array_t as_array(const json& val) {
				if (val.type() != json::value_t::array) throw std::bad_cast();
				return val.get<json::array_t>();
			}

			static int64_t as_integer(const json& val) {
				switch (val.type()) {
				case json::value_t::number_integer:
				case json::value_t::number_unsigned: return val.get<int64_t>();
				default: throw std::bad_cast();
				}
			}

			static bool as_boolean(const json& val) {
				if (val.type() != json::value_t::boolean) throw std::bad_cast();
				return val.get<bool>();
			}

			static double as_number(const json& val) {
				if (val.type() != json::value_t::number_float) throw std::bad_cast();
				return val.get<double>();
			}

			static bool parse(json& val, std::string str) {
				val = json::parse(str.begin(), str.end());
				return true;
			}

			static std::string serialize(const json& val) { return val.dump(); }
		};
	} // namespace traits
} // namespace jwt

#endif
