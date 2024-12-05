#define JWT_DISABLE_PICOJSON
#define JSONCONS_NO_DEPRECATED

#include "jwt-cpp/jwt.h"

#include "jsoncons/json.hpp"

#include <sstream>

namespace jwt {
	/**
	 * \brief Namespace containing all the json_trait implementations for a jwt::basic_claim.
	*/
	namespace traits {
		/// basic_claim's JSON trait implementation for jsoncons.
		struct danielaparker_jsoncons {
			// Needs at least https://github.com/danielaparker/jsoncons/commit/28c56b90ec7337f98a5b8942574590111a5e5831
			static_assert(jsoncons::version().minor >= 167, "A higher version of jsoncons is required!");

			using json = jsoncons::json;
			using value_type = json;
			struct object_type : json::object {
				// Add missing C++11 member types
				// https://github.com/danielaparker/jsoncons/commit/1b1ceeb572f9a2db6d37cff47ac78a4f14e072e2#commitcomment-45391411
				using value_type = key_value_type; // Enable optional jwt-cpp methods
				using mapped_type = key_value_type::value_type;
				using size_type = size_t; // for implementing count

				object_type() = default;
				object_type(const object_type&) = default;
				explicit object_type(const json::object& o) : json::object(o) {}
				object_type(object_type&&) = default;
				explicit object_type(json::object&& o) : json::object(o) {}
				~object_type() = default;
				object_type& operator=(const object_type& o) = default;
				object_type& operator=(object_type&& o) noexcept = default;

				// Add missing C++11 subscription operator
				mapped_type& operator[](const key_type& key) {
					// https://github.com/microsoft/STL/blob/2914b4301c59dc7ffc09d16ac6f7979fde2b7f2c/stl/inc/map#L325
					return try_emplace(key).first->value();
				}

				// Add missing C++11 element access
				const mapped_type& at(const key_type& key) const {
					auto target = find(key);
					if (target != end()) return target->value();

					throw std::out_of_range("invalid key");
				}

				// Add missing C++11 lookup method
				size_type count(const key_type& key) const {
					struct compare {
						bool operator()(const value_type& val, const key_type& key) const { return val.key() < key; }
						bool operator()(const key_type& key, const value_type& val) const { return key < val.key(); }
					};

					// https://en.cppreference.com/w/cpp/algorithm/binary_search#Complexity
					if (std::binary_search(this->begin(), this->end(), key, compare{})) return 1;
					return 0;
				}
			};
			class array_type : public json::array {
			public:
				using json::array::array;
				explicit array_type(const json::array& a) : json::array(a) {}
				explicit array_type(json::array&& a) : json::array(a) {}
				value_type const& front() const { return this->operator[](0U); }
			};
			using string_type = std::string; // current limitation of traits implementation
			using number_type = double;
			using integer_type = int64_t;
			using boolean_type = bool;

			static jwt::json::type get_type(const json& val) {
				using jwt::json::type;

				if (val.type() == jsoncons::json_type::bool_value) return type::boolean;
				if (val.type() == jsoncons::json_type::int64_value) return type::integer;
				if (val.type() == jsoncons::json_type::uint64_value) return type::integer;
				if (val.type() == jsoncons::json_type::half_value) return type::number;
				if (val.type() == jsoncons::json_type::double_value) return type::number;
				if (val.type() == jsoncons::json_type::string_value) return type::string;
				if (val.type() == jsoncons::json_type::array_value) return type::array;
				if (val.type() == jsoncons::json_type::object_value) return type::object;

				throw std::logic_error("invalid type");
			}

			static object_type as_object(const json& val) {
				if (val.type() != jsoncons::json_type::object_value) throw std::bad_cast();
				return object_type(val.object_value());
			}

			static array_type as_array(const json& val) {
				if (val.type() != jsoncons::json_type::array_value) throw std::bad_cast();
				return array_type(val.array_value());
			}

			static string_type as_string(const json& val) {
				if (val.type() != jsoncons::json_type::string_value) throw std::bad_cast();
				return val.as_string();
			}

			static number_type as_number(const json& val) {
				if (get_type(val) != jwt::json::type::number) throw std::bad_cast();
				return val.as_double();
			}

			static integer_type as_integer(const json& val) {
				if (get_type(val) != jwt::json::type::integer) throw std::bad_cast();
				return val.as<integer_type>();
			}

			static boolean_type as_boolean(const json& val) {
				if (val.type() != jsoncons::json_type::bool_value) throw std::bad_cast();
				return val.as_bool();
			}

			static bool parse(json& val, const std::string& str) {
				val = json::parse(str);
				return true;
			}

			static std::string serialize(const json& val) {
				std::ostringstream os;
				os << jsoncons::print(val);
				return os.str();
			}
		};
	} // namespace traits
} // namespace jwt
