#ifndef JWT_CPP_JSONCPP_TRAITS_H
#define JWT_CPP_JSONCPP_TRAITS_H

#include "jwt-cpp/jwt.h"
#include "json/json.h"

namespace jwt {
	/**
	 * \brief Namespace containing all the json_trait implementations for a jwt::basic_claim.
	*/
	namespace traits {
		/// basic_claim's JSON trait implementation for jsoncpp
		struct open_source_parsers_jsoncpp {
			using value_type = Json::Value;
			using string_type = std::string;
			class array_type : public Json::Value {
			public:
				using value_type = Json::Value;

				array_type() = default;
				array_type(const array_type&) = default;
				explicit array_type(const Json::Value& o) : Json::Value(o) {}
				array_type(array_type&&) = default;
				explicit array_type(Json::Value&& o) : Json::Value(o) {}
				template<typename Iterator>
				array_type(Iterator begin, Iterator end) {
					for (Iterator it = begin; it != end; ++it) {
						Json::Value value;
						value = *it;
						this->append(value);
					}
				}
				~array_type() = default;
				array_type& operator=(const array_type& o) = default;
				array_type& operator=(array_type&& o) noexcept = default;

				value_type const& front() const { return this->operator[](0U); }
			};
			using number_type = double;
			using integer_type = Json::Value::Int;
			using boolean_type = bool;
			class object_type : public Json::Value {
			public:
				using key_type = std::string;
				using mapped_type = Json::Value;
				using size_type = size_t;

				object_type() = default;
				object_type(const object_type&) = default;
				explicit object_type(const Json::Value& o) : Json::Value(o) {}
				object_type(object_type&&) = default;
				explicit object_type(Json::Value&& o) : Json::Value(o) {}
				~object_type() = default;
				object_type& operator=(const object_type& o) = default;
				object_type& operator=(object_type&& o) noexcept = default;

				// Add missing C++11 element access
				const mapped_type& at(const key_type& key) const {
					Json::Value const* found = find(key.data(), key.data() + key.length());
					if (!found) throw std::out_of_range("invalid key");
					return *found;
				}

				size_type count(const key_type& key) const { return this->isMember(key) ? 1 : 0; }
			};

			// Translation between the implementation notion of type, to the jwt::json::type equivilant
			static jwt::json::type get_type(const value_type& val) {
				using jwt::json::type;

				if (val.isArray())
					return type::array;
				else if (val.isString())
					return type::string;
				// Order is important https://github.com/Thalhammer/jwt-cpp/pull/320#issuecomment-1865322511
				else if (val.isInt())
					return type::integer;
				else if (val.isNumeric())
					return type::number;
				else if (val.isBool())
					return type::boolean;
				else if (val.isObject())
					return type::object;

				throw std::logic_error("invalid type");
			}

			static integer_type as_integer(const value_type& val) {
				switch (val.type()) {
				case Json::intValue: return val.asInt64();
				case Json::uintValue: return static_cast<integer_type>(val.asUInt64());
				default: throw std::bad_cast();
				}
			}

			static boolean_type as_boolean(const value_type& val) {
				if (!val.isBool()) throw std::bad_cast();
				return val.asBool();
			}

			static number_type as_number(const value_type& val) {
				if (!val.isNumeric()) throw std::bad_cast();
				return val.asDouble();
			}

			static string_type as_string(const value_type& val) {
				if (!val.isString()) throw std::bad_cast();
				return val.asString();
			}

			static object_type as_object(const value_type& val) {
				if (!val.isObject()) throw std::bad_cast();
				return object_type(val);
			}

			static array_type as_array(const value_type& val) {
				if (!val.isArray()) throw std::bad_cast();
				return array_type(val);
			}

			static bool parse(value_type& val, string_type str) {
				Json::CharReaderBuilder builder;
				const std::unique_ptr<Json::CharReader> reader(builder.newCharReader());

				return reader->parse(reinterpret_cast<const char*>(str.c_str()),
									 reinterpret_cast<const char*>(str.c_str() + str.size()), &val, nullptr);
			}

			static string_type serialize(const value_type& val) {
				Json::StreamWriterBuilder builder;
				builder["commentStyle"] = "None";
				builder["indentation"] = "";
				std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
				return Json::writeString(builder, val);
			}
		};
	} // namespace traits
} // namespace jwt

#endif
