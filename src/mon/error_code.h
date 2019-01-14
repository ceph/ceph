#ifndef CEPH_ERROR_CODE_H
 #define CEPH_ERROR_CODE_H

#include <exception>
#include <system_error>

// error_code support for command multiplexing:
namespace ceph {

enum struct command_error {
	option_invalid = 1,
	option_not_found,
};

} // namespace ceph

// Hooks for our error_codes:
namespace std {

template <>
struct is_error_code_enum<ceph::command_error> : true_type
{};

} // namespace std

// Error categories:
namespace ceph {

struct command_error_category : std::error_category
{
	const char *name() const noexcept override { return "command_error"; }

    std::string message(int error_value) const override
	{
		 switch(static_cast<ceph::command_error>(error_value))
		 {
			case command_error::option_invalid:		return "invalid option";
			case command_error::option_not_found:	return "option not found";
 		 }

// JFW: what is the most ceph-friendly action to take in this case? Throw? Assert?
		return "unrecognized error value";
    }
};

// JFW: there's system_error which I believe is for POSIX stuff. Should we be using
// that, or is this the right idea?
static int to_ceph_error_code(const ceph::command_error error_value) 
{
 switch(static_cast<ceph::command_error>(error_value))
 {
	case command_error::option_invalid:		return -EINVAL;
	case command_error::option_not_found:	return -ENOENT;
 }

// JFW: is an assert() more "ceph-appropriate"? Or throwing? Or returning something?
 return -1; 
}

const command_error_category g_command_error_category; // global handle for error_code

// Conversion support (enables ADL):
static std::error_code make_error_code(ceph::command_error& oe)
{
 return { static_cast<int>(oe), g_command_error_category };
}

} // namespace ceph

// Exception types:
namespace ceph {

class command_failure final : public std::runtime_error 
{
    const ceph::command_error ec;

    public:
    command_failure(const ceph::command_error ec, const std::string& msg)
     : std::runtime_error(msg),
       ec(ec)
    {};

    public:
    std::error_code error_code() const noexcept { return static_cast<std::error_code>(ec); }
    ceph::command_error command_error() const noexcept { return ec; }
};

} // namespace ceph

#endif
