// Choose which record fields to show, redact, or omit in diagnostic output.

#include <print>
#include <tuple>
#include <string>
#include <string_view>

#include <datum/render/std_format.h>

struct login_record final
{
    std::string name;
    std::string password;
    std::string internal_note;
};

namespace ceph::salad::datum::render {

template <>
struct display_schema<login_record> final
{
    static constexpr std::string_view name = "login_record";

    // Approve exactly these diagnostic fields:
    static constexpr auto fields = std::tuple {
        field<&login_record::name>("name"),
        display_field<&login_record::password, display_field_policy::redacted> {"password"}
    };
};

} // ceph::salad::datum::render

int main()
{
    const login_record login {"Juniper", "sesame", "local only"};

    // Format the record using its approved diagnostic fields:
    std::println("{}", login);
}
