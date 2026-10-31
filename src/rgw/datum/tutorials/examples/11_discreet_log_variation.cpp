// Choose which record fields to show, redact, or omit in diagnostic output.

#include <print>
#include <tuple>
#include <string>

#include <datum/render/display.h>

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

    // Approve exactly these diagnostic fields:
    static constexpr auto fields = std::tuple {field<&login_record::name>()};
};

} // ceph::salad::datum::render

int main()
{
    const login_record login {"Juniper", "sesame", "local only"};

    // Format the record using its approved diagnostic fields:
    std::println("{}", login);
}
