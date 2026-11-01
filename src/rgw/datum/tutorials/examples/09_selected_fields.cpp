// Choose record fields and their reading order with a field profile.

#include <print>
#include <string>

#include <datum/record.h>

namespace datum = ::ceph::salad::datum;

struct basket_record final
{
    std::string label;
    int toys;
    int capacity;
};

// Select fields in the order the consumer will read them:
using basket_fields = datum::member_fields<&basket_record::toys, &basket_record::label>;

template <typename FieldsT>
void print_fields(const auto& record)
{

    // Read each selected field through the supplied view:
    FieldsT::observe(record, [](const auto& value) { std::println("{}", value); });
}

int main()
{
    const basket_record basket {"Miso's basket", 7, 12};
    const datum::view_of inventory {basket};

    print_fields<basket_fields>(inventory);
}
