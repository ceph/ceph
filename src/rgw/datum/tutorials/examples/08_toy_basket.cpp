// Read a record's current field values through a borrowed view.

#include <print>
#include <string>

#include <datum/record.h>

namespace datum = ceph::salad::datum;

#define BASKET_FIELDS(Field) \
    Field(std::string, label) \
    Field(int, toys)
DATUM_RECORD(basket_record, BASKET_FIELDS);
#undef BASKET_FIELDS

int main()
{

    // Build a borrowed view of the chosen source record:
    basket_record basket {"Miso's basket", 7};
    const datum::view_of inventory {basket};

    std::println("before: {} holds {} toys", inventory.label, inventory.toys);

    // Change the original basket, then observe the view again:
    ++basket.toys;
    std::println("after: {} holds {} toys", inventory.label, inventory.toys);
}
