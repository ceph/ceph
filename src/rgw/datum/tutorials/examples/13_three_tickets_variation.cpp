// Charge one work unit before printing each ticket, and refuse tickets beyond the allowance.

#include <array>
#include <print>

#include <datum/budget.h>

namespace datum = ::ceph::salad::datum;

int main()
{

    // One work unit pays for one ticket:
    datum::resource_budget tickets {{.work = 4}};
    const std::array visitors {"Juniper", "Rowan", "Hazel", "Wren"};

    for (const auto visitor : visitors) {

        // Charge before printing, so refusal leaves this ticket unissued:
        if (!tickets.try_charge(datum::resource::work, 1)) {
            std::println("no ticket: {}", visitor);
            break;
        }
        std::println("ticket: {}", visitor);
    }

    std::println("tickets issued: {}", tickets.used(datum::resource::work));
}
