#include "transaction.h"
#include "crimson/common/interruptible_future.h"

namespace crimson::interruptible {
template
thread_local interrupt_cond_t<::crimson::os::seastore::TransactionConflictCondition>
interrupt_cond<::crimson::os::seastore::TransactionConflictCondition>;
}
