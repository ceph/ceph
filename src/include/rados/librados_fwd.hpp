#ifndef __LIBRADOS_FWD_HPP
#define __LIBRADOS_FWD_HPP

struct blkin_trace_info;

namespace opentelemetry {
inline namespace v1 {
namespace trace {

class SpanContext;

} // namespace trace
} // inline namespace v1
} // namespace opentelemetry

using jspan_context = opentelemetry::v1::trace::SpanContext;

namespace libradosstriper {

class RadosStriper;

} // namespace libradosstriper

namespace librados {
inline namespace v14_2_0 {

class AioCompletion;
class IoCtx;
class ListObject;
class NObjectIterator;
class ObjectCursor;
class ObjectItem;
class ObjectOperation;
class ObjectOperationCompletion;
class ObjectReadOperation;
class ObjectWriteOperation;
class PlacementGroup;
class PoolAsyncCompletion;
class Rados;
class WatchCtx;
class WatchCtx2;

} // inline namespace v14_2_0
} // namespace librados

#endif // __LIBRADOS_FWD_HPP
