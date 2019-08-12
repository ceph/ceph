#ifndef __LIBRADOS_FWD_HPP
#define __LIBRADOS_FWD_HPP

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
