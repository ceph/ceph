#ifndef CEPH_RBD_FEATURES_H
#define CEPH_RBD_FEATURES_H

#define RBD_FEATURE_LAYERING		(1ULL<<0)
#define RBD_FEATURE_STRIPINGV2		(1ULL<<1)
#define RBD_FEATURE_EXCLUSIVE_LOCK	(1ULL<<2)
#define RBD_FEATURE_OBJECT_MAP		(1ULL<<3)
#define RBD_FEATURE_FAST_DIFF           (1ULL<<4)
#define RBD_FEATURE_DEEP_FLATTEN        (1ULL<<5)
#define RBD_FEATURE_JOURNALING          (1ULL<<6)
#define RBD_FEATURE_DATA_POOL           (1ULL<<7)

/// features that make an image inaccessible for read or write by
/// clients that don't understand them
#define RBD_FEATURES_INCOMPATIBLE 	(RBD_FEATURE_LAYERING       | \
					 RBD_FEATURE_STRIPINGV2     | \
                                         RBD_FEATURE_DATA_POOL)

/// features that make an image unwritable by clients that don't understand them
#define RBD_FEATURES_RW_INCOMPATIBLE	(RBD_FEATURES_INCOMPATIBLE  | \
					 RBD_FEATURE_EXCLUSIVE_LOCK | \
					 RBD_FEATURE_OBJECT_MAP     | \
                                         RBD_FEATURE_FAST_DIFF      | \
                                         RBD_FEATURE_DEEP_FLATTEN   | \
                                         RBD_FEATURE_JOURNALING)

#define RBD_FEATURES_ALL          	(RBD_FEATURE_LAYERING       | \
					 RBD_FEATURE_STRIPINGV2     | \
                                   	 RBD_FEATURE_EXCLUSIVE_LOCK | \
                                         RBD_FEATURE_OBJECT_MAP     | \
                                         RBD_FEATURE_FAST_DIFF      | \
                                         RBD_FEATURE_DEEP_FLATTEN   | \
                                         RBD_FEATURE_JOURNALING     | \
                                         RBD_FEATURE_DATA_POOL)

/// features that may be dynamically enabled or disabled
#define RBD_FEATURES_MUTABLE            (RBD_FEATURE_EXCLUSIVE_LOCK | \
                                         RBD_FEATURE_OBJECT_MAP     | \
                                         RBD_FEATURE_FAST_DIFF      | \
                                         RBD_FEATURE_JOURNALING)

/// features that may be dynamically disabled
#define RBD_FEATURES_DISABLE_ONLY       (RBD_FEATURE_DEEP_FLATTEN)

/// features that only work when used with a single client
/// using the image for writes
#define RBD_FEATURES_SINGLE_CLIENT (RBD_FEATURE_EXCLUSIVE_LOCK | \
                                    RBD_FEATURE_OBJECT_MAP     | \
                                    RBD_FEATURE_FAST_DIFF      | \
                                    RBD_FEATURE_JOURNALING)

#endif
