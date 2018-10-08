// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
#define RBD_FEATURE_OPERATIONS          (1ULL<<8)
#define RBD_FEATURE_MIGRATING           (1ULL<<9)
#define RBD_FEATURE_IMAGE_CACHE         (1ULL<<10)

#define RBD_FEATURES_DEFAULT             (RBD_FEATURE_LAYERING | \
                                         RBD_FEATURE_EXCLUSIVE_LOCK | \
                                         RBD_FEATURE_OBJECT_MAP | \
                                         RBD_FEATURE_FAST_DIFF | \
                                         RBD_FEATURE_DEEP_FLATTEN)

#define RBD_FEATURE_NAME_LAYERING        "layering"
#define RBD_FEATURE_NAME_STRIPINGV2      "striping"
#define RBD_FEATURE_NAME_EXCLUSIVE_LOCK  "exclusive-lock"
#define RBD_FEATURE_NAME_OBJECT_MAP      "object-map"
#define RBD_FEATURE_NAME_FAST_DIFF       "fast-diff"
#define RBD_FEATURE_NAME_DEEP_FLATTEN    "deep-flatten"
#define RBD_FEATURE_NAME_JOURNALING      "journaling"
#define RBD_FEATURE_NAME_DATA_POOL       "data-pool"
#define RBD_FEATURE_NAME_OPERATIONS      "operations"
#define RBD_FEATURE_NAME_MIGRATING       "migrating"
#define RBD_FEATURE_NAME_IMAGE_CACHE     "image-cache"

/// features that make an image inaccessible for read or write by
/// clients that don't understand them
#define RBD_FEATURES_INCOMPATIBLE 	(RBD_FEATURE_LAYERING       | \
                                         RBD_FEATURE_STRIPINGV2     | \
                                         RBD_FEATURE_DATA_POOL      | \
                                         RBD_FEATURE_IMAGE_CACHE)

/// features that make an image unwritable by clients that don't understand them
#define RBD_FEATURES_RW_INCOMPATIBLE	(RBD_FEATURES_INCOMPATIBLE  | \
                                         RBD_FEATURE_EXCLUSIVE_LOCK | \
                                         RBD_FEATURE_OBJECT_MAP     | \
                                         RBD_FEATURE_FAST_DIFF      | \
                                         RBD_FEATURE_DEEP_FLATTEN   | \
                                         RBD_FEATURE_JOURNALING     | \
                                         RBD_FEATURE_OPERATIONS     | \
                                         RBD_FEATURE_MIGRATING)

#define RBD_FEATURES_ALL          	(RBD_FEATURE_LAYERING       | \
                                         RBD_FEATURE_STRIPINGV2     | \
                                         RBD_FEATURE_EXCLUSIVE_LOCK | \
                                         RBD_FEATURE_OBJECT_MAP     | \
                                         RBD_FEATURE_FAST_DIFF      | \
                                         RBD_FEATURE_DEEP_FLATTEN   | \
                                         RBD_FEATURE_JOURNALING     | \
                                         RBD_FEATURE_DATA_POOL      | \
                                         RBD_FEATURE_OPERATIONS     | \
                                         RBD_FEATURE_MIGRATING      | \
                                         RBD_FEATURE_IMAGE_CACHE)

/// features that may be dynamically enabled or disabled
#define RBD_FEATURES_MUTABLE            (RBD_FEATURE_EXCLUSIVE_LOCK | \
                                         RBD_FEATURE_OBJECT_MAP     | \
                                         RBD_FEATURE_FAST_DIFF      | \
                                         RBD_FEATURE_JOURNALING     | \
                                         RBD_FEATURE_IMAGE_CACHE)

/// features that may be dynamically disabled
#define RBD_FEATURES_DISABLE_ONLY       (RBD_FEATURE_DEEP_FLATTEN)

/// features that only work when used with a single client
/// using the image for writes
#define RBD_FEATURES_SINGLE_CLIENT      (RBD_FEATURE_EXCLUSIVE_LOCK | \
                                         RBD_FEATURE_OBJECT_MAP     | \
                                         RBD_FEATURE_FAST_DIFF      | \
                                         RBD_FEATURE_JOURNALING     | \
                                         RBD_FEATURE_IMAGE_CACHE)

/// features that will be implicitly enabled
#define RBD_FEATURES_IMPLICIT_ENABLE    (RBD_FEATURE_STRIPINGV2 | \
                                         RBD_FEATURE_DATA_POOL  | \
                                         RBD_FEATURE_FAST_DIFF  | \
                                         RBD_FEATURE_OPERATIONS | \
                                         RBD_FEATURE_MIGRATING)

/// features that cannot be controlled by the user
#define RBD_FEATURES_INTERNAL           (RBD_FEATURE_OPERATIONS | \
                                         RBD_FEATURE_MIGRATING)

#define RBD_OPERATION_FEATURE_CLONE_PARENT      (1ULL<<0)
#define RBD_OPERATION_FEATURE_CLONE_CHILD       (1ULL<<1)
#define RBD_OPERATION_FEATURE_GROUP             (1ULL<<2)
#define RBD_OPERATION_FEATURE_SNAP_TRASH        (1ULL<<3)

#define RBD_OPERATION_FEATURE_NAME_CLONE_PARENT "clone-parent"
#define RBD_OPERATION_FEATURE_NAME_CLONE_CHILD  "clone-child"
#define RBD_OPERATION_FEATURE_NAME_GROUP        "group"
#define RBD_OPERATION_FEATURE_NAME_SNAP_TRASH   "snap-trash"

/// all valid operation features
#define RBD_OPERATION_FEATURES_ALL (RBD_OPERATION_FEATURE_CLONE_PARENT | \
                                    RBD_OPERATION_FEATURE_CLONE_CHILD  | \
                                    RBD_OPERATION_FEATURE_GROUP        | \
                                    RBD_OPERATION_FEATURE_SNAP_TRASH)

#endif
/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
