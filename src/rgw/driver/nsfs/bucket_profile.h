// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace rgw { namespace sal { namespace nsfs {

class XattrStrategy;
class PathStrategy;

/* Whether a bucket carries our extensions, and what that resolves to.
 *
 * THE UNMARKED CASE IS NOOBAA'S.  A bucket directory with no marker is
 * read as NooBaa would write it and we add nothing to it:  no shadow
 * subtree, no positional layout, no S3 ACL attribute.  Marking is a
 * declared act -- at creation, or by adopting an existing tree -- and it
 * is what buys the structure our own drivers want.
 *
 * That polarity is the point.  A NooBaa root we are asked to serve is
 * unmarked by construction, because NooBaa never wrote a marker, so the
 * correct reading of it is the default rather than something an operator
 * has to remember to configure.  And a tree we have not extended can go
 * back:  rollback after cutover is refusing to mark, not undoing a
 * conversion.  See project_noobaa_interchange.
 *
 * It also means every nsfs tree written before this existed reads as
 * base, although it was written with our extensions.  Those trees are
 * adopted or wiped;  there is no way to tell them apart from a NooBaa
 * tree by inspection, which is exactly why the marker had to exist.
 */

/* The marker.  A physical xattr name, deliberately NOT routed through
 * XattrStrategy::disk_name().
 *
 * Reading it through a strategy would mean choosing the strategy first,
 * and choosing the strategy is what the marker is for.  Worse, a NooBaa
 * XattrStrategy will not claim a user.nsfs.* name at all -- it returns
 * false from parse_disk_name() and the attribute is dropped as foreign --
 * so the marker would be invisible in precisely the case it decides.
 * It is read with its own fgetxattr, before any strategy is consulted. */
inline constexpr const char* EXTENSIONS_XATTR = "user.nsfs.extensions";

/* THE VALUE IS A SET, NOT A LEVEL.
 *
 * A decimal integer in the attribute, read as a bitmask:  one bit per
 * extension, and a profile is a named combination of them.  Not an
 * ordinal, because features are expected to migrate into the shared
 * profile one at a time -- a mask says that by defining a bit, where an
 * ordinal would say it by renumbering every level above.
 *
 * A bucket carrying ANY bit this build does not implement is refused
 * rather than served on a guess.  Not a fallback and not a warning:  the
 * bits we do not know are the ones whose structure we would corrupt.
 *
 * We only ever write the named profiles below, so a combination nobody
 * named is something to refuse, not something we produce. */

/* 0 is not a value;  it is the absence of the attribute, and it means
 * the bucket is NooBaa-native. */
inline constexpr uint32_t EXTENSIONS_NONE = 0;

/* The extensions, one bit each.
 *
 * SHADOW is first because it is what the FSIO interlock already gates,
 * so a bucket already carrying `1` keeps its meaning and no tree needs
 * rewriting.
 *
 * A bit is declared here as soon as it is named, but it belongs in
 * EXTENSIONS_KNOWN only once this build can actually serve a bucket that
 * carries it.  POSITIONAL is the live example:  named, not implemented,
 * and therefore a bucket carrying it is refused rather than served
 * wrongly. */
inline constexpr uint32_t EXT_SHADOW      = 0x1;  /* .shadow, FSIO */
inline constexpr uint32_t EXT_ACLS        = 0x2;  /* S3 access control lists */
inline constexpr uint32_t EXT_POSITIONAL  = 0x4;  /* positional IO layout --
                                                   * NOT IMPLEMENTED */
/* Object metadata in RGW's representation:  our attribute names and our
 * packing, on NooBaa's layout.
 *
 * It adds no file and no directory, so without a bit of its own the
 * shared profile would be an empty set -- and an empty set is not
 * separable from a bucket carrying no attribute at all.  Anything that
 * strips xattrs turns one into the other, and we would then write
 * NooBaa's attribute names onto objects holding ours.  A profile has to
 * be a value somebody can read back. */
inline constexpr uint32_t EXT_RGW_META    = 0x8;

/* The bits this build can serve.  The refusal test, and the reason a
 * declared-but-unbuilt extension must stay out of it:  accepting a bit
 * we cannot honour is exactly the corruption the rule exists to
 * prevent. */
inline constexpr uint32_t EXTENSIONS_KNOWN =
    EXT_SHADOW | EXT_ACLS | EXT_RGW_META;

/* The named profiles.
 *
 * base    -- no attribute at all.  Read and written as NooBaa does, and
 *            nothing of ours is added.  It is an absence and not a
 *            value, so it is the one profile with no mask.
 * shared  -- RGW's metadata representation on NooBaa's layout, and
 *            whatever features move in later;  the candidates are object
 *            tagging and object lock, and neither has landed.  It exists
 *            so that S5 has somewhere to put them that is not `strong`.
 * strong  -- everything this build can serve.  Not everything named:
 *            POSITIONAL is declared and unbuilt, so it is not here, and
 *            a bucket must never be marked with an extension the writer
 *            cannot produce.
 *
 * A tree is handable back to NooBaa only at `base`. */
inline constexpr uint32_t EXTENSIONS_BASE   = EXTENSIONS_NONE;
inline constexpr uint32_t EXTENSIONS_SHARED = EXT_RGW_META;
inline constexpr uint32_t EXTENSIONS_STRONG = EXTENSIONS_KNOWN;

/* The extension set a profile name asks for, or nothing for a name we do
 * not write.
 *
 * Callers from outside the driver name a profile and not a bitmask:  the
 * mask is how the answer is stored, and "shared" is what an operator or
 * a test means.  `base` is the absence of the attribute, so setting it
 * removes rather than writes. */
inline std::optional<uint32_t> extensions_for_profile(std::string_view name)
{
  if (name == "base") return EXTENSIONS_BASE;
  if (name == "shared") return EXTENSIONS_SHARED;
  if (name == "strong") return EXTENSIONS_STRONG;
  return std::nullopt;
}

/* Whether a set is one of the three we name.  A bucket must never be
 * left carrying a combination nobody defined, so this gates what can be
 * written as well as what can be read back. */
inline bool is_named_profile(uint32_t ext)
{
  return (ext == EXTENSIONS_BASE) || (ext == EXTENSIONS_SHARED) ||
	 (ext == EXTENSIONS_STRONG);
}

/* What a newly created bucket is marked with, when the deployment marks
 * at all.  See rgw_nsfs_extensions.
 *
 * The mark states what the tree may contain, so it must not claim an
 * extension this build cannot write. */
inline constexpr uint32_t EXTENSIONS_DEFAULT = EXTENSIONS_STRONG;

/* What a bucket's marker resolves to.
 *
 * The strategy pointers are the reason this is a struct rather than a
 * bool.  Both profiles resolve to the same instances today, because the
 * noobaa implementations are S5;  what S4 buys is that S5 can land one
 * strategy at a time without touching the call sites again. */
struct BucketProfile {
  uint32_t extensions{EXTENSIONS_NONE};
  XattrStrategy* xattr_strategy{nullptr};
  PathStrategy* path_strategy{nullptr};

  bool has(uint32_t ext) const { return (extensions & ext) != 0; }

  /* "does this bucket carry anything of ours" -- the question the
   * interlocks ask before serving an extension-only interface */
  bool extended() const { return extensions != EXTENSIONS_NONE; }

  const char* name() const {
    if (extensions == EXTENSIONS_BASE) return "base";
    if (extensions == EXTENSIONS_SHARED) return "shared";
    if (extensions == EXTENSIONS_STRONG) return "strong";
    return "mixed";
  }
};

}}} // namespace rgw::sal::nsfs
