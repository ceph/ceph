// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * The fields NooBaa writes in its configuration records, by release.
 *
 * X4's second layer (DECISIONS.md 2026-10-21):  a record that parses
 * and holds a field this tree does not know is not imported.  NooBaa
 * holds itself to the same set -- at noobaa-core `08d063f86` its
 * record schemas are made strict (`nsfs_schema_utils.js`, `strictify()`
 * with `additionalProperties: false`) and every account and bucket
 * record is validated when NooBaa reads it -- so a field outside the
 * tree is one the NooBaa we are verified against would not serve.
 *
 * The tree mirrors NooBaa's schema:  an object's fields, an array's
 * items, a map's values, and `oneOf`/`anyOf`/`allOf` kept as
 * alternatives rather than merged.  It checks keys and the
 * object/array/value shape, never types or required fields;  those are
 * NooBaa's validation, and a record NooBaa wrote already passed it.
 *
 * VERSIONED THE WAY CEPH'S ENCODINGS ARE.  NooBaa's records carry no
 * version, so the store's comes from `system.json` (layer 1).  Each
 * field carries `since`, the first NooBaa release that writes it, and a
 * field is known only to a store at that release or later -- the
 * `if (struct_v >= N)` of a decoder.  Unlike `DECODE_FINISH`, nothing
 * skips an unknown field:  adoption fails closed.  Adding a release
 * means walking NooBaa's schema change and giving each new field its
 * `since` and a disposition, and then adding the release to
 * `reviewed_releases`, which is also the version gate's verified set.
 */

#pragma once

#include <span>
#include <string>
#include <string_view>

class JSONObj;

namespace rgw { namespace sal { namespace nsfs { namespace noobaa_schema {

/* What the import does with a record-level field.  A nested field's is
 * its record-level field's (`Inherit`). */
enum class Disposition {
  Inherit,
  /* the import writes RGW state from it */
  Translated,
  /* read and kept or reported, not enforced (X1's force_md5_etag) */
  Carried,
  /* known and not read today */
  NotCarried,
  /* known, and a record holding it is not imported */
  Refused,
};

struct Node;

struct Field {
  const char* name;
  /* the first NooBaa release that writes it */
  const char* since;
  Disposition disposition;
  const Node* value;
};

struct Node {
  enum class Kind {
    /* anything, as `{}` is to NooBaa's validator */
    Open,
    /* a string, number, boolean or null */
    Scalar,
    /* `fields`;  any other key when `open` */
    Object,
    /* every value is `each`, whatever its key */
    Map,
    /* every item is `each` */
    Array,
    /* one of `arms` holds it:  NooBaa's oneOf and anyOf */
    AnyOf,
    /* every one of `arms` holds it */
    AllOf,
  };
  Kind kind;
  std::span<const Field> fields{};
  const Node* each{nullptr};
  std::span<const Node* const> arms{};
  bool open{false};
};

/* `identities/<id>/identity.json`:  accounts, IAM users and roles share
 * one schema */
extern const Node& account;
/* `buckets/<name>.json` */
extern const Node& bucket;

/* The NooBaa releases this tree has been reviewed against, oldest
 * first.  The version gate admits exactly these. */
extern const std::span<const std::string_view> reviewed_releases;

/* One of NooBaa's object attributes:  `user.noobaa.<name>`, or every
 * name under `user.noobaa.<name>` when `prefix` -- X4's third layer
 * (DECISIONS.md 2026-10-26).  Versioned as the record fields are. */
struct ObjectAttribute {
  const char* name;
  bool prefix;
  const char* since;
  Disposition disposition;
};

extern const std::span<const ObjectAttribute> object_attributes;

/* Whether `xattr`, a full attribute name, is one NooBaa at the newest
 * reviewed release writes.  True for any name outside `user.noobaa.`:
 * user metadata and nsfs's own names are not NooBaa's to define. */
bool object_attribute_known(std::string_view xattr);

/* The path of the first key in `record` that NooBaa at `release` does
 * not write, with why;  empty when every key is known.
 *
 * `release` is the store's dotted version.  One that does not parse is
 * read as the newest reviewed release, and one older than the oldest
 * as the oldest, since `since` says nothing about releases before the
 * first review:  so a skewed store's unknown fields are still named,
 * and the version gate is what refuses the release itself. */
std::string unknown_field(const Node& schema, JSONObj* record,
			  std::string_view release);

}}}} // namespace rgw::sal::nsfs::noobaa_schema
