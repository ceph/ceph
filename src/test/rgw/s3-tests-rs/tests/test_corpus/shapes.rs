//! Shape axes:  what an object *is*, as against what it is called.
//!
//! Names are in `names.rs`.  These vary the content and the metadata,
//! and each class is named so coverage can be stated and so a manifest
//! line says what it was for.

/// Object size.  The interesting values are the degenerate ones and
/// whatever the deployment's part boundary is;  the multipart axis owns
/// the boundary cases, so this axis stays small and cheap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeClass {
    /// a zero-length object is an object, and a common edge
    Empty,
    /// one byte:  the smallest non-degenerate body
    OneByte,
    /// an ordinary small object
    Small,
}

impl SizeClass {
    pub const COVERAGE: &'static [SizeClass] =
        &[SizeClass::Empty, SizeClass::OneByte, SizeClass::Small];

    pub fn len(self) -> usize {
        match self {
            SizeClass::Empty => 0,
            SizeClass::OneByte => 1,
            SizeClass::Small => 1024,
        }
    }

    pub fn key(self) -> String {
        match self {
            SizeClass::Empty => "s-empty".into(),
            SizeClass::OneByte => "s-one".into(),
            SizeClass::Small => "s-small".into(),
        }
    }

    pub fn body(self) -> Vec<u8> {
        vec![b'a'; self.len()]
    }
}

/// User metadata.  Two of these are predictions rather than variations:
/// NooBaa stores an object's MD5 as `user.content_md5` and its storage
/// class as `user.storage_class`, both **outside** their
/// `user.noobaa.` namespace, and their user-metadata mapping is
/// `user.` + the key.  So a client setting `x-amz-meta-content_md5`
/// writes the attribute their gateway uses for the ETag.
///
/// `docs/NOOBAA_WRITE_SURFACE.md` §1 has the derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaClass {
    /// no user metadata at all
    None,
    /// an ordinary header
    Plain,
    /// **collides with the attribute holding the ETag**
    CollidesContentMd5,
    /// **collides with the attribute holding the storage class**
    CollidesStorageClass,
    /// a value needing encoding on the wire
    AwkwardValue,
    /// many headers, to approach the response's metadata size limit
    Many,
}

impl MetaClass {
    pub const COVERAGE: &'static [MetaClass] = &[
        MetaClass::None,
        MetaClass::Plain,
        MetaClass::CollidesContentMd5,
        MetaClass::CollidesStorageClass,
        MetaClass::AwkwardValue,
        MetaClass::Many,
    ];

    pub fn key(self) -> String {
        match self {
            MetaClass::None => "m-none".into(),
            MetaClass::Plain => "m-plain".into(),
            MetaClass::CollidesContentMd5 => "m-collide-content-md5".into(),
            MetaClass::CollidesStorageClass => "m-collide-storage-class".into(),
            MetaClass::AwkwardValue => "m-awkward".into(),
            MetaClass::Many => "m-many".into(),
        }
    }

    /// The headers to set, without the `x-amz-meta-` prefix.
    pub fn headers(self) -> Vec<(String, String)> {
        match self {
            MetaClass::None => vec![],
            MetaClass::Plain => vec![("author".into(), "nobody".into())],
            MetaClass::CollidesContentMd5 => {
                vec![("content_md5".into(), "not-a-digest".into())]
            }
            MetaClass::CollidesStorageClass => {
                vec![("storage_class".into(), "NOT_A_CLASS".into())]
            }
            MetaClass::AwkwardValue => {
                vec![("note".into(), "a b+c%d".into())]
            }
            MetaClass::Many => (0..20)
                .map(|i| (format!("k{i:02}"), "v".repeat(64)))
                .collect(),
        }
    }

    pub fn rationale(self) -> &'static str {
        match self {
            MetaClass::None => "no user metadata",
            MetaClass::Plain => "the ordinary case",
            MetaClass::CollidesContentMd5 => {
                "writes the attribute their gateway holds the ETag in"
            }
            MetaClass::CollidesStorageClass => {
                "writes the attribute their gateway holds the storage class in"
            }
            MetaClass::AwkwardValue => "a value needing encoding",
            MetaClass::Many => "approaches the metadata size limit",
        }
    }
}

/// Tag sets.  Their format stores one attribute per tag, ours stores
/// the set in one, so the count is the axis that matters.
///
/// **The boundary is ten, not fifty.**  S3 permits ten tags on an
/// *object*;  fifty is the limit for a *bucket*, and conflating the two
/// is how an earlier version of this axis generated a case that was
/// refused for being wrong rather than for being interesting.  RGW's
/// own limit agrees -- `RGWObjTags::max_obj_tags` defaults to ten.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagClass {
    None,
    One,
    /// ten:  S3's limit for an object, so the last accepted value
    AtLimit,
    /// eleven:  expected to be refused, which is the fact recorded
    OverLimit,
}

impl TagClass {
    pub const COVERAGE: &'static [TagClass] = &[
        TagClass::None,
        TagClass::One,
        TagClass::AtLimit,
        TagClass::OverLimit,
    ];

    pub fn key(self) -> String {
        match self {
            TagClass::None => "t-none".into(),
            TagClass::One => "t-one".into(),
            TagClass::AtLimit => "t-at-limit".into(),
            TagClass::OverLimit => "t-over-limit".into(),
        }
    }

    pub fn count(self) -> usize {
        match self {
            TagClass::None => 0,
            TagClass::One => 1,
            TagClass::AtLimit => 10,
            TagClass::OverLimit => 11,
        }
    }

    /// `Tagging` header form:  `k0=v0&k1=v1`.
    pub fn header(self) -> Option<String> {
        if self.count() == 0 {
            return None;
        }
        Some(
            (0..self.count())
                .map(|i| format!("tk{i:02}=tv{i:02}"))
                .collect::<Vec<_>>()
                .join("&"),
        )
    }
}

/// How many parts an upload has.
///
/// 1 and 2 are the degenerate ends:  a single-part upload still gets
/// the `-1` etag suffix, and 2 is the smallest upload with a boundary
/// in it.  **512 and 513** are named rather than generated:  the nsfs
/// staging layout keeps a record per part and overflowed its attribute
/// at 513, which was the cause of the 1000-part failure.  A generator
/// picking round numbers would have straddled it without landing on
/// it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PartCount {
    One,
    Two,
    AtStride,
    OverStride,
}

impl PartCount {
    pub fn count(self) -> usize {
        match self {
            PartCount::One => 1,
            PartCount::Two => 2,
            PartCount::AtStride => 512,
            PartCount::OverStride => 513,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            PartCount::One => "p1",
            PartCount::Two => "p2",
            PartCount::AtStride => "p512",
            PartCount::OverStride => "p513",
        }
    }
}

/// The sizes of the parts relative to each other.
///
/// `Uniform` is the shape the stride optimisation applies to -- every
/// part the same size, so a part's offset is computable rather than
/// recorded.  `ShortFinal` is the ordinary case, since an object is
/// rarely a multiple of its part size.  `Mixed` defeats the stride
/// altogether and is the one the fixture cannot generate, because it
/// chunks a total size uniformly.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PartPattern {
    Uniform,
    ShortFinal,
    Mixed,
}

impl PartPattern {
    pub fn label(self) -> &'static str {
        match self {
            PartPattern::Uniform => "uni",
            PartPattern::ShortFinal => "short",
            PartPattern::Mixed => "mixed",
        }
    }

    /// The size of each part, in order.  `base` is the part size the
    /// minimum allows (vstart sets `rgw_multipart_min_part_size=32`),
    /// and only the final part may be under it.
    pub fn sizes(self, parts: usize, base: usize) -> Vec<usize> {
        match self {
            PartPattern::Uniform => vec![base; parts],
            PartPattern::ShortFinal => {
                let mut v = vec![base; parts];
                if parts > 1 {
                    *v.last_mut().unwrap() = base / 2 + 1;
                }
                v
            }
            PartPattern::Mixed => (0..parts)
                .map(|i| base + (i % 3) * base)
                .collect(),
        }
    }
}

/// What state the upload is left in when the corpus stops.
///
/// An in-flight upload is an artifact in its own right:  it has a
/// staging layout on disk, it answers `ListMultipartUploads` and
/// `ListParts`, and conversion has to re-stage it rather than abort
/// it.  An aborted one is the control -- it must leave neither.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum UploadState {
    Completed,
    InFlight,
    Aborted,
}

impl UploadState {
    pub fn label(self) -> &'static str {
        match self {
            UploadState::Completed => "done",
            UploadState::InFlight => "open",
            UploadState::Aborted => "gone",
        }
    }
}

/// The coverage tier:  every value of every axis at least once, not
/// the cross product.  The product is the interaction tier's job.
pub const MPU_COVERAGE: &[(PartCount, PartPattern, UploadState)] = &[
    (PartCount::One, PartPattern::Uniform, UploadState::Completed),
    (PartCount::Two, PartPattern::ShortFinal, UploadState::Completed),
    (PartCount::Two, PartPattern::Mixed, UploadState::Completed),
    (PartCount::AtStride, PartPattern::Uniform, UploadState::Completed),
    (PartCount::OverStride, PartPattern::Uniform, UploadState::Completed),
    (PartCount::Two, PartPattern::Uniform, UploadState::InFlight),
    (PartCount::Two, PartPattern::Uniform, UploadState::Aborted),
];

/// What is being copied.
///
/// A multipart source is here because its ETag carries the `-<parts>`
/// suffix and a copy re-assembles the bytes, so what the copy's ETag
/// becomes is a question rather than an assumption.  Zero-length is
/// here because it is the shape that skipped the write path and so
/// skipped everything hung off it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CopySource {
    Plain,
    Empty,
    Multipart,
}

impl CopySource {
    pub fn label(self) -> &'static str {
        match self {
            CopySource::Plain => "plain",
            CopySource::Empty => "empty",
            CopySource::Multipart => "mpu",
        }
    }
}

/// Where the copy lands.
///
/// `SelfSame` is the degenerate one S3 refuses unless the metadata
/// directive is REPLACE, and it is a recorded gap on this backend, so
/// the corpus captures whatever comes back rather than asserting.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CopyDest {
    SameBucket,
    OtherBucket,
    SelfSame,
}

impl CopyDest {
    pub fn label(self) -> &'static str {
        match self {
            CopyDest::SameBucket => "same",
            CopyDest::OtherBucket => "other",
            CopyDest::SelfSame => "self",
        }
    }
}

/// Whether the copy keeps the source's metadata and tags or replaces
/// them.  Two directives, and they are independent:  REPLACE metadata
/// with COPY tagging is a real combination.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Directive {
    Copy,
    Replace,
}

impl Directive {
    pub fn label(self) -> &'static str {
        match self {
            Directive::Copy => "keep",
            Directive::Replace => "repl",
        }
    }
}

/// The coverage tier:  every value of every axis at least once.
pub const COPY_COVERAGE: &[(CopySource, CopyDest, Directive, Directive)] = &[
    (CopySource::Plain, CopyDest::SameBucket, Directive::Copy, Directive::Copy),
    (CopySource::Plain, CopyDest::OtherBucket, Directive::Replace, Directive::Replace),
    (CopySource::Plain, CopyDest::SameBucket, Directive::Replace, Directive::Copy),
    (CopySource::Empty, CopyDest::SameBucket, Directive::Copy, Directive::Copy),
    (CopySource::Multipart, CopyDest::SameBucket, Directive::Copy, Directive::Copy),
    (CopySource::Multipart, CopyDest::OtherBucket, Directive::Copy, Directive::Copy),
    (CopySource::Plain, CopyDest::SelfSame, Directive::Replace, Directive::Copy),
];

/// Object Lock state.
///
/// Worth its own axis because the two formats disagree about *shape*
/// and not only about names.  NooBaa writes three plain attributes --
/// `user.noobaa.legal_hold`, `retention_mode`, `retention_date` --
/// where legal hold is the literal `ON` or `OFF`;  ours is a
/// ceph-encoded `RGWObjectLegalHold`, and retention is two of their
/// attributes against one of ours.  `NOOBAA_ONDISK_STABILITY.md` also
/// names object lock as the part of their layout still moving, so this
/// is the axis most likely to need revisiting.
///
/// `GovernanceThenHold` exists because the two are independent:  an
/// object can carry retention and a legal hold at once, and removing
/// one must not remove the other.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LockClass {
    None,
    Governance,
    Compliance,
    LegalHoldOn,
    LegalHoldOff,
    GovernanceThenHold,
    BucketDefault,
}

impl LockClass {
    pub fn label(self) -> &'static str {
        match self {
            LockClass::None => "none",
            LockClass::Governance => "gov",
            LockClass::Compliance => "comp",
            LockClass::LegalHoldOn => "hold-on",
            LockClass::LegalHoldOff => "hold-off",
            LockClass::GovernanceThenHold => "gov-hold",
            LockClass::BucketDefault => "default",
        }
    }

    /// The retention mode this class sets explicitly, if any.  The
    /// bucket default is not set per object, which is the point of it.
    pub fn retention(self) -> Option<&'static str> {
        match self {
            LockClass::Governance | LockClass::GovernanceThenHold => Some("GOVERNANCE"),
            LockClass::Compliance => Some("COMPLIANCE"),
            _ => None,
        }
    }

    /// The legal hold this class sets explicitly, if any.
    pub fn legal_hold(self) -> Option<&'static str> {
        match self {
            LockClass::LegalHoldOn | LockClass::GovernanceThenHold => Some("ON"),
            LockClass::LegalHoldOff => Some("OFF"),
            _ => None,
        }
    }
}

pub const LOCK_COVERAGE: &[LockClass] = &[
    LockClass::None,
    LockClass::Governance,
    LockClass::Compliance,
    LockClass::LegalHoldOn,
    LockClass::LegalHoldOff,
    LockClass::GovernanceThenHold,
    LockClass::BucketDefault,
];

/// 2030-01-01T00:00:00Z, fixed rather than now-plus-an-offset.
///
/// A moving date would make every manifest differ from the last for a
/// reason that is not a change in behaviour, and the corpus exists to
/// be diffed.  Far enough out that nothing lapses mid-run.
///
/// The consequence is deliberate:  an object under COMPLIANCE
/// retention cannot be deleted through S3 until this date, by anyone.
/// The corpus tree is reset with `rgw-vstart.sh --clean`, which wipes
/// the filesystem and does not go through S3 at all.
pub const RETAIN_UNTIL_SECS: i64 = 1_893_456_000;
