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
