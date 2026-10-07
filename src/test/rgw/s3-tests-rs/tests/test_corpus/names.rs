//! Identifiers, generated rather than fixed.
//!
//! Every name a corpus artifact carries is **derived from the parameters
//! that produced it**, never random.  A manifest keyed on random names
//! cannot be diffed between two gateways, and diffing two gateways is
//! the whole purpose of the corpus.
//!
//! The classes below are the ones that stress naming:  depth, the
//! trailing slash that makes a directory object, unicode, length, and
//! the characters an S3 key may carry but a filesystem path or an HTTP
//! header handles awkwardly.  Each is named so coverage can be stated
//! rather than estimated.

/// A key class, and the key it generates.
///
/// The name is the identifier *and* the label:  a manifest line reads
/// `object  k-trailing-slash/  size  0` and says what it is testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyClass {
    /// the ordinary case, against which the others are read
    Plain,
    /// one prefix component
    Depth1,
    /// three prefix components
    Depth3,
    /// **a directory object** -- theirs is a directory carrying
    /// `user.noobaa.dir_content`, ours is a sentinel inside it
    TrailingSlash,
    /// a key that is also a prefix:  `foo` exists *and* `foo/bar` does.
    /// Their layout produces it naturally;  ours cannot write it
    BothObjectAndPrefix,
    /// the same, the child half
    BothObjectAndPrefixChild,
    /// multi-byte characters in the key
    Unicode,
    /// at S3's 1024-byte key limit, in one path component.  **A
    /// filesystem backend refuses this**:  a component is bounded by
    /// NAME_MAX, 255 on Linux, so the key is unwritable and the corpus
    /// records the refusal rather than the object
    MaxLengthOneComponent,
    /// at S3's 1024-byte limit, spread over components short enough to
    /// be path names.  The writable form of the same limit
    MaxLengthSplit,
    /// characters that must survive percent-encoding on the wire
    NeedsEncoding,
    /// trailing space, which a filesystem keeps and a human cannot see
    TrailingSpace,
    /// a name our scaffolding also uses
    ReservedFolder,
    /// a name under their versions directory
    ReservedVersions,
    /// a name under their temp prefix, which they hide by prefix match
    ReservedTemp,
    /// an arbitrary dotfile, which they list as an ordinary object
    Dotfile,
}

impl KeyClass {
    pub const COVERAGE: &'static [KeyClass] = &[
        KeyClass::Plain,
        KeyClass::Depth1,
        KeyClass::Depth3,
        KeyClass::TrailingSlash,
        KeyClass::BothObjectAndPrefix,
        KeyClass::BothObjectAndPrefixChild,
        KeyClass::Unicode,
        KeyClass::MaxLengthOneComponent,
        KeyClass::MaxLengthSplit,
        KeyClass::NeedsEncoding,
        KeyClass::TrailingSpace,
        KeyClass::ReservedFolder,
        KeyClass::ReservedVersions,
        KeyClass::ReservedTemp,
        KeyClass::Dotfile,
    ];

    /// The key itself.  Stable across runs and across gateways.
    pub fn key(self) -> String {
        match self {
            KeyClass::Plain => "k-plain".into(),
            KeyClass::Depth1 => "d1/k-depth1".into(),
            KeyClass::Depth3 => "d1/d2/d3/k-depth3".into(),
            KeyClass::TrailingSlash => "k-dirobj/".into(),
            KeyClass::BothObjectAndPrefix => "k-both".into(),
            KeyClass::BothObjectAndPrefixChild => "k-both/child".into(),
            KeyClass::Unicode => "k-unicode-\u{00e9}\u{4e2d}\u{6587}".into(),
            // 1024 is the S3 limit;  the prefix keeps it identifiable
            KeyClass::MaxLengthOneComponent => {
                let head = "k-maxlen1-";
                let mut s = String::from(head);
                s.push_str(&"x".repeat(1024 - head.len()));
                s
            }
            // the same 1024 bytes, in components under NAME_MAX
            KeyClass::MaxLengthSplit => {
                let head = "k-maxlens";
                let seg = "y".repeat(200);
                let mut s = String::from(head);
                while s.len() + 1 + seg.len() <= 1024 {
                    s.push('/');
                    s.push_str(&seg);
                }
                let pad = 1024 - s.len();
                s.push_str(&"z".repeat(pad));
                s
            }
            KeyClass::NeedsEncoding => "k-encode a+b%c#d?e".into(),
            KeyClass::TrailingSpace => "k-trailing-space ".into(),
            KeyClass::ReservedFolder => ".folder".into(),
            KeyClass::ReservedVersions => ".versions/k-under-versions".into(),
            KeyClass::ReservedTemp => ".noobaa-nsfs-k-under-temp".into(),
            KeyClass::Dotfile => ".fofopidoo".into(),
        }
    }

    /// Why this key is in the corpus, carried into the manifest so a
    /// reader of a diff knows what the line was for.
    pub fn rationale(self) -> &'static str {
        match self {
            KeyClass::Plain => "the ordinary case",
            KeyClass::Depth1 | KeyClass::Depth3 => "prefix depth",
            KeyClass::TrailingSlash => "directory object",
            KeyClass::BothObjectAndPrefix
            | KeyClass::BothObjectAndPrefixChild => {
                "a key that is also a prefix;  RGW cannot write this"
            }
            KeyClass::Unicode => "multi-byte key",
            KeyClass::MaxLengthOneComponent => {
                "S3's 1024-byte key limit in one component, which NAME_MAX refuses"
            }
            KeyClass::MaxLengthSplit => {
                "the same limit split into components a path can hold"
            }
            KeyClass::NeedsEncoding => "characters needing percent-encoding",
            KeyClass::TrailingSpace => "a trailing space is part of the key",
            KeyClass::ReservedFolder => "collides with our sentinel name",
            KeyClass::ReservedVersions => "under their versions directory",
            KeyClass::ReservedTemp => "under their temp prefix",
            KeyClass::Dotfile => "an arbitrary dotfile is an ordinary object",
        }
    }
}

/// Bucket names, also derived.  `tier` separates one run's buckets from
/// another's without making them random.
pub fn bucket(tier: &str, label: &str) -> String {
    format!("corpus-{tier}-{label}")
}
