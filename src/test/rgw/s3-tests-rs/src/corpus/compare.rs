//! Comparing two captures of one tree, against the expected-difference
//! list.
//!
//! A capture is a directory of artifacts, `<stem>.manifest` and
//! `<stem>.inventory` per axis, written by the corpus.  Two captures are
//! compared artifact by artifact:  every fact or entry is keyed, and a
//! key present on one side only, or with different values on the two
//! sides, is a difference.  Each difference is matched against the rows
//! of the expected-difference list for that pair of captures, and one no
//! row accounts for is unexpected.
//!
//! **The list's classes**, each a statement about the one pair its row
//! names:
//!
//! | class | meaning |
//! |---|---|
//! | `equal` | values must be identical |
//! | `recognize:<r>` | both values pass through normalizer `<r>`, then must be identical |
//! | `differs` | values may differ |
//! | `vanishes` | present on the left, must be absent on the right |
//! | `absent` | present on one side only, either side |
//! | `filtered` | should never appear;  the writer drops it |
//!
//! A row may also name a subject -- an object, a bucket, a path -- so it
//! accounts for a difference on that subject only.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::{inventory, manifest};

/* ---------------------------------------------------------------- */
/* the expected-difference list */

/// What a row says about a difference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Class {
    Equal,
    Recognize(String),
    Differs,
    Vanishes,
    /// the reverse of Vanishes:  absent on the left, present on the right
    Appears,
    Absent,
    /// a known defect, not yet fixed:  any difference matches, and the
    /// comparison still fails, reporting it under its own heading so a
    /// new failure is not lost among the known ones
    Known,
    Filtered,
}

/// One row of the expected-difference list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// line number in the file, for the report
    pub line: usize,
    pub pair: String,
    /// `md5-on`, `md5-off`, or empty for both
    pub when: String,
    /// `manifest`, `inventory` or `run`
    pub artifact: String,
    /// which subject the row is about, as a pattern:  `object <key>` or
    /// `bucket <name>` for a manifest, a path for an inventory.  Empty
    /// means any, so a row about one object does not excuse the same
    /// difference on every other
    pub subject: String,
    pub key: String,
    pub class: Class,
    pub reason: String,
}

/// Read the list.  Comment and blank lines are skipped;  a malformed row
/// is an error naming its line, since a row the comparison silently
/// ignored would let a difference through.
pub fn parse_rows(text: &str) -> Result<Vec<Row>, String> {
    let mut rows = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let n = i + 1;
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split('\t').collect();
        if f.len() != 7 {
            return Err(format!("line {n}: {} columns, not 7", f.len()));
        }
        let class = match f[5] {
            "equal" => Class::Equal,
            "differs" => Class::Differs,
            "vanishes" => Class::Vanishes,
            "appears" => Class::Appears,
            "absent" => Class::Absent,
            "known" => Class::Known,
            "filtered" => Class::Filtered,
            c => match c.strip_prefix("recognize:") {
                Some(r) if normalizer(r).is_some() => Class::Recognize(r.to_string()),
                Some(r) => return Err(format!("line {n}: no normalizer {r:?}")),
                None => return Err(format!("line {n}: no class {c:?}")),
            },
        };
        if !matches!(f[2], "manifest" | "inventory" | "run") {
            return Err(format!("line {n}: no artifact {:?}", f[2]));
        }
        if f[6].trim().is_empty() {
            return Err(format!("line {n}: a row needs a reason"));
        }
        rows.push(Row {
            line: n,
            pair: f[0].to_string(),
            when: f[1].to_string(),
            artifact: f[2].to_string(),
            subject: f[3].to_string(),
            key: f[4].to_string(),
            class,
            reason: f[6].to_string(),
        });
    }
    Ok(rows)
}

/// `*` matches any run of characters, anywhere in the pattern.
pub fn glob(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !s.starts_with(first) || !s[first.len()..].ends_with(last)
        || s.len() < first.len() + last.len() {
        return false;
    }
    let mut rest = &s[first.len()..s.len() - last.len()];
    for p in &parts[1..parts.len() - 1] {
        match rest.find(p) {
            Some(at) => rest = &rest[at + p.len()..],
            None => return false,
        }
    }
    true
}

/* ---------------------------------------------------------------- */
/* normalizers, for `recognize:<r>` */

/// The normalizer `<r>` names, or `None` if there is none.
pub fn normalizer(name: &str) -> Option<fn(&str) -> String> {
    match name {
        "synthetic-identity" => Some(synthetic_identity),
        _ => None,
    }
}

/// A file-instance ETag token, `"mtime-<t>-ino-<i>"`, with the mtime
/// replaced by `*`.  NooBaa's mtime loses its low bits to a double, so
/// the two gateways' tokens differ there;  the inode is the same file
/// and stays.  Anything else -- a digest -- is returned unchanged, so a
/// token compared with a digest still differs.
pub fn synthetic_identity(v: &str) -> String {
    let t = v.trim_matches('"');
    if let Some(rest) = t.strip_prefix("mtime-") {
        if let Some((m, ino)) = rest.split_once("-ino-") {
            let alnum = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric());
            if alnum(m) && alnum(ino) {
                return format!("mtime-*-ino-{ino}");
            }
        }
    }
    v.to_string()
}

/* ---------------------------------------------------------------- */
/* captures */

/// One capture's artifacts, keyed.  Per stem, per artifact, per key:
/// the set of values.  A key is `subject \t field`, where for a manifest
/// the subject is `scope name` and the field the manifest's field, and
/// for an inventory the subject is the path and the field `type`,
/// `mode`, `owner`, `size` or `xattr:<name>`.
#[derive(Debug, Default)]
pub struct Capture {
    pub artifacts: BTreeMap<(String, String), BTreeMap<(String, String), BTreeSet<String>>>,
}

impl Capture {
    /// Read every `.manifest` and `.inventory` in `dir`.  Lines that do
    /// not parse are errors:  a comparison that dropped them would
    /// describe less than the capture holds.
    pub fn read(dir: &Path) -> Result<Capture, String> {
        let mut c = Capture::default();
        let entries = std::fs::read_dir(dir)
            .map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
        for e in entries.filter_map(|e| e.ok()) {
            let path = e.path();
            let name = e.file_name().to_string_lossy().to_string();
            let (stem, artifact) = if let Some(s) = name.strip_suffix(".manifest") {
                (s.to_string(), "manifest")
            } else if let Some(s) = name.strip_suffix(".inventory") {
                (s.to_string(), "inventory")
            } else {
                continue;
            };
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
            let keyed = if artifact == "manifest" {
                key_manifest(&text)
            } else {
                key_inventory(&text)
            }
            .map_err(|bad| format!("{}: lines that do not parse: {bad:?}", path.display()))?;
            c.artifacts.insert((stem, artifact.to_string()), keyed);
        }
        Ok(c)
    }
}

type Keyed = BTreeMap<(String, String), BTreeSet<String>>;

pub fn key_manifest(text: &str) -> Result<Keyed, Vec<String>> {
    let (facts, bad) = manifest::parse(text);
    if !bad.is_empty() {
        return Err(bad);
    }
    let mut k = Keyed::new();
    for f in facts {
        k.entry((format!("{} {}", f.scope, f.name), f.field))
            .or_default()
            .insert(f.value);
    }
    Ok(k)
}

pub fn key_inventory(text: &str) -> Result<Keyed, Vec<String>> {
    let (entries, bad) = inventory::parse(text);
    if !bad.is_empty() {
        return Err(bad);
    }
    let mut k = Keyed::new();
    for e in entries {
        match e {
            inventory::Entry::Stat { path, kind, mode, uid, gid, size } => {
                let mut put = |f: &str, v: String| {
                    k.entry((path.clone(), f.to_string())).or_default().insert(v);
                };
                put("type", kind);
                put("mode", format!("{mode:04o}"));
                put("owner", format!("{uid}:{gid}"));
                put("size", size.to_string());
            }
            inventory::Entry::Xattr { path, name } => {
                k.entry((path, format!("xattr:{name}")))
                    .or_default()
                    .insert("present".to_string());
            }
            inventory::Entry::Unreadable { path } => {
                k.entry((path, "unreadable".to_string()))
                    .or_default()
                    .insert("present".to_string());
            }
        }
    }
    Ok(k)
}

/* ---------------------------------------------------------------- */
/* the comparison */

/// One difference between two captures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub stem: String,
    pub artifact: String,
    /// `scope name` or a path;  empty for a whole artifact
    pub subject: String,
    /// what a row's key is matched against:  the manifest field, the
    /// inventory field, or for a missing artifact the stem
    pub field: String,
    pub left: Option<BTreeSet<String>>,
    pub right: Option<BTreeSet<String>>,
}

/// What became of a difference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// a row accounts for it
    Expected { row: usize },
    /// a row matched and its class does not allow this difference
    Violates { row: usize, why: String },
    /// no row matched
    Unexpected,
    /// a `known` row matched:  a known defect, which still fails
    Known { row: usize },
}

/// Every difference between `left` and `right`.
pub fn differences(left: &Capture, right: &Capture) -> Vec<Difference> {
    let mut out = Vec::new();
    let artifacts: BTreeSet<_> =
        left.artifacts.keys().chain(right.artifacts.keys()).cloned().collect();
    for (stem, artifact) in artifacts {
        let k = (stem.clone(), artifact.clone());
        match (left.artifacts.get(&k), right.artifacts.get(&k)) {
            (Some(l), Some(r)) => {
                let keys: BTreeSet<_> = l.keys().chain(r.keys()).cloned().collect();
                for key in keys {
                    let (lv, rv) = (l.get(&key), r.get(&key));
                    if lv == rv {
                        continue;
                    }
                    out.push(Difference {
                        stem: stem.clone(),
                        artifact: artifact.clone(),
                        subject: key.0.clone(),
                        field: key.1.clone(),
                        left: lv.cloned(),
                        right: rv.cloned(),
                    });
                }
            }
            (l, r) => out.push(Difference {
                stem: stem.clone(),
                artifact: "run".to_string(),
                subject: String::new(),
                field: stem.clone(),
                left: l.map(|_| BTreeSet::from([artifact.clone()])),
                right: r.map(|_| BTreeSet::from([artifact.clone()])),
            }),
        }
    }
    out
}

/// The row that accounts for a difference, first match wins, and what
/// its class makes of it.
pub fn evaluate(d: &Difference, rows: &[Row], pair: &str, modality: &str) -> Outcome {
    let Some(row) = rows.iter().find(|r| {
        r.pair == pair
            && (r.when.is_empty() || r.when == modality)
            && r.artifact == d.artifact
            && (r.subject.is_empty() || glob(&r.subject, &d.subject))
            && glob(&r.key, &d.field)
    }) else {
        return Outcome::Unexpected;
    };
    let violates = |why: &str| Outcome::Violates { row: row.line, why: why.to_string() };
    match (&row.class, &d.left, &d.right) {
        (Class::Known, _, _) => Outcome::Known { row: row.line },
        (Class::Equal, _, _) => violates("must be identical"),
        (Class::Filtered, _, _) => violates("should have been dropped by the writer"),
        (Class::Differs, Some(_), Some(_)) => Outcome::Expected { row: row.line },
        (Class::Differs, _, _) => violates("may differ, but must be present on both sides"),
        (Class::Vanishes, Some(_), None) => Outcome::Expected { row: row.line },
        (Class::Vanishes, _, _) => violates("must be present on the left and absent on the right"),
        (Class::Appears, None, Some(_)) => Outcome::Expected { row: row.line },
        (Class::Appears, _, _) => violates("must be absent on the left and present on the right"),
        (Class::Absent, Some(_), None) | (Class::Absent, None, Some(_)) => {
            Outcome::Expected { row: row.line }
        }
        (Class::Absent, _, _) => violates("must be present on one side only"),
        (Class::Recognize(r), Some(l), Some(rt)) => {
            let n = normalizer(r).expect("checked when the row was read");
            let norm = |s: &BTreeSet<String>| s.iter().map(|v| n(v)).collect::<BTreeSet<_>>();
            if norm(l) == norm(rt) {
                Outcome::Expected { row: row.line }
            } else {
                violates("differs after normalizing")
            }
        }
        (Class::Recognize(_), _, _) => violates("must be present on both sides to be recognized"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pair: &str, artifact: &str, key: &str, class: Class) -> Row {
        Row {
            line: 1,
            pair: pair.into(),
            when: String::new(),
            artifact: artifact.into(),
            subject: String::new(),
            key: key.into(),
            class,
            reason: "r".into(),
        }
    }

    fn diff(artifact: &str, field: &str, l: Option<&str>, r: Option<&str>) -> Difference {
        let set = |v: Option<&str>| v.map(|v| BTreeSet::from([v.to_string()]));
        Difference {
            stem: "s".into(),
            artifact: artifact.into(),
            subject: "object k".into(),
            field: field.into(),
            left: set(l),
            right: set(r),
        }
    }

    #[test]
    fn glob_matches_anywhere() {
        assert!(glob("etag", "etag"));
        assert!(!glob("etag", "head-etag"));
        assert!(glob("put-*", "put-status"));
        assert!(glob("*-failed", "head-failed"));
        assert!(glob("version.*.owner", "version.v1.owner"));
        assert!(!glob("version.*.owner", "version.v1.size"));
        assert!(glob("xattr:user.noobaa.*", "xattr:user.noobaa.version_id"));
        assert!(!glob("a*b", "ab-c"));
    }

    #[test]
    fn rows_parse_and_reject_what_is_malformed() {
        let ok = "# c\n\np\t\tmanifest\t\tetag\trecognize:synthetic-identity\twhy\n";
        assert_eq!(parse_rows(ok).unwrap().len(), 1);
        assert!(parse_rows("p\t\tmanifest\tetag\tequal\twhy\n").is_err(), "6 columns");
        assert!(parse_rows("p\t\tmanifest\t\tetag\tpersists\twhy\n").is_err(), "old class");
        assert!(parse_rows("p\t\tmanifest\t\tetag\trecognize:nope\twhy\n").is_err());
        assert!(parse_rows("p\t\tmanifest\t\tetag\tequal\t \n").is_err(), "no reason");
        assert!(parse_rows("p\t\tdisk\t\tetag\tequal\twhy\n").is_err(), "artifact");
    }

    #[test]
    fn synthetic_identity_keeps_the_inode() {
        assert_eq!(synthetic_identity("\"mtime-dm2zb26g4q9s-ino-50iai\""),
                   synthetic_identity("\"mtime-dm2zb26g4q95-ino-50iai\""));
        assert_ne!(synthetic_identity("\"mtime-a-ino-1\""),
                   synthetic_identity("\"mtime-a-ino-2\""), "another file");
        assert_eq!(synthetic_identity("\"0123abcd\""), "\"0123abcd\"", "a digest");
    }

    /* Each class, both polarities:  a difference it accounts for and one
     * it does not. */
    #[test]
    fn each_class_accepts_and_rejects() {
        let p = "a:b";
        let j = |c: Class, d: Difference| evaluate(&d, &[row(p, "manifest", "f", c)], p, "md5-off");
        let exp = |v: Outcome| matches!(v, Outcome::Expected { .. });
        let bad = |v: Outcome| matches!(v, Outcome::Violates { .. });

        assert!(bad(j(Class::Equal, diff("manifest", "f", Some("1"), Some("2")))));
        assert!(bad(j(Class::Filtered, diff("manifest", "f", Some("1"), None))));

        assert!(exp(j(Class::Differs, diff("manifest", "f", Some("1"), Some("2")))));
        assert!(bad(j(Class::Differs, diff("manifest", "f", Some("1"), None))));

        assert!(exp(j(Class::Vanishes, diff("manifest", "f", Some("1"), None))));
        assert!(bad(j(Class::Vanishes, diff("manifest", "f", None, Some("1")))));
        assert!(bad(j(Class::Vanishes, diff("manifest", "f", Some("1"), Some("2")))));

        let known = |v: Outcome| matches!(v, Outcome::Known { .. });
        assert!(known(j(Class::Known, diff("manifest", "f", Some("1"), None))));
        assert!(known(j(Class::Known, diff("manifest", "f", Some("1"), Some("2")))));

        assert!(exp(j(Class::Appears, diff("manifest", "f", None, Some("1")))));
        assert!(bad(j(Class::Appears, diff("manifest", "f", Some("1"), None))));
        assert!(bad(j(Class::Appears, diff("manifest", "f", Some("1"), Some("2")))));

        assert!(exp(j(Class::Absent, diff("manifest", "f", Some("1"), None))));
        assert!(exp(j(Class::Absent, diff("manifest", "f", None, Some("1")))));
        assert!(bad(j(Class::Absent, diff("manifest", "f", Some("1"), Some("2")))));

        let r = || Class::Recognize("synthetic-identity".into());
        assert!(exp(j(r(), diff("manifest", "f", Some("\"mtime-a-ino-1\""),
                                Some("\"mtime-b-ino-1\"")))));
        assert!(bad(j(r(), diff("manifest", "f", Some("\"mtime-a-ino-1\""),
                                Some("\"0123abcd\"")))));
    }

    /* A row about one subject accounts for that subject's difference
     * and not for the same field on another. */
    #[test]
    fn a_subject_scopes_a_row() {
        let mut r = row("a:b", "manifest", "listed", Class::Absent);
        r.subject = "object .noobaa-nsfs-*".into();
        let mut d = diff("manifest", "listed", None, Some("yes"));
        d.subject = "object .noobaa-nsfs-k-under-temp".into();
        assert!(matches!(evaluate(&d, &[r.clone()], "a:b", "md5-off"),
                         Outcome::Expected { .. }));
        d.subject = "object k-plain".into();
        assert_eq!(evaluate(&d, &[r], "a:b", "md5-off"), Outcome::Unexpected);
    }

    #[test]
    fn rows_are_scoped_to_pair_modality_and_artifact() {
        let mut r = row("a:b", "manifest", "f", Class::Differs);
        let d = diff("manifest", "f", Some("1"), Some("2"));
        assert_eq!(evaluate(&d, &[r.clone()], "b:c", "md5-off"), Outcome::Unexpected);
        assert_eq!(evaluate(&diff("inventory", "f", Some("1"), Some("2")),
                         &[r.clone()], "a:b", "md5-off"), Outcome::Unexpected);
        r.when = "md5-on".into();
        assert_eq!(evaluate(&d, &[r.clone()], "a:b", "md5-off"), Outcome::Unexpected);
        assert!(matches!(evaluate(&d, &[r], "a:b", "md5-on"), Outcome::Expected { .. }));
    }

    #[test]
    fn differences_cover_keys_values_and_whole_artifacts() {
        let mut l = Capture::default();
        let mut r = Capture::default();
        l.artifacts.insert(("ax".into(), "manifest".into()),
                           key_manifest("object\tk\tetag\t1\nobject\tk\tsize\t3\n").unwrap());
        r.artifacts.insert(("ax".into(), "manifest".into()),
                           key_manifest("object\tk\tetag\t2\nobject\tk\tsize\t3\nobject\tk\tnew\tx\n").unwrap());
        l.artifacts.insert(("lock".into(), "manifest".into()), Keyed::new());
        let d = differences(&l, &r);
        assert!(d.iter().any(|d| d.field == "etag" && d.left.is_some() && d.right.is_some()));
        assert!(d.iter().any(|d| d.field == "new" && d.left.is_none()));
        assert!(!d.iter().any(|d| d.field == "size"), "an equal fact is not a difference");
        assert!(d.iter().any(|d| d.artifact == "run" && d.field == "lock" && d.right.is_none()));
    }

    #[test]
    fn an_inventory_keys_each_attribute_and_stat_field() {
        let k = key_inventory("p\tf\t0660\t1\t2\t3\np\txattr\tuser.a\n").unwrap();
        assert!(k.contains_key(&("p".into(), "mode".into())));
        assert!(k.contains_key(&("p".into(), "owner".into())));
        assert!(k.contains_key(&("p".into(), "xattr:user.a".into())));
        assert_eq!(k[&("p".to_string(), "owner".to_string())],
                   BTreeSet::from(["1:2".to_string()]));
    }
}
