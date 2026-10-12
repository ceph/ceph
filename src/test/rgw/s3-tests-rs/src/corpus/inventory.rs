//! The disk inventory:  what S3 cannot say.
//!
//! Same format discipline as the manifest -- line-oriented, sorted
//! bytewise, percent-encoded -- because the comparison is `diff`.
//!
//! Two questions this answers and the manifest cannot:
//!
//!   * **Does anything of NooBaa's remain?**  `NSFS_ADOPTION.md` §8
//!     says a bucket is converted when no object carries a
//!     NooBaa-named attribute, and that is not observable over S3.
//!     Attribute **names** are recorded, not values:  the values are
//!     what the manifest already covers.
//!   * **Did a created file get the ownership it should have?**
//!     Conversion creates a sentinel and an upload's staging files,
//!     and they must carry the uid and gid of what they re-express.
//!
//! No timestamps and no inode numbers.  They differ on every run and
//! carry nothing the comparison wants.

use std::collections::BTreeSet;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use super::env;
use super::manifest::{decode, encode};

#[derive(Default)]
pub struct Inventory {
    lines: BTreeSet<String>,
    /* Every regular file seen, with its mode.  Kept apart from the
     * rendered lines so an invariant can read it without parsing the
     * inventory's own output back. */
    files: Vec<(String, u32)>,
    /* every directory, the same way */
    dirs: Vec<(String, u32)>,
}

impl Inventory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.lines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        for l in &self.lines {
            out.push_str(l);
            out.push('\n');
        }
        out
    }

    /// Walk `root`, recording every entry below it.  Paths are recorded
    /// **relative to `root`**, so an inventory taken on one gateway
    /// compares with one taken on another.
    pub fn walk(&mut self, root: &Path) -> std::io::Result<()> {
        self.walk_at(root, root)
    }

    fn walk_at(&mut self, root: &Path, dir: &Path) -> std::io::Result<()> {
        let mut entries: Vec<_> =
            std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
        /* read_dir order is the filesystem's;  sort so a partial walk
         * is still deterministic.  The BTreeSet sorts the output
         * anyway, but a failure part-way through should truncate at a
         * predictable place. */
        entries.sort_by_key(|e| e.file_name());

        for e in entries {
            let path = e.path();
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .as_os_str()
                .to_string_lossy()
                .to_string();

            /* lstat:  a symlink is recorded as itself, never followed.
             * Following one would record the target twice and could
             * leave the tree entirely. */
            let md = std::fs::symlink_metadata(&path)?;
            let ty = if md.file_type().is_symlink() {
                "l"
            } else if md.is_dir() {
                "d"
            } else if md.is_file() {
                "f"
            } else {
                "o"
            };

            self.lines.insert(format!(
                "{}\t{}\t{:04o}\t{}\t{}\t{}",
                encode(&rel),
                ty,
                md.mode() & 0o7777,
                md.uid(),
                md.gid(),
                if ty == "f" { md.size() } else { 0 }
            ));

            if ty == "f" {
                self.files.push((rel.clone(), md.mode() & 0o7777));
            } else if ty == "d" {
                self.dirs.push((rel.clone(), md.mode() & 0o7777));
            }

            for name in list_xattr_names(&path)? {
                self.lines
                    .insert(format!("{}\txattr\t{}", encode(&rel), encode(&name)));
            }

            if md.is_dir() && !md.file_type().is_symlink() {
                self.walk_at(root, &path)?;
            }
        }
        Ok(())
    }

    /// Every file under a bucket is 0660, NooBaa's mode, which nsfs
    /// creates with too:  0666 under `rgw_nsfs_umask`, default 0007.
    ///
    /// Invisible over S3, so no manifest or listing check can see it,
    /// and nsfs trees are shared with NFS and POSIX clients where the
    /// mode is what decides access.  This caught a zero-length object
    /// at 0700, when the mode was still narrowed on the write path and
    /// an empty body went through neither.
    ///
    /// **`CORPUS_OBJECT_MODE` says which mode, and `any` turns the
    /// check off.**  This is the one assertion in the corpus that is
    /// not a property of every S3 implementation -- it is a statement
    /// about the tree -- and a run with a different `rgw_nsfs_umask`
    /// has to be able to say so.
    ///
    /// Returns one line per offender, empty when all is well.
    pub fn check_object_modes(&self) -> Vec<String> {
        let spec = std::env::var(env::OBJECT_MODE).ok();
        Self::modes_against(&self.files, &mode_policy(spec.as_deref(), 0o660),
                            env::OBJECT_MODE)
    }

    /// Every directory under a bucket is 0770, NooBaa's directory mode,
    /// which nsfs creates with too:  0777 under `rgw_nsfs_umask`.
    /// `CORPUS_DIR_MODE` says which, as `CORPUS_OBJECT_MODE` does for
    /// files -- a run with impersonation on, where the gateway owns the
    /// directories it makes, needs it.
    pub fn check_directory_modes(&self) -> Vec<String> {
        let spec = std::env::var(env::DIR_MODE).ok();
        Self::modes_against(&self.dirs, &mode_policy(spec.as_deref(), 0o770),
                            env::DIR_MODE)
    }

    /// The same check against a policy given rather than read, so both
    /// polarities are testable without mutating the environment -- the
    /// axes run in one process and share it.
    fn modes_against(entries: &[(String, u32)], policy: &ModePolicy,
                     knob: &str) -> Vec<String> {
        let want = match policy {
            ModePolicy::Any => return Vec::new(),
            /* A knob nobody can read is worse than no knob:  reported
             * as a problem, so the axis fails and names it, rather
             * than silently falling back to the default. */
            ModePolicy::Malformed(v) => {
                return vec![format!(
                    "{knob}={v:?} is neither an octal mode nor `any`"
                )]
            }
            ModePolicy::Exactly(m) => *m,
        };
        entries
            .iter()
            .filter(|(_, m)| *m != want)
            .map(|(p, m)| format!("{p:?} is mode {m:04o}, not {want:04o}"))
            .collect()
    }
}

/// One inventory line, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    /// `path  type  mode  uid  gid  size`, mode in octal;  size is 0
    /// for anything but a regular file
    Stat {
        path: String,
        kind: String,
        mode: u32,
        uid: u32,
        gid: u32,
        size: u64,
    },
    /// `path  xattr  name`
    Xattr { path: String, name: String },
}

impl Entry {
    pub fn path(&self) -> &str {
        match self {
            Entry::Stat { path, .. } | Entry::Xattr { path, .. } => path,
        }
    }
}

/// Read one line.  `None` for a line in neither shape.
pub fn parse_line(line: &str) -> Option<Entry> {
    let f: Vec<&str> = line.split('\t').collect();
    match f.as_slice() {
        [path, "xattr", name] => Some(Entry::Xattr {
            path: decode(path),
            name: decode(name),
        }),
        [path, kind, mode, uid, gid, size] => Some(Entry::Stat {
            path: decode(path),
            kind: kind.to_string(),
            mode: u32::from_str_radix(mode, 8).ok()?,
            uid: uid.parse().ok()?,
            gid: gid.parse().ok()?,
            size: size.parse().ok()?,
        }),
        _ => None,
    }
}

/// Read a rendered inventory.  Lines that do not parse are returned
/// apart, so a reader can report them rather than drop them.
pub fn parse(text: &str) -> (Vec<Entry>, Vec<String>) {
    let mut entries = Vec::new();
    let mut bad = Vec::new();
    for line in text.lines().filter(|l| !l.is_empty()) {
        match parse_line(line) {
            Some(e) => entries.push(e),
            None => bad.push(line.to_string()),
        }
    }
    (entries, bad)
}

/// What `check_object_modes` requires.
#[derive(Debug, PartialEq, Eq)]
enum ModePolicy {
    /// do not look at modes at all
    Any,
    /// every regular file must be exactly this
    Exactly(u32),
    /// the knob was set to something unreadable
    Malformed(String),
}

/// Read the knob.  Unset is the strict default.
fn mode_policy(spec: Option<&str>, default: u32) -> ModePolicy {
    let Some(raw) = spec else {
        return ModePolicy::Exactly(default);
    };
    let v = raw.trim();
    if v.eq_ignore_ascii_case("any") {
        return ModePolicy::Any;
    }
    /* Octal without needing the prefix, because a mode is written
     * octal:  `0600`, `600` and `0o600` are the same answer. */
    let digits = v.trim_start_matches("0o").trim_start_matches("0O");
    match u32::from_str_radix(digits, 8) {
        Ok(m) if m <= 0o7777 => ModePolicy::Exactly(m),
        _ => ModePolicy::Malformed(raw.to_string()),
    }
}

/// Extended attribute **names** on a path, without following symlinks.
///
/// `std` wraps `stat` but not `listxattr`, so this is the one place the
/// inventory needs libc.  `llistxattr` rather than `listxattr`, for the
/// same reason the walk uses `lstat`.
///
/// Note that this omits `user.*` names the caller cannot read -- that is
/// `listxattr`'s documented behaviour and the reason impersonated
/// conversion is unsafe (`docs/IMPERSONATED_XATTR.md`).  The corpus runs
/// unimpersonated, so it sees everything.
fn list_xattr_names(path: &Path) -> std::io::Result<Vec<String>> {
    let c = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let want = unsafe { libc::llistxattr(c.as_ptr(), std::ptr::null_mut(), 0) };
    if want < 0 {
        let err = std::io::Error::last_os_error();
        /* ENOTSUP:  the filesystem has no xattrs.  Not an error for an
         * inventory -- it means there are none to record. */
        if err.raw_os_error() == Some(libc::ENOTSUP) {
            return Ok(Vec::new());
        }
        return Err(err);
    }
    if want == 0 {
        return Ok(Vec::new());
    }

    let mut buf = vec![0i8; want as usize];
    let got =
        unsafe { libc::llistxattr(c.as_ptr(), buf.as_mut_ptr(), buf.len()) };
    if got < 0 {
        return Err(std::io::Error::last_os_error());
    }

    /* a NUL-separated list */
    let bytes: Vec<u8> = buf[..got as usize].iter().map(|b| *b as u8).collect();
    Ok(bytes
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).to_string())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The walk records what it is told to and nothing it is not:  mode,
    /// ownership and type, with no timestamp or inode to vary between
    /// runs.
    #[test]
    fn walk_records_mode_type_and_xattr_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::create_dir(root.join("sub")).unwrap();
        std::fs::write(root.join("sub/f"), b"abc").unwrap();

        let mut inv = Inventory::new();
        inv.walk(root).expect("walk");
        let out = inv.render();

        assert!(out.contains("sub\td\t"), "the directory is missing: {out}");
        assert!(
            out.lines().any(|l| l.starts_with("sub/f\tf\t") && l.ends_with("\t3")),
            "the file's type or size is wrong: {out}"
        );
        assert!(
            !out.contains("mtime") && !out.contains("ino"),
            "the inventory recorded something that varies per run: {out}"
        );
    }

    /// Strict unless told otherwise, and an unreadable value is a
    /// complaint rather than a silent default.
    #[test]
    fn the_mode_knob_defaults_strict() {
        assert_eq!(mode_policy(None, 0o660), ModePolicy::Exactly(0o660));
        assert_eq!(mode_policy(None, 0o770), ModePolicy::Exactly(0o770));
        assert_eq!(mode_policy(Some("any"), 0o660), ModePolicy::Any);
        assert_eq!(mode_policy(Some("ANY"), 0o660), ModePolicy::Any);
        assert_eq!(mode_policy(Some("0644"), 0o660), ModePolicy::Exactly(0o644));
        assert_eq!(mode_policy(Some("644"), 0o660), ModePolicy::Exactly(0o644));
        assert_eq!(mode_policy(Some(" 0600 "), 0o660), ModePolicy::Exactly(0o600));
        assert!(matches!(mode_policy(Some("rw-"), 0o660), ModePolicy::Malformed(_)));
        /* 0o10000 is not a mode */
        assert!(matches!(mode_policy(Some("10000"), 0o660), ModePolicy::Malformed(_)));
    }

    /// The check has to be able to fail, and the knob has to be able to
    /// stop it failing.  Both polarities on one offending file:  a
    /// knob whose negative case is unreachable would prove nothing
    /// about what a capture from their gateway suppresses.
    #[test]
    fn an_offending_mode_is_caught_by_default_and_only_by_default() {
        let files = vec![("sub/f".to_string(), 0o700)];
        let k = "CORPUS_OBJECT_MODE";

        let strict = Inventory::modes_against(&files, &ModePolicy::Exactly(0o660), k);
        assert_eq!(strict.len(), 1, "0700 was not caught: {strict:?}");
        assert!(
            strict[0].contains("0700") && strict[0].contains("0660"),
            "the complaint does not say what it found or wanted: {strict:?}"
        );

        assert!(
            Inventory::modes_against(&files, &ModePolicy::Any, k).is_empty(),
            "`any` still complained"
        );
        assert!(
            Inventory::modes_against(&files, &ModePolicy::Exactly(0o700), k).is_empty(),
            "0700 was wanted and still complained"
        );
        let bad = Inventory::modes_against(&files, &ModePolicy::Malformed("rw-".into()), k);
        assert_eq!(bad.len(), 1, "a malformed knob did not report itself");
        assert!(bad[0].contains(k), "the complaint does not name its knob");
    }

    /// Directories are checked apart from files, so a 0770 directory
    /// is not an offending file and a 0660 file not an offending
    /// directory.  Both polarities, on a real walk.
    #[test]
    fn directories_are_checked_against_their_own_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::create_dir(root.join("d")).unwrap();
        std::fs::set_permissions(root.join("d"),
                                 std::fs::Permissions::from_mode(0o770)).unwrap();
        std::fs::write(root.join("d/f"), b"x").unwrap();
        std::fs::set_permissions(root.join("d/f"),
                                 std::fs::Permissions::from_mode(0o660)).unwrap();

        let mut inv = Inventory::new();
        inv.walk(root).expect("walk");
        assert!(Inventory::modes_against(&inv.dirs, &ModePolicy::Exactly(0o770), "d")
                .is_empty());
        assert!(Inventory::modes_against(&inv.files, &ModePolicy::Exactly(0o660), "f")
                .is_empty());
        assert_eq!(Inventory::modes_against(&inv.dirs, &ModePolicy::Exactly(0o750), "d")
                   .len(), 1, "a 0770 directory passed a 0750 check");
    }

    /// A walk's rendering parses back to the entries it recorded, file
    /// and attribute lines alike.
    #[test]
    fn a_rendered_inventory_parses_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::write(root.join("f\tx"), b"abc").unwrap();
        let mut inv = Inventory::new();
        inv.walk(root).expect("walk");
        let (entries, bad) = parse(&inv.render());
        assert!(bad.is_empty(), "lines did not parse: {bad:?}");
        assert!(entries.iter().any(|e| matches!(e,
            Entry::Stat { path, kind, size: 3, .. } if path == "f\tx" && kind == "f")),
            "the file did not parse back: {entries:?}");
        assert_eq!(parse_line("p\tf\t0660\t1\t1"), None, "a short line parsed");
        assert_eq!(parse_line("p\txattr\tuser.a"),
                   Some(Entry::Xattr { path: "p".into(), name: "user.a".into() }));
    }

    /// Sorted bytewise and free of duplicates, so the comparison is a
    /// plain diff.  The negative case is reachable:  read_dir order is
    /// the filesystem's, not sorted.
    #[test]
    fn walk_output_is_sorted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        for n in ["c", "a", "b"] {
            std::fs::write(root.join(n), b"x").unwrap();
        }
        let mut inv = Inventory::new();
        inv.walk(root).expect("walk");
        let rendered = inv.render();
        let keys: Vec<&str> = rendered
            .lines()
            .map(|l| l.split('\t').next().unwrap())
            .collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        assert_eq!(keys, sorted, "not sorted bytewise");
    }
}

/// Take the disk half for one bucket, write it beside the manifest, and
/// return what the disk-level invariants object to.
///
/// Optional because the S3 half runs anywhere and the filesystem half
/// cannot:  `CORPUS_DATA_ROOT` names the gateway's data root, and
/// without it the inventory is simply not taken (and nothing is
/// asserted -- a remote run must not fail an invariant it cannot
/// measure).  A workflow needing both on separate hosts carries the
/// tree.
///
/// One function rather than a block per axis, so a new axis gets the
/// invariants by calling this rather than by remembering to.
pub fn capture(bucket: &str, dir: &Path, stem: &str) -> Vec<String> {
    let Ok(data_root) = std::env::var(env::DATA_ROOT) else {
        return Vec::new();
    };
    let bucket_dir = Path::new(&data_root).join(bucket);
    let mut inv = Inventory::new();
    if let Err(e) = inv.walk(&bucket_dir) {
        eprintln!(
            "corpus {stem}: no inventory, {} is not readable: {e}",
            bucket_dir.display()
        );
        return Vec::new();
    }
    let ipath = dir.join(format!("{stem}.inventory"));
    std::fs::write(&ipath, inv.render()).expect("write inventory");
    eprintln!("corpus {stem}: {} disk facts -> {}", inv.len(), ipath.display());
    let mut problems = inv.check_object_modes();
    problems.extend(inv.check_directory_modes());
    problems
}

/// The attribute names on a directory itself.
///
/// `walk()` records what is *under* a root and not the root, and a
/// bucket's configuration lives on the bucket directory rather than in
/// it -- so this is the one thing the walk cannot answer.  Returns an
/// empty list when the path is unreadable, which is the same as "the
/// disk half is not being taken".
pub fn root_xattr_names(root: &Path) -> Vec<String> {
    list_xattr_names(root).unwrap_or_default()
}

/// A directory's own attributes as name plus a digest of the value.
///
/// Names alone are not enough to tell whether a configuration was
/// persisted.  A configuration that updates an attribute the bucket
/// already has -- anything folded into a bucket-info blob -- adds no
/// name, so a names-only comparison reads as "stored nowhere" when it
/// is in fact stored.  Comparing digests catches the modification as
/// well as the addition.
///
/// The digest is FNV-1a, which is not a security property:  it only
/// has to differ when the bytes differ.
pub fn root_xattr_digest(root: &Path) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for name in root_xattr_names(root) {
        out.push((name.clone(), fnv1a(&read_xattr(root, &name).unwrap_or_default())));
    }
    out.sort();
    out
}

/// FNV-1a.  Not a security property:  it only has to differ when the
/// bytes differ.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        h ^= *byte as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

/// A JSON record's top-level fields, name plus a digest of the value.
///
/// For a backend that keeps a bucket's configuration in a record
/// rather than in the bucket directory's attributes.  NooBaa is one:
/// `tag`, `cors_configuration_rules`, `s3_policy`,
/// `lifecycle_configuration_rules`, `logging` and `encryption` all
/// live in `<config_root>/buckets/<name>.json`, and none of them
/// reaches the directory.
///
/// **Which keys exist is read from the file.**  Nothing here encodes
/// NooBaa's schema -- no field name appears in this function -- which
/// is the point:  interpreting their record is what I1 owns, and
/// composing our reading of it is exactly what the capture exists to
/// stop relying on.  A name and a digest is an observation, not an
/// interpretation.
///
/// An unreadable or non-object file yields nothing, which is the same
/// answer as "this backend has no record".
pub fn record_field_digest(path: &Path) -> Vec<(String, u64)> {
    let Ok(text) = std::fs::read(path) else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&text) else {
        return Vec::new();
    };
    let Some(map) = value.as_object() else {
        return Vec::new();
    };
    let mut out: Vec<(String, u64)> = map
        .iter()
        .map(|(k, v)| (k.clone(), fnv1a(v.to_string().as_bytes())))
        .collect();
    out.sort();
    out
}

/// One attribute's value, without following symlinks.
fn read_xattr(path: &Path, name: &str) -> std::io::Result<Vec<u8>> {
    let cp = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let cn = CString::new(name)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let len = unsafe {
        libc::lgetxattr(cp.as_ptr(), cn.as_ptr(), std::ptr::null_mut(), 0)
    };
    if len < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut buf = vec![0u8; len as usize];
    let got = unsafe {
        libc::lgetxattr(
            cp.as_ptr(),
            cn.as_ptr(),
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
        )
    };
    if got < 0 {
        return Err(std::io::Error::last_os_error());
    }
    buf.truncate(got as usize);
    Ok(buf)
}
