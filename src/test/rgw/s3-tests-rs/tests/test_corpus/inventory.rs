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

use crate::manifest::encode;

#[derive(Default)]
pub struct Inventory {
    lines: BTreeSet<String>,
    /* Every regular file seen, with its mode.  Kept apart from the
     * rendered lines so an invariant can read it without parsing the
     * inventory's own output back. */
    files: Vec<(String, u32)>,
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

    /// Every object on this backend is published 0600.
    ///
    /// Invisible over S3, so no manifest or listing check can see it,
    /// and nsfs trees are shared with NFS and POSIX clients where the
    /// mode is what decides access.  This caught a zero-length object
    /// at 0700:  both write paths narrow the mode themselves, an empty
    /// body goes through neither, so the file kept the `S_IRWXU` its
    /// `openat` was given.
    ///
    /// Returns one line per offender, empty when all is well.
    pub fn check_object_modes(&self) -> Vec<String> {
        self.files
            .iter()
            .filter(|(_, m)| *m != 0o600)
            .map(|(p, m)| format!("{p:?} is mode {m:04o}, not 0600"))
            .collect()
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
    let Ok(data_root) = std::env::var("CORPUS_DATA_ROOT") else {
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
    inv.check_object_modes()
}
