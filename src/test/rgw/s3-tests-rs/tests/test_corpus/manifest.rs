//! The S3 manifest:  what S3 says about the corpus.
//!
//! Line-oriented, one fact per line, sorted bytewise.  The comparison
//! between two gateways is then `diff`, a difference names the *fact*
//! rather than the object, and a failure is readable without tooling.
//!
//! One fact per line is deliberately verbose.  A line per object means
//! one changed fact rewrites the line, and the diff stops saying what
//! actually differs.

use std::collections::BTreeSet;

/// Accumulates facts and emits them sorted.
///
/// `BTreeSet` rather than a vector:  it sorts bytewise as it goes and
/// it makes a duplicate fact impossible to emit twice, which would
/// otherwise show up as a spurious difference.
#[derive(Default)]
pub struct Manifest {
    lines: BTreeSet<String>,
}

impl Manifest {
    pub fn new() -> Self {
        Self::default()
    }

    /// `scope \t name \t field \t value`
    pub fn fact(&mut self, scope: &str, name: &str, field: &str, value: &str) {
        self.lines.insert(format!(
            "{}\t{}\t{}\t{}",
            scope,
            encode(name),
            field,
            encode(value)
        ));
    }

    pub fn object(&mut self, key: &str, field: &str, value: &str) {
        self.fact("object", key, field, value);
    }

    pub fn bucket(&mut self, name: &str, field: &str, value: &str) {
        self.fact("bucket", name, field, value);
    }

    pub fn len(&self) -> usize {
        self.lines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// Sorted, newline-terminated, ready to `diff`.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for l in &self.lines {
            out.push_str(l);
            out.push('\n');
        }
        out
    }
}

/// Percent-encode anything that would break the format or vary by
/// locale:  the field separator, the escape character itself, and
/// everything outside printable ASCII.
///
/// **An S3 key may contain a tab or a newline**, and the corpus
/// deliberately generates keys that are awkward, so the format cannot
/// assume otherwise.  Encoding is applied to names and values alike so
/// a reader needs one rule rather than two.
pub fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'%' => out.push_str("%25"),
            b'\t' => out.push_str("%09"),
            b'\n' => out.push_str("%0A"),
            b'\r' => out.push_str("%0D"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The encoding has to survive the keys the corpus generates, and
    /// the negative case is reachable:  an unencoded tab would split a
    /// line into the wrong number of fields.
    #[test]
    fn encoding_keeps_one_fact_on_one_line() {
        assert_eq!(encode("plain"), "plain");
        assert_eq!(encode("a\tb"), "a%09b");
        assert_eq!(encode("a\nb"), "a%0Ab");
        assert_eq!(encode("100%"), "100%25");
        assert_eq!(encode("k-\u{00e9}"), "k-%C3%A9");
        /* a trailing space survives, because it is part of the key */
        assert_eq!(encode("trailing "), "trailing ");
    }

    #[test]
    fn facts_sort_and_deduplicate() {
        let mut m = Manifest::new();
        m.object("b", "size", "1");
        m.object("a", "size", "2");
        m.object("a", "size", "2");
        let out = m.render();
        assert_eq!(m.len(), 2, "the duplicate fact was emitted twice");
        assert!(
            out.starts_with("object\ta\tsize\t2\n"),
            "not sorted bytewise: {out:?}"
        );
    }
}
