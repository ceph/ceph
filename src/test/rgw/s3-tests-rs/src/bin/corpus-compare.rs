//! Compare the three captures of one adopted tree.
//!
//! ```text
//! corpus-compare <capture-root> <expected-difference-list> <md5-on|md5-off>
//! ```
//!
//! `<capture-root>` holds `noobaa/`, `rgw-unconverted/` and
//! `rgw-converted/`, as `adoption/standup.rs` writes them.  Two pairs are
//! compared:  `noobaa:unconverted`, where a difference no row accounts
//! for is an adoption defect, and `unconverted:converted`, where it is a
//! conversion defect.  The rules are `s3_tests_rs::corpus::compare`.
//!
//! Exits 1 if any difference is unexpected, breaks its row's class, or
//! is a known defect (a `known` row), 2 if the inputs cannot be read.
//! Known defects are reported under their own heading, with the row's
//! reason, so that a new failure stands out from them.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::ExitCode;

use s3_tests_rs::corpus::compare::{differences, judge, parse_rows, Capture, Difference,
                                   Verdict};

const PAIRS: [(&str, &str, &str, &str); 2] = [
    ("noobaa:unconverted", "adoption", "noobaa", "rgw-unconverted"),
    ("unconverted:converted", "conversion", "rgw-unconverted", "rgw-converted"),
];

fn show(v: &Option<std::collections::BTreeSet<String>>) -> String {
    match v {
        None => "<absent>".to_string(),
        Some(s) => s.iter().cloned().collect::<Vec<_>>().join(" | "),
    }
}

fn describe(d: &Difference) -> String {
    format!("{}.{}  {}  {}:  {}  ->  {}",
            d.stem, d.artifact, d.subject, d.field, show(&d.left), show(&d.right))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [root, list, modality] = args.as_slice() else {
        eprintln!("usage:  corpus-compare <capture-root> <expected-difference-list> \
                   <md5-on|md5-off>");
        return ExitCode::from(2);
    };
    let rows = match std::fs::read_to_string(list)
        .map_err(|e| format!("cannot read {list}: {e}"))
        .and_then(|t| parse_rows(&t).map_err(|e| format!("{list}: {e}")))
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };

    let mut failed = false;
    let mut used: BTreeMap<usize, usize> = BTreeMap::new();
    for (pair, kind, l, r) in PAIRS {
        let read = |d: &str| Capture::read(&Path::new(root).join(d));
        let (left, right) = match (read(l), read(r)) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(e), _) | (_, Err(e)) => {
                eprintln!("{e}");
                return ExitCode::from(2);
            }
        };
        let diffs = differences(&left, &right);
        let mut unexpected = Vec::new();
        let mut violations = Vec::new();
        let mut known: BTreeMap<usize, Vec<String>> = BTreeMap::new();
        let mut expected = 0usize;
        for d in &diffs {
            match judge(d, &rows, pair, modality) {
                Verdict::Expected { row } => {
                    expected += 1;
                    *used.entry(row).or_default() += 1;
                }
                Verdict::Violates { row, why } => {
                    *used.entry(row).or_default() += 1;
                    violations.push(format!("line {row}, {why}:  {}", describe(d)));
                }
                Verdict::Unexpected => unexpected.push(describe(d)),
                Verdict::Known { row } => {
                    *used.entry(row).or_default() += 1;
                    known.entry(row).or_default().push(describe(d));
                }
            }
        }
        let nknown: usize = known.values().map(Vec::len).sum();
        println!("== {kind} ({pair}):  {} differences, {expected} expected, {} \
                  breaking their row, {} unexpected, {nknown} known defects",
                 diffs.len(), violations.len(), unexpected.len());
        for v in &violations {
            println!("  BREAKS      {v}");
        }
        for u in &unexpected {
            println!("  UNEXPECTED  {u}");
        }
        for (line, facts) in &known {
            let reason = rows.iter().find(|r| r.line == *line)
                .map(|r| r.reason.as_str()).unwrap_or("");
            println!("  KNOWN       line {line}, {} facts:  {reason}", facts.len());
            for f in facts {
                println!("                {f}");
            }
        }
        failed |= !violations.is_empty() || !unexpected.is_empty() || nknown > 0;
    }

    let unused: Vec<_> = rows
        .iter()
        .filter(|r| (r.when.is_empty() || r.when == *modality) && !used.contains_key(&r.line))
        .collect();
    if !unused.is_empty() {
        println!("== rows that matched no difference in {modality}");
        for r in unused {
            println!("  line {}:  {}  {}  {}", r.line, r.pair, r.artifact, r.key);
        }
    }
    if failed { ExitCode::from(1) } else { ExitCode::SUCCESS }
}
