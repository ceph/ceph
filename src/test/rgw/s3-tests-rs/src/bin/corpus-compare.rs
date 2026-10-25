//! Compare the three captures of one adopted tree.
//!
//! ```text
//! corpus-compare <capture-root> <expected-difference-list> <md5-on|md5-off>
//! ```
//!
//! `<capture-root>` holds `noobaa/`, `rgw-unadopted/` and
//! `rgw-converted/`, as `adoption/standup.rs` writes them.  Two pairs are
//! compared:  `noobaa:unadopted`, where a difference no row accounts
//! for is an adoption defect, and `unadopted:converted`, where it is a
//! conversion defect.  The rules are `s3_tests_rs::corpus::compare`.
//!
//! Exits 1 if any difference is unexpected, breaks its row's class, or
//! is a known defect (a `known` row), 2 if the inputs cannot be read.
//! Known defects are reported under their own heading, with the row's
//! reason, so that a new failure stands out from them.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::ExitCode;

use s3_tests_rs::corpus::compare::{differences, evaluate, parse_rows, Capture, Class,
                                   Difference, Outcome};

const PAIRS: [(&str, &str, &str, &str); 2] = [
    ("noobaa:unadopted", "adoption", "noobaa", "rgw-unadopted"),
    ("unadopted:converted", "conversion", "rgw-unadopted", "rgw-converted"),
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
            match evaluate(d, &rows, pair, modality) {
                Outcome::Expected { row } => {
                    expected += 1;
                    *used.entry(row).or_default() += 1;
                }
                Outcome::Violates { row, why } => {
                    *used.entry(row).or_default() += 1;
                    violations.push(format!("line {row}, {why}:  {}", describe(d)));
                }
                Outcome::Unexpected => unexpected.push(describe(d)),
                Outcome::Known { row } => {
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

    /* Two kinds of row match nothing, and they mean opposite things.  A
     * guard -- `equal` or `filtered` -- asserts a difference must never
     * occur, so in a good run it matches nothing.  Any other class
     * expects a difference, so one that found none may be stale.
     * Reported under separate headings so the list is not read as one
     * list of things to remove. */
    let unused: Vec<_> = rows
        .iter()
        .filter(|r| (r.when.is_empty() || r.when == *modality) && !used.contains_key(&r.line))
        .collect();
    let class_name = |c: &Class| match c {
        Class::Equal => "equal".to_string(),
        Class::Recognize(r) => format!("recognize:{r}"),
        Class::Differs => "differs".to_string(),
        Class::Vanishes => "vanishes".to_string(),
        Class::Appears => "appears".to_string(),
        Class::Absent => "absent".to_string(),
        Class::Known => "known".to_string(),
        Class::Filtered => "filtered".to_string(),
    };
    let is_guard = |c: &Class| matches!(c, Class::Equal | Class::Filtered);
    if !unused.is_empty() {
        println!("== rows that matched no difference in {modality}");
        for (guards, heading) in [
            (true, "guards -- equal and filtered rows, which match nothing in a good run:"),
            (false, "possibly stale -- these expect a difference and found none:"),
        ] {
            println!("  {heading}");
            let mut any = false;
            for r in unused.iter().filter(|r| is_guard(&r.class) == guards) {
                println!("    line {}:  {}  {}  {}  ({})", r.line, r.pair, r.artifact,
                         r.key, class_name(&r.class));
                any = true;
            }
            if !any {
                println!("    (none)");
            }
        }
    }
    if failed { ExitCode::from(1) } else { ExitCode::SUCCESS }
}
