/* Copyright contributors to the Ceph project */
'use strict';

/*
 * NooBaa's own validator over records NooBaa wrote, and over each with
 * one key it does not write, for `standup.rs schema-agreement`.
 *
 * usage:  node schema-agreement.js <noobaa-core worktree>/src < records
 *
 * stdin is one record per line, `account<TAB>json` or `bucket<TAB>json`.
 * stdout is one case per line, `kind<TAB>refused<TAB>form<TAB>json`,
 * where refused is 1 when NooBaa's validator rejects the record, and
 * form is `record` for the record itself or `variant` for one of the
 * variants that follow it, one per object inside it with the key
 * `standup_unknown_field` added there.  The nsfs unit test
 * `NooBaaSchema.AgreesWithNooBaasValidator` reads these and requires the
 * driver's schema tree to accept and refuse exactly the same records.
 *
 * Validated the way `manage_nsfs/nsfs_schema_utils.js` does at the pin:
 * requiring it strictifies the schemas in place, and the validator is
 * built with the same keywords over the same definitions.  A variant
 * differs from a record NooBaa accepted by one key, so the key is the
 * only thing that can make NooBaa refuse it.
 */

const path = require('path');
const { createRequire } = require('module');
const readline = require('readline');

const src = process.argv[2];
const from = createRequire(path.join(src, 'index.js'));
from('./manage_nsfs/nsfs_schema_utils.js');
const { default: Ajv } = from('ajv');
const { KEYWORDS } = from('./util/schema_keywords');
const common_api = from('./api/common_api');
const schemas = {
    account: from('./server/system_services/schemas/nsfs_account_schema'),
    bucket: from('./server/system_services/schemas/nsfs_bucket_schema'),
};

const ajv = new Ajv({ verbose: true, allErrors: true });
for (const k of ['methods', 'doc', 'date', 'idate', 'objectid', 'binary', 'wrapper']) {
    ajv.addKeyword(KEYWORDS[k]);
}
ajv.addSchema(common_api);
const validate = {
    account: ajv.compile(schemas.account),
    bucket: ajv.compile(schemas.bucket),
};

/* every object inside v, by path, the record itself included */
function objects(v, at, out) {
    if (Array.isArray(v)) {
        v.forEach((x, i) => objects(x, at.concat(i), out));
    } else if (v && typeof v === 'object') {
        out.push(at);
        for (const [k, x] of Object.entries(v)) objects(x, at.concat(k), out);
    }
    return out;
}

/* on a copy:  the `wrapper` keyword replaces each string it wraps */
function emit(kind, form, record) {
    const refused = validate[kind](JSON.parse(JSON.stringify(record))) ? 0 : 1;
    process.stdout.write(`${kind}\t${refused}\t${form}\t${JSON.stringify(record)}\n`);
}

const rl = readline.createInterface({ input: process.stdin });
rl.on('line', line => {
    const tab = line.indexOf('\t');
    if (tab < 0) return;
    const kind = line.slice(0, tab);
    const record = JSON.parse(line.slice(tab + 1));
    emit(kind, 'record', record);
    for (const at of objects(record, [], [])) {
        const variant = JSON.parse(JSON.stringify(record));
        let node = variant;
        for (const k of at) node = node[k];
        node.standup_unknown_field = 1;
        emit(kind, 'variant', variant);
    }
});
