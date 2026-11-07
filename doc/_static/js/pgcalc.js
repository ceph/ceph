// The PG calculator on doc/rados/operations/pgcalc. It fills <div id="pgcalc">
// and needs no libraries and nothing from other sites.
(function () {
  'use strict';

  var POW2_BELOW_THRESHOLD = 0.25;

  var FIELDS = ['poolName', 'app', 'type', 'size', 'k', 'm', 'osdNum', 'percData', 'targPGsPerOSD'];
  var TEXT_FIELDS = { poolName: 1, app: 1, type: 1 };
  var GLOBALS = [
    { key: 'size', name: 'Size', note: 'replicated pools only' },
    { key: 'osdNum', name: 'OSD #' },
    { key: 'targPGsPerOSD', name: 'Target PGs per OSD' }
  ];
  var FAILURE_DOMAINS = ['host', 'chassis', 'rack', 'row', 'room', 'datacenter'];
  var EC_DOCS = 'https://docs.ceph.com/en/latest/rados/operations/erasure-code/#erasure-coded-pool-overhead';

  function pool(poolName, app, percData, k, m) {
    return { poolName: poolName, app: app, type: k ? 'erasure' : 'replicated', size: 3, k: k || 4, m: m || 2,
      osdNum: 100, percData: percData, targPGsPerOSD: 200 };
  }

  var RGW_META = [
    pool('.rgw.root', 'rgw', 0.1),
    pool('default.rgw.control', 'rgw', 0.1),
    pool('default.rgw.meta', 'rgw', 0.1),
    pool('default.rgw.log', 'rgw', 0.1),
    pool('default.rgw.buckets.index', 'rgw', 3),
    pool('default.rgw.buckets.non-ec', 'rgw', 1)
  ];
  var PRESETS = {
    'All-in-One': [
      pool('rbd', 'rbd', 100)
    ],
    'OpenStack': [
      pool('cinder-backup', 'rbd', 25),
      pool('cinder-volumes', 'rbd', 53),
      pool('ephemeral-vms', 'rbd', 15),
      pool('glance-images', 'rbd', 7)
    ],
    'OpenStack with RGW': RGW_META.concat([
      pool('default.rgw.buckets.data', 'rgw', 19.8),
      pool('cinder-backup', 'rbd', 18),
      pool('cinder-volumes', 'rbd', 42.8),
      pool('ephemeral-vms', 'rbd', 10),
      pool('glance-images', 'rbd', 5)
    ]),
    'RGW only': RGW_META.concat([
      pool('default.rgw.buckets.data', 'rgw', 95.6)
    ]),
    'RGW with an erasure-coded data pool': RGW_META.concat([
      pool('default.rgw.buckets.data', 'rgw', 95.6, 4, 2)
    ]),
    'CephFS': [
      pool('cephfs.cephfs.meta', 'cephfs', 1),
      pool('cephfs.cephfs.data', 'cephfs', 99)
    ],
    'RBD and librados': [
      pool('rbd', 'rbd', 75),
      pool('myObjects', 'myapp', 25)
    ]
  };
  var DEFAULT_PRESET = 'OpenStack with RGW';

  // The power of two nearest to n, or the next one up if that is more than 25%
  // below n.
  function nearestPow2(n) {
    var p = Math.pow(2, Math.round(Math.log(n) / Math.log(2)));
    return p < n * (1 - POW2_BELOW_THRESHOLD) ? p * 2 : p;
  }

  // Shards per PG: the replica count, or k+m.
  function shards(r) {
    return r.type === 'erasure' ? r.k + r.m : r.size;
  }

  // Sizes, k, m, and OSD counts are whole numbers: a size of at least 1, k of
  // at least 2, and m of at least 1.
  function whole(n, min) { return n >= min && n % 1 === 0; }
  function badSize(r) { return r.type !== 'erasure' && !whole(r.size, 1); }
  function badK(r) { return r.type === 'erasure' && !whole(r.k, 2); }
  function badM(r) { return r.type === 'erasure' && !whole(r.m, 1); }

  // Suggested pg_num for one pool, or NaN if a value is missing or not usable.
  function suggestedPGs(r) {
    if (badSize(r) || badK(r) || badM(r) || !whole(r.osdNum, 1)) return NaN;
    var size = shards(r);
    if (!(size > 0 && r.osdNum > 0 && r.percData >= 0 && r.targPGsPerOSD > 0)) return NaN;
    var min = nearestPow2(Math.floor(r.osdNum / size) + 1);
    if (min < r.osdNum) min *= 2;
    var calc = nearestPow2(Math.floor(r.targPGsPerOSD * r.osdNum * r.percData / (100 * size)));
    return Math.max(min, calc);
  }

  // RGW index and metadata pools and CephFS metadata pools, which most
  // deployments should keep replicated.
  function indexOrMeta(name) {
    return name === '.rgw.root' || /(\.index|\.meta|metadata|\.log|\.control|\.non-ec)$/.test(name);
  }

  var CSS =
    '#pgcalc-root{margin:1em 0 1.5em}' +
    '#pgcalc-root .pgc-bar{display:flex;flex-wrap:wrap;gap:8px 12px;align-items:flex-end;margin-bottom:10px}' +
    '#pgcalc-root label{display:flex;flex-direction:column;gap:2px;font-size:.9em}' +
    '#pgcalc-root select,#pgcalc-root input{font:inherit;padding:3px 6px;border:1px solid #b9c3cc;border-radius:3px;background:#fff;color:inherit;box-shadow:none}' +
    '#pgcalc-root input{width:3.6em;text-align:right}' +
    '#pgcalc-root input.pgc-km{width:2.6em}' +
    '#pgcalc-root input.pgc-name{width:12em;text-align:left}' +
    '#pgcalc-root td select{padding:3px 1px}' +
    '#pgcalc-root td:first-child button{padding:2px 7px}' +
    '#pgcalc-root input.pgc-app{width:4.6em;text-align:left}' +
    '#pgcalc-root td input{padding:3px 4px}' +
    '#pgcalc-root input.pgc-root{width:7em;text-align:left}' +
    '#pgcalc-root .pgc-bad{border-color:#c62828;background:#fdecea}' +
    '#pgcalc-root .pgc-caution{border-color:#b26a00;background:#fff6e0}' +
    '#pgcalc-root button{font:inherit;font-size:.9em;padding:4px 12px;border:1px solid #b9c3cc;border-radius:3px;background:#f4f6f8;color:inherit;cursor:pointer}' +
    '#pgcalc-root button:hover{background:#e6ebef}' +
    '#pgcalc-root .pgc-scroll{overflow-x:auto;max-width:100%}' +
    '#pgcalc-root table{border-collapse:collapse;margin:0;font-size:.85em}' +
    '#pgcalc-root th,#pgcalc-root td{padding:4px 3px;border-bottom:1px solid #dde3e8;text-align:right;vertical-align:middle;white-space:nowrap}' +
    '#pgcalc-root th{white-space:normal;vertical-align:bottom;max-width:6.5em}' +
    '#pgcalc-root th:nth-child(2),#pgcalc-root td:nth-child(2),#pgcalc-root th:nth-child(3),#pgcalc-root td:nth-child(3),' +
    '#pgcalc-root th:nth-child(4),#pgcalc-root td:nth-child(4){text-align:left}' +
    '#pgcalc-root th button{padding:0;border:0;background:none;font:inherit;font-weight:bold;text-align:inherit;text-decoration:underline dotted}' +
    '#pgcalc-root td.pgc-count{font-weight:bold;font-variant-numeric:tabular-nums;color:#0a7f0a}' +
    '#pgcalc-root td.pgc-count.pgc-na{font-weight:normal;color:inherit}' +
    '#pgcalc-root tr.pgc-amber td{background:#fff4d6}' +
    '#pgcalc-root tr.pgc-amber td:first-child{box-shadow:inset 4px 0 #fab219}' +
    '#pgcalc-root tr.pgc-red td{background:#fbeaea}' +
    '#pgcalc-root tr.pgc-red td.pgc-count{color:inherit}' +
    '#pgcalc-root tr.pgc-red td:first-child{box-shadow:inset 4px 0 #d03b3b}' +
    '#pgcalc-root ul.pgc-errors{margin:8px 0;padding:8px 12px 8px 28px;border-left:4px solid #d03b3b;background:#fbeaea;font-size:.9em}' +
    '#pgcalc-root ul.pgc-cautions a{color:inherit;text-decoration:underline}' +
    '#pgcalc-root tfoot td{font-weight:bold;border-bottom:0}' +
    '#pgcalc-root .pgc-warn{color:#c62828}' +
    '#pgcalc-root ul.pgc-notes{margin:8px 0;font-size:.9em}' +
    '#pgcalc-root ul.pgc-cautions{margin:8px 0;padding:8px 12px 8px 28px;border-left:4px solid #b26a00;background:#fff6e0;font-size:.9em}' +
    '#pgcalc-root pre{max-height:28em;overflow:auto;margin:8px 0;padding:10px 12px;border:1px solid #dde3e8;background:#f8f9fa;' +
    'font-family:SFMono-Regular,Menlo,Consolas,"Liberation Mono",monospace;font-size:.95em;line-height:1.45;white-space:pre}' +
    '#pgcalc-root dialog{border:1px solid #b9c3cc;border-radius:4px;padding:16px}' +
    '#pgcalc-root dialog input{width:6em}' +
    '#pgcalc-root dialog .pgc-bar{margin:12px 0 0}';

  function el(tag, attrs, text) {
    var e = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (k) { e.setAttribute(k, attrs[k]); });
    if (text !== undefined) e.textContent = text;
    return e;
  }

  function labeled(text, control) {
    var l = el('label', {}, text);
    l.appendChild(control);
    return l;
  }

  function init(root) {
    var style = el('style');
    style.textContent = CSS;
    document.head.appendChild(style);

    var useCase = el('select');
    Object.keys(PRESETS).forEach(function (name) { useCase.appendChild(el('option', { value: name }, name)); });
    useCase.value = DEFAULT_PRESET;
    var failureDomain = el('select');
    FAILURE_DOMAINS.forEach(function (t) { failureDomain.appendChild(el('option', { value: t }, t)); });
    var crushRoot = el('input', { 'class': 'pgc-root', value: 'default', spellcheck: 'false' });
    var addBtn = el('button', { type: 'button' }, 'Add pool');
    var genBtn = el('button', { type: 'button' }, 'Generate commands');
    var bar = el('div', { 'class': 'pgc-bar' });
    [labeled('Use case', useCase), labeled('Failure domain', failureDomain), labeled('CRUSH root', crushRoot),
      addBtn, genBtn].forEach(function (e) { bar.appendChild(e); });

    var apps = el('datalist', { id: 'pgcalc-apps' });
    ['rbd', 'rgw', 'cephfs'].forEach(function (a) { apps.appendChild(el('option', { value: a })); });

    var head = el('tr');
    head.appendChild(el('th', { 'aria-label': 'Remove' }));
    head.appendChild(el('th', { scope: 'col' }, 'Pool name'));
    head.appendChild(el('th', { scope: 'col' }, 'Application'));
    head.appendChild(el('th', { scope: 'col' }, 'Type'));
    var GLOBAL_HEAD = { size: 'Size or k + m', osdNum: 'OSD #', targPGsPerOSD: 'Target PGs per OSD' };
    ['size', 'osdNum', 'percData', 'targPGsPerOSD'].forEach(function (key) {
      var th = el('th', { scope: 'col' });
      var g = GLOBALS.filter(function (x) { return x.key === key; })[0];
      if (g) {
        var b = el('button', { type: 'button', title: 'Set ' + g.name + ' for every pool' + (g.note ? ' (' + g.note + ')' : '') },
          GLOBAL_HEAD[key]);
        b.addEventListener('click', function () { askGlobal(g); });
        th.appendChild(b);
      } else {
        th.textContent = '%Data';
      }
      head.appendChild(th);
    });
    head.appendChild(el('th', { scope: 'col' }, 'Suggested PG count'));
    var thead = el('thead');
    thead.appendChild(head);
    var tbody = el('tbody');
    var foot = el('tr');
    var percCell = el('td', { colspan: '7' });
    var pgCell = el('td', { colspan: '2' });
    foot.appendChild(percCell);
    foot.appendChild(pgCell);
    var tfoot = el('tfoot');
    tfoot.appendChild(foot);
    var table = el('table');
    [thead, tbody, tfoot].forEach(function (e) { table.appendChild(e); });
    var scroll = el('div', { 'class': 'pgc-scroll' });
    scroll.appendChild(table);

    var errors = el('ul', { 'class': 'pgc-errors', role: 'alert' });
    errors.hidden = true;
    var cautions = el('ul', { 'class': 'pgc-cautions', role: 'note' });
    cautions.hidden = true;
    var notes = el('ul', { 'class': 'pgc-notes' });
    var percNote = el('li', {}, 'The total %Data should be a multiple of 100%: 100% for each set of OSDs.');
    notes.appendChild(percNote);
    notes.appendChild(el('li', {}, 'The PG total counts each PG once. To compare it with the target PGs per ' +
      'OSD, multiply each pool’s count by its size, or by k+m for an erasure-coded pool.'));
    notes.appendChild(el('li', {}, 'For simplicity, all OSDs are assumed to be the same size. When sizes vary, ' +
      'larger OSDs hold proportionally more PG replicas; see below.'));

    var cmds = el('div');
    cmds.hidden = true;
    var pre = el('pre');
    var cmdBar = el('div', { 'class': 'pgc-bar' });
    var copyBtn = el('button', { type: 'button' }, 'Copy');
    var saveLink = el('a', { download: 'pool-commands.txt', href: '#' }, 'Download');
    cmdBar.appendChild(copyBtn);
    cmdBar.appendChild(saveLink);
    cmds.appendChild(pre);
    cmds.appendChild(cmdBar);

    var dialog = el('dialog');
    var dialogForm = el('form', { method: 'dialog' });
    var dialogLabel = el('label');
    var dialogInput = el('input', { type: 'number', min: '0', step: 'any' });
    var dialogBar = el('div', { 'class': 'pgc-bar' });
    dialogBar.appendChild(el('button', { value: 'set' }, 'Set'));
    dialogBar.appendChild(el('button', { value: 'cancel', formnovalidate: '' }, 'Cancel'));
    dialogForm.appendChild(dialogLabel);
    dialogForm.appendChild(dialogBar);
    dialog.appendChild(dialogForm);

    [bar, apps, scroll, errors, cautions, notes, cmds, dialog].forEach(function (e) { root.appendChild(e); });

    var globalField = null;

    function input(tr, key) {
      return tr.querySelector('[data-key="' + key + '"]');
    }

    function rowValues(tr) {
      var v = {};
      FIELDS.forEach(function (key) {
        var value = input(tr, key).value;
        v[key] = TEXT_FIELDS[key] ? value.trim() : parseFloat(value);
      });
      return v;
    }

    function numberInput(key, label, value, cls) {
      var i = el('input', { 'data-key': key, 'aria-label': label, type: 'number', min: '0', step: 'any' });
      if (cls) i.className = cls;
      i.value = value;
      return i;
    }

    function addRow(values) {
      var tr = el('tr');
      var del = el('button', { type: 'button', title: 'Remove this pool', 'aria-label': 'Remove this pool' }, '×');
      del.addEventListener('click', function () { tr.remove(); update(); });

      var name = el('input', { 'data-key': 'poolName', 'aria-label': 'Pool name', 'class': 'pgc-name', spellcheck: 'false' });
      name.value = values.poolName;
      var app = el('input', { 'data-key': 'app', 'aria-label': 'Application', 'class': 'pgc-app', list: 'pgcalc-apps',
        spellcheck: 'false' });
      app.value = values.app;
      var type = el('select', { 'data-key': 'type', 'aria-label': 'Type' });
      type.appendChild(el('option', { value: 'replicated' }, 'Replicated'));
      type.appendChild(el('option', { value: 'erasure' }, 'Erasure'));
      type.value = values.type;

      var rep = el('span'), sizeIn = numberInput('size', 'Size', values.size);
      sizeIn.min = '1';
      sizeIn.step = '1';
      rep.appendChild(sizeIn);
      var ec = el('span');
      var kIn = numberInput('k', 'k', values.k, 'pgc-km'), mIn = numberInput('m', 'm', values.m, 'pgc-km');
      kIn.min = '2';
      kIn.step = mIn.step = '1';
      mIn.min = '1';
      ec.appendChild(kIn);
      ec.appendChild(document.createTextNode(' + '));
      ec.appendChild(mIn);

      var osdIn = numberInput('osdNum', 'OSD #', values.osdNum);
      osdIn.min = '1';
      osdIn.step = '1';
      var cells = [del, name, app, type, [rep, ec], osdIn,
        numberInput('percData', '%Data', Number(values.percData).toFixed(2)),
        numberInput('targPGsPerOSD', 'Target PGs per OSD', values.targPGsPerOSD)];
      cells.forEach(function (c) {
        var td = el('td');
        [].concat(c).forEach(function (e) { td.appendChild(e); });
        tr.appendChild(td);
      });
      tr.appendChild(el('td', { 'class': 'pgc-count' }));
      tr.querySelectorAll('input, select').forEach(function (e) {
        e.addEventListener('input', update);
        e.addEventListener('change', update);
      });
      tbody.appendChild(tr);
    }

    // A PG puts each replica or shard on its own OSD, in its own failure domain.
    function unplaceable(v) {
      return !isNaN(suggestedPGs(v)) && v.osdNum < shards(v);
    }

    function update() {
      var totalPerc = 0, totalPGs = 0, warned = [], failed = [];
      Array.prototype.forEach.call(tbody.rows, function (tr) {
        var v = rowValues(tr), pgs = suggestedPGs(v), ec = v.type === 'erasure';
        input(tr, 'size').parentNode.hidden = ec;
        input(tr, 'k').parentNode.hidden = !ec;
        if (v.percData >= 0) totalPerc += v.percData;
        input(tr, 'percData').classList.toggle('pgc-bad', !(v.percData >= 0 && v.percData <= 100));
        input(tr, 'size').classList.toggle('pgc-bad', badSize(v));
        input(tr, 'osdNum').classList.toggle('pgc-bad', !whole(v.osdNum, 1));
        input(tr, 'targPGsPerOSD').classList.toggle('pgc-bad', !(v.targPGsPerOSD > 0));
        input(tr, 'k').classList.toggle('pgc-bad', badK(v));
        input(tr, 'm').classList.toggle('pgc-bad', badM(v));
        var ecMeta = ec && indexOrMeta(v.poolName);
        input(tr, 'type').classList.toggle('pgc-caution', ecMeta);
        if (ecMeta) {
          warned.push([v.poolName, 'erasure coding for an index or metadata pool. A future release may support this, ' +
            'but most deployments should stick with replication.']);
        }
        var wide = ec && !badK(v) && v.k > 8;
        input(tr, 'k').classList.toggle('pgc-caution', wide);
        if (wide) {
          warned.push([v.poolName, 'k=' + v.k + '. With k above 8, writes and rebuilds are slow, and each step up in k ' +
            'saves less space than the last.', EC_DOCS, 'EC overhead table']);
        }
        var thin = ec ? v.m === 1 : v.size >= 1 && v.size <= 2;
        tr.classList.toggle('pgc-amber', thin);
        if (thin && ec) {
          warned.push([v.poolName, 'm=1. Expect data unavailability during maintenance and data loss when component ' +
            'failures overlap. Profiles with m=1 are strongly discouraged for data you care about.', EC_DOCS]);
        } else if (thin) {
          warned.push([v.poolName, v.size + (v.size === 1 ? ' copy' : ' copies') + '. With fewer than three copies, ' +
            'expect data unavailability during maintenance and data loss when component failures overlap.']);
        }
        var noRoom = unplaceable(v);
        tr.classList.toggle('pgc-red', noRoom);
        if (noRoom) {
          var n = shards(v), fd = failureDomain.value;
          failed.push([v.poolName, (ec ? 'k+m = ' + n + ' shards' : n + ' replicas') + ' on ' + v.osdNum +
            (v.osdNum === 1 ? ' OSD' : ' OSDs') + '. Each PG needs ' + n + ' OSDs, each in a different ' + fd +
            ', so this pool’s PGs can’t be placed. The generated commands leave it out.']);
        }
        tr.lastChild.textContent = isNaN(pgs) ? '—' : pgs;
        tr.lastChild.classList.toggle('pgc-na', isNaN(pgs));
        if (!isNaN(pgs)) totalPGs += pgs;
      });
      var off = Math.abs(totalPerc.toFixed(2) % 100) > 0;
      percCell.textContent = 'Total %Data: ' + totalPerc.toFixed(2) + '%';
      percCell.classList.toggle('pgc-warn', off);
      percNote.classList.toggle('pgc-warn', off);
      pgCell.textContent = 'PG total: ' + totalPGs;
      errors.textContent = '';
      failed.forEach(function (f) {
        var li = el('li');
        li.appendChild(el('b', {}, '✖ ' + f[0] + ':'));
        li.appendChild(document.createTextNode(' ' + f[1]));
        errors.appendChild(li);
      });
      errors.hidden = !failed.length;
      cautions.textContent = '';
      warned.forEach(function (w) {
        var li = el('li');
        li.appendChild(el('b', {}, '⚠ ' + w[0] + ':'));
        li.appendChild(document.createTextNode(' ' + w[1]));
        if (w[2]) {
          li.appendChild(document.createTextNode(' '));
          li.appendChild(el('a', { href: w[2], target: '_blank', rel: 'noopener' }, w[3] || 'Why'));
        }
        cautions.appendChild(li);
      });
      cautions.hidden = !warned.length;
      cmds.hidden = true;
    }

    function loadPreset() {
      tbody.textContent = '';
      PRESETS[useCase.value].forEach(addRow);
      update();
    }

    function askGlobal(g) {
      globalField = g;
      dialogLabel.textContent = g.name + ' for every pool' + (g.note ? ' (' + g.note + ')' : '');
      dialogLabel.appendChild(dialogInput);
      var first = tbody.querySelector('[data-key="' + g.key + '"]');
      dialogInput.value = first ? first.value : '';
      dialogInput.min = '1';
      dialogInput.step = g.key === 'targPGsPerOSD' ? 'any' : '1';
      dialog.returnValue = '';
      dialog.showModal();
    }

    dialog.addEventListener('close', function () {
      if (dialog.returnValue !== 'set' || dialogInput.value === '') return;
      tbody.querySelectorAll('[data-key="' + globalField.key + '"]').forEach(function (i) {
        i.value = dialogInput.value;
      });
      update();
    });

    addBtn.addEventListener('click', function () {
      var last = tbody.rows.length ? rowValues(tbody.rows[tbody.rows.length - 1]) : pool('', '', 0);
      addRow({ poolName: 'newPool', app: '', type: 'replicated', size: last.size || 3, k: last.k || 4, m: last.m || 2,
        osdNum: last.osdNum, percData: 0, targPGsPerOSD: last.targPGsPerOSD });
      update();
    });

    function commands() {
      var fd = failureDomain.value, crush = crushRoot.value.trim() || 'default';
      var suffix = crush === 'default' ? fd : crush + '-' + fd;
      var repRule = 'replicated_' + suffix.replace(/-/g, '_');
      var rules = [], profiles = {}, body = [], repRuleAdded = false, rbdEC = false, cephfs = false;
      Array.prototype.forEach.call(tbody.rows, function (tr) {
        var v = rowValues(tr), pgs = suggestedPGs(v);
        if (!v.poolName || isNaN(pgs) || unplaceable(v)) return;
        if (v.type === 'erasure') {
          var profile = 'ec-' + v.k + '-' + v.m + '-' + suffix;
          if (!profiles[profile]) {
            profiles[profile] = 1;
            rules.push('ceph osd erasure-code-profile set ' + profile + ' k=' + v.k + ' m=' + v.m +
              ' crush-failure-domain=' + fd + ' crush-root=' + crush);
            rules.push('ceph osd crush rule create-erasure ' + profile + ' ' + profile);
          }
          body.push('ceph osd pool create ' + v.poolName + ' ' + pgs + ' ' + pgs + ' erasure ' + profile + ' ' + profile);
          if (v.app === 'rbd' || v.app === 'cephfs') body.push('ceph osd pool set ' + v.poolName + ' allow_ec_overwrites true');
          if (v.app) body.push('ceph osd pool application enable ' + v.poolName + ' ' + v.app);
          if (v.app === 'rbd') rbdEC = true;
        } else {
          if (!repRuleAdded) {
            repRuleAdded = true;
            rules.unshift('ceph osd crush rule create-replicated ' + repRule + ' ' + crush + ' ' + fd);
          }
          body.push('ceph osd pool create ' + v.poolName + ' ' + pgs + ' ' + pgs + ' replicated ' + repRule);
          body.push('ceph osd pool set ' + v.poolName + ' size ' + v.size);
          if (v.app === 'rbd') body.push('rbd pool init ' + v.poolName);
          else if (v.app) body.push('ceph osd pool application enable ' + v.poolName + ' ' + v.app);
        }
        if (v.app === 'cephfs') cephfs = true;
        body.push('');
      });
      var lines = [
        '## Pools from the Ceph PG calculator.',
        '## The PG autoscaler is on for new pools by default and may change pg_num later.',
        '## To keep these pg_num values, also run for each pool:',
        '##   ceph osd pool set <pool> pg_autoscale_mode off',
        '',
        '## CRUSH rules and erasure-code profiles'
      ].concat(rules, ['', '## Pools'], body);
      if (rbdEC) {
        lines.push('## RBD keeps image metadata in a replicated pool. Put image data in an erasure-coded pool with:');
        lines.push('##   rbd create --size <size> --data-pool <erasure-coded pool> <replicated pool>/<image>');
      }
      if (cephfs) {
        lines.push('## Then create the file system: ceph fs new <fs_name> <metadata pool> <data pool>');
        lines.push('## Keep its default data pool replicated, and add erasure-coded data pools with:');
        lines.push('##   ceph fs add_data_pool <fs_name> <pool>');
      }
      return lines.join('\n').replace(/\n+$/, '\n');
    }

    genBtn.addEventListener('click', function () {
      var text = commands();
      pre.textContent = text;
      if (saveLink.href.indexOf('blob:') === 0) URL.revokeObjectURL(saveLink.href);
      saveLink.href = URL.createObjectURL(new Blob([text], { type: 'text/plain' }));
      cmds.hidden = false;
    });

    copyBtn.addEventListener('click', function () {
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(pre.textContent).then(function () {
          copyBtn.textContent = 'Copied';
          setTimeout(function () { copyBtn.textContent = 'Copy'; }, 1500);
        });
      } else {
        var range = document.createRange();
        range.selectNodeContents(pre);
        var sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
      }
    });

    useCase.addEventListener('change', loadPreset);
    failureDomain.addEventListener('change', update);
    crushRoot.addEventListener('input', function () { cmds.hidden = true; });
    loadPreset();
  }

  function start() {
    var root = document.getElementById('pgcalc-root');
    if (root) init(root);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
