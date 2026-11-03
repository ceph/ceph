/* The code and selected ASCII keys are rendered from the same range choices. */
(function () {
  function initialize() {
    const demo = document.querySelector('.datum-in-practice .selection-demo');
    if (!demo) return;

    const buttons = demo.querySelectorAll('.step');
    const keys = demo.querySelectorAll('.key');
    const code = demo.querySelector('.selection-code code');
    const status = demo.querySelector('.selection-status');
    const prefix = demo.dataset.prefix;

    function select(button) {
      const statements = [
        'auto selected = datum::key::byte_query::prefix(' + JSON.stringify(prefix) + ');'
      ];
      const conditions = [function (key) { return key.startsWith(prefix); }];
      if (button.dataset.start !== undefined) {
        const start = button.dataset.start;
        statements.push('selected = datum::key::starting_after(selected, ' +
          JSON.stringify(start) + ');');
        conditions.push(function (key) { return key > start; });
      }
      if (button.dataset.end !== undefined) {
        const end = button.dataset.end;
        statements.push('selected = datum::key::ending_before(selected, ' +
          JSON.stringify(end) + ');');
        conditions.push(function (key) { return key < end; });
      }
      code.textContent = statements.concat([
        '',
        'const auto intervals = iv::compile_intervals<1>(selected);',
        'std::ranges::for_each(names | std::views::filter([&selected](const auto name) {',
        '    return iv::contains(selected, name);',
        '}), [](const auto name) { std::println("{}", name); });'
      ]).join('\n');

      const selected = [];
      buttons.forEach(function (other) {
        other.classList.toggle('active', other === button);
        other.setAttribute('aria-pressed', String(other === button));
      });
      keys.forEach(function (item) {
        const keep = conditions.every(function (condition) { return condition(item.dataset.key); });
        item.classList.toggle('kept', keep);
        item.classList.toggle('excluded', !keep);
        item.setAttribute('aria-label', item.dataset.key + (keep ? ': selected' : ': excluded'));
        if (keep) selected.push(item.dataset.key);
      });
      status.textContent = selected.length ?
        'Selected: ' + selected.join(', ') : 'Selected: (empty)';
    }

    select(buttons[0]);
    buttons.forEach(function (button) {
      button.disabled = false;
      button.addEventListener('click', function () { select(button); });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initialize, {once: true});
  } else {
    initialize();
  }
}());
