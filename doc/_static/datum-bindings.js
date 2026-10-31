/* The illustration changes only the displayed logical-key selection. */
(function () {
  function initialize() {
    const page = document.querySelector('.datum-in-practice');
    if (!page) return;

    const buttons = page.querySelectorAll('.step');
    const keys = page.querySelectorAll('.key');
    const status = page.querySelector('.selection-status');
    buttons.forEach(function (button) {
      button.addEventListener('click', function () {
        const stage = Number(button.dataset.stage);
        const selected = [];
        buttons.forEach(function (other) {
          other.classList.toggle('active', other === button);
          other.setAttribute('aria-pressed', String(other === button));
        });
        keys.forEach(function (item) {
          const key = item.dataset.key;
          const keep = key.startsWith('photos/') &&
            (stage < 1 || key > 'photos/20') &&
            (stage < 2 || key < 'photos/80');
          item.classList.toggle('kept', keep);
          item.classList.toggle('excluded', !keep);
          if (keep) selected.push(key);
        });
        status.textContent = 'Selected: ' + selected.join(', ');
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initialize, {once: true});
  } else {
    initialize();
  }
}());
