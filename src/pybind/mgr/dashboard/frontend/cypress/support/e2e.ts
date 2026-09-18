import 'cypress-axe';

import './commands';

afterEach(() => {
  // Prefer a real page over #/403 so the next scenario can mount the SPA.
  cy.visit('#/dashboard', { failOnStatusCode: false });
});

Cypress.on('uncaught:exception', (err: Error) => {
  const ignoredErrors = [
    'ResizeObserver loop limit exceeded',
    'api/prometheus/rules',
    'NG0100: ExpressionChangedAfterItHasBeenCheckedError',
    'NgClass can only toggle CSS classes'
  ];
  if (ignoredErrors.some((error) => err.message.includes(error))) {
    return false;
  }
  // Host label edit can race with orch inventory refresh after host add.
  if (err.message.includes('Http failure response') && err.message.includes('/api/host/')) {
    return false;
  }
  return true;
});

Cypress.on('fail', (err: Error) => {
  if (err.message.includes('xhr') && err.message.includes('canceled')) {
    return false; // Ignore canceled XHR requests
  }
  throw err;
});
