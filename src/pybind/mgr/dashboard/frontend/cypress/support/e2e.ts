import '@applitools/eyes-cypress/commands';
import 'cypress-axe';

import './commands';

afterEach(() => {
  cy.visit('#/403');
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
  return true;
});

Cypress.on('fail', (err: Error) => {
  if (err.message.includes('xhr') && err.message.includes('canceled')) {
    return false; // Ignore canceled XHR requests
  }
  throw err;
});
