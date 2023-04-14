import '@applitools/eyes-cypress/commands';
import 'cypress-axe';

import './commands';

afterEach(() => {
  cy.visit('#/403');
});

Cypress.on('uncaught:exception', (err: Error) => {
  if (
    err.message.includes('ResizeObserver loop limit exceeded') ||
    err.message.includes('api/prometheus/rules') ||
    err.message.includes('NG0100: ExpressionChangedAfterItHasBeenCheckedError')
  ) {
    return false;
  }
  return true;
});
