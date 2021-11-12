import '@applitools/eyes-cypress/commands';

import './commands';

afterEach(() => {
  cy.visit('#/403');
});
