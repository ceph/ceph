import { LogsPageHelper } from '../cluster/logs.po';

describe('Copy to clipboard button accessibility', { retries: 0 }, () => {
  const logs = new LogsPageHelper();

  beforeEach(() => {
    cy.login();
    logs.navigateTo();
  });

  it('icon-only copy to clipboard button should have an accessible name', () => {
    // The logs page renders the shared clipboard button without visible text.
    cy.get('.log-actions cd-copy-2-clipboard-button button')
      .should('exist')
      .and('have.attr', 'aria-label', 'Copy to Clipboard');
    cy.injectAxe();
    cy.checkAccessibility('.log-actions');
  });
});
