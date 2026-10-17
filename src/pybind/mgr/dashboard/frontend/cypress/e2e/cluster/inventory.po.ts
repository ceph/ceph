import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/inventory', id: 'cd-inventory' }
};

export class InventoryPageHelper extends PageHelper {
  pages = pages;

  identify() {
    this.getFirstTableCell().click();
    cy.wait(1000);
    cy.contains('[data-testid="primary-action"]', 'Identify', { timeout: 15000 })
      .should('not.be.disabled')
      .click();
    cy.get('cds-modal').within(() => {
      cy.get('#duration').select('15 minutes');
      cy.get('#duration').select('10 minutes');
      cy.get('cd-back-button').click();
    });
    cy.get('cds-modal').should('not.exist');
    cy.get(`${this.pages.index.id}`);
  }
}
