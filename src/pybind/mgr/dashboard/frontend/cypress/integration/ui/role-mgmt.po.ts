import { PageHelper } from '../page-helper.po';

export class RoleMgmtPageHelper extends PageHelper {
  pages = {
    index: { url: '#/user-management/roles', id: 'cd-role-list' },
    create: { url: '#/user-management/roles/create', id: 'cd-role-form' }
  };

  create(name: string, description: string) {
    this.navigateTo('create');
    // Waits for data to load
    cy.contains('grafana');

    // fill in fields
    cy.get('#name').type(name);
    cy.get('#description').type(description);

    // Click the create button and wait for role to be made
    cy.contains('button', 'Create Role').click();
    cy.get('.breadcrumb-item.active').should('not.have.text', 'Create');

    this.getFirstTableCell(name).should('exist');
  }

  edit(name: string, description: string) {
    this.navigateEdit(name);
    // Waits for data to load
    cy.contains('grafana');

    // fill in fields with new values
    cy.get('#description').clear().type(description);

    // Click the edit button and check new values are present in table
    cy.contains('button', 'Edit Role').click();
    cy.get('.breadcrumb-item.active').should('not.have.text', 'Edit');

    this.getFirstTableCell(name).should('exist');
    this.getFirstTableCell(description).should('exist');
  }
}
