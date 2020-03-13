import { PageHelper } from '../page-helper.po';

export class RoleMgmtPageHelper extends PageHelper {
  pages = {
    index: { url: '#/user-management/roles', id: 'cd-role-list' },
    create: { url: '#/user-management/roles/create', id: 'cd-role-form' }
  };

  create(name: string, description: string) {
    this.navigateTo('create');

    // fill in fields
    cy.get('#name').type(name);
    cy.get('#description').type(description);

    // Click the create button and wait for role to be made
    cy.contains('button', 'Create Role').click();

    this.getFirstTableCell(name).should('exist');
  }

  edit(name: string, description: string) {
    this.getFirstTableCell(name).click(); // select role from table
    cy.contains('button', 'Edit').click(); // click button to move to edit page

    // fill in fields with new values
    cy.get('#description').clear().type(description);

    // Click the edit button and check new values are present in table
    cy.contains('button', 'Edit Role').click();

    this.getFirstTableCell(name).should('exist');
    this.getFirstTableCell(description).should('exist');
  }
}
