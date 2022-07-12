import { PageHelper } from '../page-helper.po';

export class UserMgmtPageHelper extends PageHelper {
  pages = {
    index: { url: '#/user-management/users', id: 'cd-user-list' },
    create: { url: '#/user-management/users/create', id: 'cd-user-form' }
  };

  create(username: string, password: string, name: string, email: string) {
    this.navigateTo('create');

    // fill in fields
    cy.get('#username').type(username);
    cy.get('#password').type(password);
    cy.get('#confirmpassword').type(password);
    cy.get('#name').type(name);
    cy.get('#email').type(email);

    // Click the create button and wait for user to be made
    cy.get('[data-cy=submitBtn]').click();
    this.getFirstTableCell(username).should('exist');
  }

  edit(username: string, password: string, name: string, email: string) {
    this.navigateEdit(username);

    // fill in fields with new values
    cy.get('#password').clear().type(password);
    cy.get('#confirmpassword').clear().type(password);
    cy.get('#name').clear().type(name);
    cy.get('#email').clear().type(email);

    // Click the edit button and check new values are present in table
    const editButton = cy.get('[data-cy=submitBtn]');
    editButton.click();
    this.getFirstTableCell(email).should('exist');
    this.getFirstTableCell(name).should('exist');
  }
}
