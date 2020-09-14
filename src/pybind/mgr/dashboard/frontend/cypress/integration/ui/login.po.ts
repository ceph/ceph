import { PageHelper } from '../page-helper.po';

export class LoginPageHelper extends PageHelper {
  pages = {
    index: { url: '#/login', id: 'cd-login' },
    dashboard: { url: '#/dashboard', id: 'cd-dashboard' }
  };

  doLogin() {
    cy.get('[name=username]').type('admin');
    cy.get('#password').type('admin');
    cy.contains('input', 'Login').click();
    cy.get('cd-dashboard').should('exist');
  }

  doLogout() {
    cy.get('cd-identity a').click();
    cy.contains('cd-identity span', 'Sign out').click();
    cy.get('cd-login').should('exist');
    cy.location('hash').should('be', '#login');
  }
}
