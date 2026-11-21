import { RoleMgmtPageHelper } from '../ui/role-mgmt.po';
import { UserMgmtPageHelper } from '../ui/user-mgmt.po';

describe('User management accessibility', { retries: 0 }, () => {
  const users = new UserMgmtPageHelper();
  const roles = new RoleMgmtPageHelper();

  beforeEach(() => {
    cy.login();
  });

  it('users page should have no accessibility violations', () => {
    users.navigateTo();
    cy.get('cd-user-list cd-table').should('exist');
    cy.contains('Loading').should('not.exist');
    cy.injectAxe();
    cy.checkAccessibility('cd-user-list');
  });

  it('roles page should have no accessibility violations', () => {
    roles.navigateTo();
    cy.get('cd-role-list cd-table').should('exist');
    cy.contains('Loading').should('not.exist');
    cy.injectAxe();
    cy.checkAccessibility('cd-role-list');
  });
});
