import { UsersPageHelper } from './users.po';

describe('Cluster Users', () => {
  const users = new UsersPageHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    users.navigateTo();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', () => {
      users.expectBreadcrumbText('Users');
    });
  });

  describe('Cluster users table', () => {
    it('should verify the table is not empty', () => {
      users.checkForUsers();
    });

    it('should verify the keys are hidden', () => {
      users.verifyKeysAreHidden();
    });
  });
});
