import { RolesPageHelper } from './roles.po';
import { AccountsPageHelper } from './accounts.po';

describe('RGW roles page', () => {
  const roles = new RolesPageHelper();
  const accounts = new AccountsPageHelper();
  const accountName = 'roles-test-account';
  const roleName = 'testRole';
  // Minimal valid IAM trust policy (empty "{}" is rejected by RGW role create).
  const trustPolicy =
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam:::user/testuser"},"Action":["sts:AssumeRole"]}]}';

  before(() => {
    cy.login();
    accounts.navigateTo('create');
    accounts.create({ name: accountName, email: 'test@example.com' });
  });

  after(() => {
    cy.login();
    accounts.navigateTo();
    cy.get('cd-table').should('exist');
    accounts.delete(accountName, null, null, true, false, false, false);
  });

  beforeEach(() => {
    cy.login();
    accounts.navigateTo();
    accounts.getResourcePage(accountName).click();
    cy.contains('cds-sidenav-item a', /^Roles$/).click();
    cy.location('hash').should('include', '/roles');
    // Wait for the roles list to render
    cy.get('cd-rgw-account-roles-list').should('exist');
  });

  describe('Create, Edit & Delete rgw roles', () => {
    it('should create rgw role', () => {
      roles.create(roleName, '/', trustPolicy);
      roles.checkExist(roleName, true);
    });

    it('should edit rgw role', () => {
      roles.edit(roleName, 3);
    });

    it('should delete rgw role', () => {
      roles.deleteRole(roleName);
      roles.checkExist(roleName, false);
    });
  });
});
