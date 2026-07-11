import { RolesPageHelper } from './roles.po';
import { AccountsPageHelper } from './accounts.po';

describe('RGW roles page', () => {
  const roles = new RolesPageHelper();
  const accounts = new AccountsPageHelper();
  const accountName = 'roles-test-account';
  const roleName = 'testRole';

  before(() => {
    cy.login();
    accounts.navigateTo('create');
    accounts.create({ name: accountName, email: 'test@example.com' });
  });

  after(() => {
    cy.login();
    accounts.navigateTo();
    accounts.delete(accountName, null, null, true, false, false, false);
  });

  beforeEach(() => {
    cy.login();
    accounts.navigateTo();
    accounts.getExpandCollapseElement(accountName).click();
    cy.contains('cds-tab-headers button[role="tab"]', 'Roles').click();
    // Wait for the roles list to render
    cy.get('cd-rgw-account-roles-list').should('exist');
  });

  describe('Create, Edit & Delete rgw roles', () => {
    it('should create rgw role', () => {
      roles.create(roleName, '/', '{}');
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
