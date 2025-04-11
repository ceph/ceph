import { AccountsPageHelper } from './accounts.po';

describe('RGW Accounts page', () => {
  const accounts = new AccountsPageHelper();
  const account_name = 'e2eaccount';

  beforeEach(() => {
    cy.login();
    accounts.navigateTo();
  });

  describe('breadcrumb tests', () => {
    it('should open and show breadcrumb', () => {
      accounts.expectBreadcrumbText('Accounts');
    });
  });

  describe('create, edit & delete account tests', () => {
    it('should create account with all details', () => {
      const account = {
        name: account_name,
        tenant: 'test',
        email: 'account@test.com'
      };
      accounts.navigateTo('create');
      accounts.create(account);
    });

    it('should edit account', () => {
      const account = {
        name: account_name,
        tenant: 'test',
        email: 'account@test.com',
        max_buckets: '1000',
        max_users: '0',
        max_roles: '-1',
        max_groups: '1000',
        max_access_keys: '2'
      };
      accounts.edit(account);
    });

    it('should delete account', () => {
      accounts.delete(account_name, null, null, true, false, false, false);
    });
  });

  describe('create, edit & delete account tests', () => {
    it('should put invalid input into account creation form and check fields are marked invalid', () => {
      accounts.navigateTo('create');
      accounts.invalidCreate();
    });

    it('should put invalid input into account edit form and check fields are marked invalid', () => {
      accounts.navigateTo('create');
      accounts.invalidEdit();
    });
  });
});
