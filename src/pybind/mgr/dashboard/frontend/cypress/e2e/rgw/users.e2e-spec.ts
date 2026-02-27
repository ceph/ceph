import { AccountsPageHelper } from './accounts.po';
import { UsersPageHelper } from './users.po';

describe('RGW users page', () => {
  const users = new UsersPageHelper();
  const accounts = new AccountsPageHelper();
  const tenant = 'e2e_000tenant';
  const user_id = 'e2e_000user_create_edit_delete';
  const user_name = tenant + '$' + user_id;

  beforeEach(() => {
    cy.login();
    users.navigateTo();
  });

  describe('breadcrumb tests', () => {
    it('should open and show breadcrumb', () => {
      users.expectBreadcrumbText('Users');
    });
  });

  describe('create, edit & delete user tests', () => {
    it('should create user', () => {
      users.navigateTo('create');
      users.create(tenant, user_id, 'Some Name', 'original@website.com', '1200');
      users.getFirstTableCell(user_id).should('exist');
    });

    it('should show user key details', () => {
      users.checkUserKeys(user_name);
    });

    it('should edit users full name, email and max buckets', () => {
      users.edit(user_name, 'Another Identity', 'changed@othersite.com', '1969');
    });

    it('should delete user', () => {
      users.delete(user_name, null, null, true, false, false, true);
    });
  });

  describe('Invalid input tests', () => {
    it('should put invalid input into user creation form and check fields are marked invalid', () => {
      users.invalidCreate();
    });

    it('should put invalid input into user edit form and check fields are marked invalid', () => {
      users.invalidEdit();
    });
  });

  describe('link user with account test', () => {
    let account_id: string;
    const user_id = 'account_user';
    const account = {
      name: 'test_account',
      email: 'test@test',
      tenant: 'tenanted_acc'
    };

    it('should create an account and store account_id', () => {
      accounts.navigateTo('create');
      accounts.create(account);
      accounts.navigateTo();
      accounts
        .getTableRow(account.name)
        .find('td')
        .eq(3)
        .invoke('text')
        .then((acc_id: string) => {
          cy.log(acc_id);
          account_id = acc_id;
        });
    });

    it('should link user with account', () => {
      users.navigateTo();
      users.navigateTo('create');
      users.linkAccount(account_id, account.name, user_id, account.tenant);
    });

    it('should make user as root account user', () => {
      users.navigateTo();
      users.makeRootAccount(account.name, user_id, account.tenant);
    });

    it('should not delete account if users are linked to it', () => {
      accounts.navigateTo();
      accounts.invalidDelete(account.name, 'delete');
    });

    it('should delete user and account', () => {
      users.navigateTo();
      users.delete(`${account.tenant}$${user_id}`, null, null, true, false, false, true);
      accounts.navigateTo();
      accounts.delete(account.name, null, null, true, false, false, false);
    });
  });
});
