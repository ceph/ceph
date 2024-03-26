import { UsersPageHelper } from './users.po';

describe('RGW users page', () => {
  const users = new UsersPageHelper();
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

    it('should edit users full name, email and max buckets', () => {
      users.edit(user_name, 'Another Identity', 'changed@othersite.com', '1969');
    });

    it('should delete user', () => {
      users.delete(user_name);
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
});
