import { UsersPageHelper } from './users.po';

describe('RGW users page', () => {
  const users = new UsersPageHelper();
  const user_name = 'e2e_000user_create_edit_delete';

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
      users.create(user_name, 'Some Name', 'original@website.com', '1200');
      users.getFirstTableCell(user_name).should('exist');
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
