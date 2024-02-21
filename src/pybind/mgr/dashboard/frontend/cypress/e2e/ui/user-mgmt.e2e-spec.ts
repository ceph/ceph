import { UserMgmtPageHelper } from './user-mgmt.po';

describe('User Management page', () => {
  const userMgmt = new UserMgmtPageHelper();
  const user_name = 'e2e_user_mgmt_user';

  beforeEach(() => {
    cy.login();
    userMgmt.navigateTo();
  });

  describe('breadcrumb tests', () => {
    it('should check breadcrumb on users tab of user management page', () => {
      userMgmt.expectBreadcrumbText('Users');
    });

    it('should check breadcrumb on user creation page', () => {
      userMgmt.navigateTo('create');
      userMgmt.expectBreadcrumbText('Create');
    });
  });

  describe('user create, edit & delete test', () => {
    it('should create a user', () => {
      userMgmt.create(user_name, 'cool_password', 'Jeff', 'realemail@realwebsite.com');
    });

    it('should edit a user', () => {
      userMgmt.edit(user_name, 'cool_password_number_2', 'Geoff', 'w@m');
    });

    it('should delete a user', () => {
      userMgmt.delete(user_name);
    });
  });
});
