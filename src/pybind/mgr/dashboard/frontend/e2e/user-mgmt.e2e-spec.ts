import { Helper } from './helper.po';
import { UserMgmtPageHelper } from './user-mgmt.po';

describe('User Management page', () => {
  let userManagement: UserMgmtPageHelper;
  const user_name = 'user_mgmt_create_edit_delete_user';
  const role_name = 'user_mgmt_create_edit_delete_role';

  beforeAll(() => {
    userManagement = new UserMgmtPageHelper();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb tests', () => {
    it('should check breadcrumb on users tab of user management page', async () => {
      await userManagement.navigateTo('users');
      await userManagement.waitTextToBePresent(userManagement.getBreadcrumb(), 'Users');
    });

    it('should check breadcrumb on roles tab of user management page', async () => {
      await userManagement.navigateTo('roles');
      await userManagement.waitTextToBePresent(userManagement.getBreadcrumb(), 'Roles');
    });

    it('should check breadcrumb on user creation page', async () => {
      await userManagement.navigateTo('userCreate');
      await userManagement.waitTextToBePresent(userManagement.getBreadcrumb(), 'Create');
    });

    it('should check breadcrumb on role creation page', async () => {
      await userManagement.navigateTo('roleCreate');
      await userManagement.waitTextToBePresent(userManagement.getBreadcrumb(), 'Create');
    });
  });

  describe('user create, edit & delete test', () => {
    it('should create a user', async () => {
      await userManagement.userCreate(
        user_name,
        'cool_password',
        'Jeff',
        'realemail@realwebsite.com'
      );
    });

    it('should edit a user', async () => {
      await userManagement.userEdit(user_name, 'cool_password_number_2', 'Geoff', 'w@m');
    });

    it('should delete a user', async () => {
      await userManagement.userDelete(user_name);
    });
  });

  describe('role create, edit & delete test', () => {
    it('should create a role', async () => {
      await userManagement.roleCreate(role_name, 'An interesting description');
    });

    it('should edit a role', async () => {
      await userManagement.roleEdit(role_name, 'A far more interesting description');
    });

    it('should delete a role', async () => {
      await userManagement.roleDelete(role_name);
    });
  });
});
