import { Helper } from './helper.po';

describe('User Management page', () => {
  let usermgmt: Helper['usermgmt'];
  const user_name = 'user_mgmt_create_edit_delete_user';
  const role_name = 'user_mgmt_create_edit_delete_role';

  beforeAll(() => {
    usermgmt = new Helper().usermgmt;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb tests', () => {
    it('should check breadcrumb on users tab of user management page', () => {
      usermgmt.navigateTo('users');
      expect(usermgmt.getBreadcrumbText()).toEqual('Users');
    });

    it('should check breadcrumb on roles tab of user management page', () => {
      usermgmt.navigateTo('roles');
      expect(usermgmt.getBreadcrumbText()).toEqual('Roles');
    });

    it('should check breadcrumb on user creation page', () => {
      usermgmt.navigateTo('userCreate');
      expect(usermgmt.getBreadcrumbText()).toEqual('Create');
    });

    it('should check breadcrumb on role creation page', () => {
      usermgmt.navigateTo('roleCreate');
      expect(usermgmt.getBreadcrumbText()).toEqual('Create');
    });
  });

  describe('user create, edit & delete test', () => {
    it('should create a user', () => {
      usermgmt.userCreate(user_name, 'cool_password', 'Jeff', 'realemail@realwebsite.com');
    });

    it('should edit a user', () => {
      usermgmt.userEdit(user_name, 'cool_password_number_2', 'Geoff', 'w@m');
    });

    it('should delete a user', () => {
      usermgmt.userDelete(user_name);
    });
  });

  describe('role create, edit & delete test', () => {
    it('should create a role', () => {
      usermgmt.roleCreate(role_name, 'An interesting description');
    });

    it('should edit a role', () => {
      usermgmt.roleEdit(role_name, 'A far more interesting description');
    });

    it('should delete a role', () => {
      usermgmt.roleDelete(role_name);
    });
  });
});
