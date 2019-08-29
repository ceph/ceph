import { RoleMgmtPageHelper } from './role-mgmt.po';

describe('Role Management page', () => {
  let roleMgmt: RoleMgmtPageHelper;
  const role_name = 'user_mgmt_create_edit_delete_role';

  beforeAll(() => {
    roleMgmt = new RoleMgmtPageHelper();
  });

  afterEach(async () => {
    await RoleMgmtPageHelper.checkConsole();
  });

  describe('breadcrumb tests', () => {
    it('should check breadcrumb on roles tab on user management page', async () => {
      await roleMgmt.navigateTo();
      await roleMgmt.waitTextToBePresent(roleMgmt.getBreadcrumb(), 'Roles');
    });

    it('should check breadcrumb on role creation page', async () => {
      await roleMgmt.navigateTo('create');
      await roleMgmt.waitTextToBePresent(roleMgmt.getBreadcrumb(), 'Create');
    });
  });

  describe('role create, edit & delete test', () => {
    it('should create a role', async () => {
      await roleMgmt.create(role_name, 'An interesting description');
    });

    it('should edit a role', async () => {
      await roleMgmt.edit(role_name, 'A far more interesting description');
    });

    it('should delete a role', async () => {
      await roleMgmt.navigateTo();
      await roleMgmt.delete(role_name);
    });
  });
});
