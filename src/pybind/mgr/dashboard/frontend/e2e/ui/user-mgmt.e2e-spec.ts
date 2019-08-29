import { UserMgmtPageHelper } from './user-mgmt.po';

describe('User Management page', () => {
  let userMgmt: UserMgmtPageHelper;
  const user_name = 'user_mgmt_create_edit_delete_user';

  beforeAll(() => {
    userMgmt = new UserMgmtPageHelper();
  });

  afterEach(async () => {
    await UserMgmtPageHelper.checkConsole();
  });

  describe('breadcrumb tests', () => {
    it('should check breadcrumb on users tab of user management page', async () => {
      await userMgmt.navigateTo();
      await userMgmt.waitTextToBePresent(userMgmt.getBreadcrumb(), 'Users');
    });

    it('should check breadcrumb on user creation page', async () => {
      await userMgmt.navigateTo('create');
      await userMgmt.waitTextToBePresent(userMgmt.getBreadcrumb(), 'Create');
    });
  });

  describe('user create, edit & delete test', () => {
    it('should create a user', async () => {
      await userMgmt.create(user_name, 'cool_password', 'Jeff', 'realemail@realwebsite.com');
    });

    it('should edit a user', async () => {
      await userMgmt.edit(user_name, 'cool_password_number_2', 'Geoff', 'w@m');
    });

    it('should delete a user', async () => {
      await userMgmt.navigateTo();
      await userMgmt.delete(user_name);
    });
  });
});
