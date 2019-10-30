import { UsersPageHelper } from './users.po';

describe('RGW users page', () => {
  let users: UsersPageHelper;
  const user_name = '000user_create_edit_delete';

  beforeAll(() => {
    users = new UsersPageHelper();
  });

  afterEach(async () => {
    await UsersPageHelper.checkConsole();
  });

  describe('breadcrumb tests', () => {
    beforeEach(async () => {
      await users.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await users.waitTextToBePresent(users.getBreadcrumb(), 'Users');
    });
  });

  describe('create, edit & delete user tests', () => {
    beforeEach(async () => {
      await users.navigateTo();
      await users.uncheckAllTableRows();
    });

    it('should create user', async () => {
      await users.navigateTo('create');
      await users.create(user_name, 'Some Name', 'original@website.com', '1200');
      await expect(users.getFirstTableCellWithText(user_name).isPresent()).toBe(true);
    });

    it('should edit users full name, email and max buckets', async () => {
      await users.edit(user_name, 'Another Identity', 'changed@othersite.com', '1969');
    });

    it('should delete user', async () => {
      await users.delete(user_name);
    });
  });

  describe('Invalid input tests', () => {
    it('should put invalid input into user creation form and check fields are marked invalid', async () => {
      await users.invalidCreate();
    });

    it('should put invalid input into user edit form and check fields are marked invalid', async () => {
      await users.invalidEdit();
    });
  });
});
