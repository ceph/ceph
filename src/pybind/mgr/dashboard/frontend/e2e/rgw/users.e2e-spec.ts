import { Helper } from '../helper.po';

describe('RGW users page', () => {
  let users;
  const user_name = '000user_create_edit_delete';

  beforeAll(() => {
    users = new Helper().users;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      users.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(users.getBreadcrumbText()).toEqual('Users');
    });
  });

  describe('create, edit & delete user test', () => {
    beforeAll(() => {
      users.navigateTo();
    });

    it('should create user', () => {
      users.create(user_name, 'Some Name', 'original@website.com', '1200');
      expect(users.getTableCell(user_name).isPresent()).toBe(true);
    });

    it('should edit users full name, email and max buckets', () => {
      users.edit(user_name, 'Another Identity', 'changed@othersite.com', '1969');
      // checks for succsessful editing are done within edit function
    });

    it('should delete user', () => {
      users.delete(user_name);
      expect(users.getTableCell(user_name).isPresent()).toBe(false);
    });
  });

  describe('Invalid input test', () => {
    beforeAll(() => {
      users.navigateTo();
    });

    it('should put invalid input into user creation form and check fields are marked invalid', () => {
      users.invalidCreate();
    });

    it('should put invalid input into user edit form and check fields are marked invalid', () => {
      users.invalidEdit();
    });
  });
});
