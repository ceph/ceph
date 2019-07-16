import { Helper } from '../helper.po';

describe('RGW users page', () => {
  let users;

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
});
