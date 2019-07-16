import { Helper } from '../helper.po';

describe('Manager modules page', () => {
  let mgrmodules;

  beforeAll(() => {
    mgrmodules = new Helper().mgrmodules;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      mgrmodules.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(mgrmodules.getBreadcrumbText()).toEqual('Manager modules');
    });
  });
});
