import { Helper } from '../helper.po';

describe('Iscsi Page', () => {
  let iscsi;

  beforeAll(() => {
    iscsi = new Helper().iscsi;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      iscsi.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(iscsi.getBreadcrumbText()).toEqual('Overview');
    });
  });
});
