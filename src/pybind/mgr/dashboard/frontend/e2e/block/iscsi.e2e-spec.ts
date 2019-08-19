import { Helper } from '../helper.po';

describe('Iscsi Page', () => {
  let iscsi: Helper['iscsi'];

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

  describe('fields check', () => {
    beforeAll(() => {
      iscsi.navigateTo();
    });

    it('should check that tables are displayed and legends are correct', () => {
      // Check tables are displayed
      expect(
        iscsi
          .getTable()
          .get(0)
          .isDisplayed()
      );
      expect(
        iscsi
          .getTable()
          .get(1)
          .isDisplayed()
      );

      // Check that legends are correct
      expect(
        iscsi
          .getLegends()
          .get(0)
          .getText()
      ).toMatch('Gateways');
      expect(
        iscsi
          .getLegends()
          .get(1)
          .getText()
      ).toMatch('Images');
    });
  });
});
