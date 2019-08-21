import { Helper } from '../helper.po';
import { IscsiPageHelper } from './iscsi.po';

describe('Iscsi Page', () => {
  let iscsi: IscsiPageHelper;

  beforeAll(() => {
    iscsi = new Helper().iscsi;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await iscsi.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await iscsi.getBreadcrumbText()).toEqual('Overview');
    });
  });

  describe('fields check', () => {
    beforeAll(async () => {
      await iscsi.navigateTo();
    });

    it('should check that tables are displayed and legends are correct', async () => {
      // Check tables are displayed
      expect(
        await iscsi
          .getDataTable()
          .get(0)
          .isDisplayed()
      );
      expect(
        await iscsi
          .getDataTable()
          .get(1)
          .isDisplayed()
      );

      // Check that legends are correct
      expect(
        await iscsi
          .getLegends()
          .get(0)
          .getText()
      ).toMatch('Gateways');
      expect(
        await iscsi
          .getLegends()
          .get(1)
          .getText()
      ).toMatch('Images');
    });
  });
});
