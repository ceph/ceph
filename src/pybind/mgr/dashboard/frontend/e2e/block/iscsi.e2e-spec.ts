import { Helper } from '../helper.po';
import { IscsiPageHelper } from './iscsi.po';

describe('Iscsi Page', () => {
  let iscsi: IscsiPageHelper;

  beforeAll(() => {
    iscsi = new IscsiPageHelper();
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
      const dataTables = iscsi.getDataTables();
      expect(await dataTables.get(0).isDisplayed());
      expect(await dataTables.get(1).isDisplayed());

      // Check that legends are correct
      const legends = iscsi.getLegends();
      expect(await legends.get(0).getText()).toMatch('Gateways');
      expect(await legends.get(1).getText()).toMatch('Images');
    });
  });
});
