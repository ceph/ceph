import { IscsiPageHelper } from './iscsi.po';

describe('Iscsi Page', () => {
  let iscsi: IscsiPageHelper;

  beforeAll(() => {
    iscsi = new IscsiPageHelper();
  });

  afterEach(async () => {
    await IscsiPageHelper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await iscsi.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await iscsi.waitTextToBePresent(iscsi.getBreadcrumb(), 'Overview');
    });
  });

  describe('fields check', () => {
    beforeAll(async () => {
      await iscsi.navigateTo();
    });

    it('should check that tables are displayed and legends are correct', async () => {
      // Check tables are displayed
      const dataTables = iscsi.getDataTables();
      await expect(dataTables.get(0).isDisplayed());
      await expect(dataTables.get(1).isDisplayed());

      // Check that legends are correct
      const legends = iscsi.getLegends();
      await expect(legends.get(0).getText()).toMatch('Gateways');
      await expect(legends.get(1).getText()).toMatch('Images');
    });
  });
});
