import { MonitorsPageHelper } from './monitors.po';

describe('Monitors page', () => {
  let monitors: MonitorsPageHelper;

  beforeAll(() => {
    monitors = new MonitorsPageHelper();
  });

  afterEach(async () => {
    await MonitorsPageHelper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await monitors.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await monitors.waitTextToBePresent(monitors.getBreadcrumb(), 'Monitors');
    });
  });

  describe('fields check', () => {
    beforeAll(async () => {
      await monitors.navigateTo();
    });

    it('should check status table is present', async () => {
      // check for table header 'Status'
      await expect(
        monitors
          .getLegends()
          .get(0)
          .getText()
      ).toMatch('Status');

      // check for fields in table
      await expect(monitors.getStatusTables().getText()).toMatch('Cluster ID');
      await expect(monitors.getStatusTables().getText()).toMatch('monmap modified');
      await expect(monitors.getStatusTables().getText()).toMatch('monmap epoch');
      await expect(monitors.getStatusTables().getText()).toMatch('quorum con');
      await expect(monitors.getStatusTables().getText()).toMatch('quorum mon');
      await expect(monitors.getStatusTables().getText()).toMatch('required con');
      await expect(monitors.getStatusTables().getText()).toMatch('required mon');
    });

    it('should check In Quorum and Not In Quorum tables are present', async () => {
      // check for there to be two tables
      await expect(monitors.getDataTables().count()).toEqual(2);

      // check for table header 'In Quorum'
      await expect(
        monitors
          .getLegends()
          .get(1)
          .getText()
      ).toMatch('In Quorum');

      // check for table header 'Not In Quorum'
      await expect(
        monitors
          .getLegends()
          .get(2)
          .getText()
      ).toMatch('Not In Quorum');

      // verify correct columns on In Quorum table
      await expect(
        monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Name');
      await expect(
        monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Rank');
      await expect(
        monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Public Address');
      await expect(
        monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Open Sessions');

      // verify correct columns on Not In Quorum table
      await expect(
        monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Name');
      await expect(
        monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Rank');
      await expect(
        monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Public Address');
    });
  });
});
