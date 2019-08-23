import { Helper } from '../helper.po';
import { MonitorsPageHelper } from './monitors.po';

describe('Monitors page', () => {
  let monitors: MonitorsPageHelper;

  beforeAll(() => {
    monitors = new MonitorsPageHelper();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await monitors.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await monitors.getBreadcrumbText()).toEqual('Monitors');
    });
  });

  describe('fields check', () => {
    beforeAll(async () => {
      await monitors.navigateTo();
    });

    it('should check status table is present', async () => {
      // check for table header 'Status'
      expect(
        await monitors
          .getLegends()
          .get(0)
          .getText()
      ).toMatch('Status');

      // check for fields in table
      expect(await monitors.getStatusTables().getText()).toMatch('Cluster ID');
      expect(await monitors.getStatusTables().getText()).toMatch('monmap modified');
      expect(await monitors.getStatusTables().getText()).toMatch('monmap epoch');
      expect(await monitors.getStatusTables().getText()).toMatch('quorum con');
      expect(await monitors.getStatusTables().getText()).toMatch('quorum mon');
      expect(await monitors.getStatusTables().getText()).toMatch('required con');
      expect(await monitors.getStatusTables().getText()).toMatch('required mon');
    });

    it('should check In Quorum and Not In Quorum tables are present', async () => {
      // check for there to be two tables
      expect(await monitors.getDataTables().count()).toEqual(2);

      // check for table header 'In Quorum'
      expect(
        await monitors
          .getLegends()
          .get(1)
          .getText()
      ).toMatch('In Quorum');

      // check for table header 'Not In Quorum'
      expect(
        await monitors
          .getLegends()
          .get(2)
          .getText()
      ).toMatch('Not In Quorum');

      // verify correct columns on In Quorum table
      expect(
        await monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Name');
      expect(
        await monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Rank');
      expect(
        await monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Public Address');
      expect(
        await monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Open Sessions');

      // verify correct columns on Not In Quorum table
      expect(
        await monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Name');
      expect(
        await monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Rank');
      expect(
        await monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Public Address');
    });
  });
});
