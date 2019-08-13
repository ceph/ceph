import { Helper } from '../helper.po';

describe('Monitors page', () => {
  let monitors: Helper['monitors'];

  beforeAll(() => {
    monitors = new Helper().monitors;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      monitors.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(monitors.getBreadcrumbText()).toEqual('Monitors');
    });
  });

  describe('fields check', () => {
    beforeAll(() => {
      monitors.navigateTo();
    });

    it('should check status table is present', () => {
      // check for table header 'Status'
      expect(
        monitors
          .getLegends()
          .get(0)
          .getText()
      ).toMatch('Status');

      // check for fields in table
      expect(monitors.getStatusTable().getText()).toMatch('Cluster ID');
      expect(monitors.getStatusTable().getText()).toMatch('monmap modified');
      expect(monitors.getStatusTable().getText()).toMatch('monmap epoch');
      expect(monitors.getStatusTable().getText()).toMatch('quorum con');
      expect(monitors.getStatusTable().getText()).toMatch('quorum mon');
      expect(monitors.getStatusTable().getText()).toMatch('required con');
      expect(monitors.getStatusTable().getText()).toMatch('required mon');
    });

    it('should check In Quorum and Not In Quorum tables are present', () => {
      // check for there to be two tables
      expect(monitors.getDataTable().count()).toEqual(2);

      // check for table header 'In Quorum'
      expect(
        monitors
          .getLegends()
          .get(1)
          .getText()
      ).toMatch('In Quorum');

      // check for table header 'Not In Quorum'
      expect(
        monitors
          .getLegends()
          .get(2)
          .getText()
      ).toMatch('Not In Quorum');

      // verify correct columns on In Quorum table
      expect(
        monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Name');
      expect(
        monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Rank');
      expect(
        monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Public Address');
      expect(
        monitors
          .getDataTableHeaders()
          .get(0)
          .getText()
      ).toMatch('Open Sessions');

      // verify correct columns on Not In Quorum table
      expect(
        monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Name');
      expect(
        monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Rank');
      expect(
        monitors
          .getDataTableHeaders()
          .get(1)
          .getText()
      ).toMatch('Public Address');
    });
  });
});
