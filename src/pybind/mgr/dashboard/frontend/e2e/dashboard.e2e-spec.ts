import { $$, browser, by, element } from 'protractor';
import { Helper } from './helper.po';

describe('Dashboard Main Page', () => {
  let dashboard: Helper['dashboard'];
  let daemons: Helper['daemons'];
  let hosts: Helper['hosts'];
  let osds: Helper['osds'];
  let pools: Helper['pools'];
  let monitors: Helper['monitors'];
  let iscsi: Helper['iscsi'];

  beforeAll(() => {
    dashboard = new Helper().dashboard;
    daemons = new Helper().daemons;
    hosts = new Helper().hosts;
    osds = new Helper().osds;
    pools = new Helper().pools;
    monitors = new Helper().monitors;
    iscsi = new Helper().iscsi;
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('Check that all hyperlinks on cells lead to the correct page and fields exist', () => {
    beforeEach(async () => {
      await dashboard.navigateTo();
    });

    it('should check all linked cells lead to correct page', async () => {
      // Grabs cell and then clicks the hyperlink, some cells require different
      // methods as stated below

      // Monitors Cell
      expect(await browser.getCurrentUrl()).toContain('/#/dashboard');
      await dashboard.cellLink('Monitors');
      expect(await dashboard.getBreadcrumbText()).toEqual('Monitors');

      // OSDs Cell
      // await browser.navigate().back();
      await dashboard.navigateBack();
      expect(await browser.getCurrentUrl()).toContain('/#/dashboard');
      await dashboard.cellLink('OSDs');
      expect(await dashboard.getBreadcrumbText()).toEqual('OSDs');

      // Hosts Cell
      // await browser.navigate().back();
      await dashboard.navigateBack();
      expect(await browser.getCurrentUrl()).toContain('/#/dashboard');
      await dashboard.cellLink('Hosts');
      expect(await dashboard.getBreadcrumbText()).toEqual('Hosts');

      // Object Gateways Cell
      // await browser.navigate().back();
      await dashboard.navigateBack();
      expect(await browser.getCurrentUrl()).toContain('/#/dashboard');
      await element
        .all(by.partialLinkText('Object'))
        .last()
        .click(); // Since there is a space and there are 2 occurances of
      expect(await dashboard.getBreadcrumbText()).toEqual('Daemons'); // 'Object Gateways', this method was used to grab the link

      // iSCSI Gateways Cell
      // await browser.navigate().back();
      await dashboard.navigateBack();
      expect(await browser.getCurrentUrl()).toContain('/#/dashboard');
      await dashboard.partialCellLink('iSCSI'); // Since there is a space between iSCSI and Gateways this method was
      expect(await dashboard.getBreadcrumbText()).toEqual('Overview'); // used to grab and click the link

      // Pools Cell
      // await browser.navigate().back();
      await dashboard.navigateBack();
      expect(await browser.getCurrentUrl()).toContain('/#/dashboard');
      await dashboard.cellLink('Pools');
      expect(await dashboard.getBreadcrumbText()).toEqual('Pools');
    });

    it('should verify that cells exist on dashboard in proper order', async () => {
      // Ensures that info cards are all displayed on the dashboard tab while being
      // in the proper order, checks for card title and position via indexing into
      // a list of all info cards
      await dashboard.navigateTo();
      expect(await browser.getCurrentUrl()).toContain('/#/dashboard');
      expect(await dashboard.infoCardText(0)).toContain('Cluster Status');
      expect(await dashboard.infoCardText(1)).toContain('Monitors');
      expect(await dashboard.infoCardText(2)).toContain('OSDs');
      expect(await dashboard.infoCardText(3)).toContain('Manager Daemons');
      expect(await dashboard.infoCardText(4)).toContain('Hosts');
      expect(await dashboard.infoCardText(5)).toContain('Object Gateways');
      expect(await dashboard.infoCardText(6)).toContain('Metadata Servers');
      expect(await dashboard.infoCardText(7)).toContain('iSCSI Gateways');
      expect(await dashboard.infoCardText(8)).toContain('Client IOPS');
      expect(await dashboard.infoCardText(9)).toContain('Client Throughput');
      expect(await dashboard.infoCardText(10)).toContain('Client Read/Write');
      expect(await dashboard.infoCardText(11)).toContain('Recovery Throughput');
      expect(await dashboard.infoCardText(12)).toContain('Scrub');
      expect(await dashboard.infoCardText(13)).toContain('Pools');
      expect(await dashboard.infoCardText(14)).toContain('Raw Capacity');
      expect(await dashboard.infoCardText(15)).toContain('Objects');
      expect(await dashboard.infoCardText(16)).toContain('PGs per OSD');
      expect(await dashboard.infoCardText(17)).toContain('PG Status');
    });

    it('should verify that info card group titles are present and in the right order', async () => {
      // Checks that the group titles on the dashboard are correct and in the right order
      await dashboard.navigateTo();
      expect(await browser.getCurrentUrl()).toContain('/#/dashboard');
      expect(await dashboard.checkGroupTitles(0, 'Status'));
      expect(await dashboard.checkGroupTitles(1, 'Performance'));
      expect(await dashboard.checkGroupTitles(2, 'Capacity'));
    });
  });

  describe('Should check that dashboard cells have correct information', () => {
    beforeAll(async () => {
      await dashboard.navigateTo();
    });

    it('should verify that proper number of object gateway daemons on dashboard', async () => {
      // Checks that dashboard id card for Object Gateway has the correct number of Daemons
      // by checking the Daemons page and taking the count found at the bottom of the table
      await dashboard.navigateTo();
      const dashCount = await dashboard.cardNumb(5); // Grabs number of daemons from dashboard Object Gateway card
      await daemons.navigateTo();
      // Grabs number of daemons from table footer
      const tableCount = (await daemons.getTableCount().getText()).slice(13);
      expect(dashCount).toContain(tableCount);
    });

    it('should verify that proper number of monitors on dashboard', async () => {
      // Checks that dashboard id card for Monitors has the correct number of Monitors
      // by checking the Monitors page and taking the count found at the bottom of the table
      await dashboard.navigateTo();
      // Grabs number of monitors from dashboard Monitor card
      const dashCount = await dashboard.cardNumb(1);
      await monitors.navigateTo();
      // Grabs number of monitors from table footer
      const tableCount = (await $$('.datatable-footer-inner')
        .first()
        .getText()).slice(0, -6);
      expect(dashCount).toContain(tableCount);
    });

    it('should verify that proper number of hosts on dashboard', async () => {
      // Checks that dashboard id card for Hosts has the correct number of hosts
      // by checking the Hosts page and taking the count found at the bottom of the table
      await dashboard.navigateTo();
      // Grabs number of hosts from dashboard Hosts card
      const dashCount = await dashboard.cardNumb(4);
      await hosts.navigateTo();
      // Grabs number of hosts from table footer
      const tableCount = (await hosts.getTableCount().getText()).slice(13, -6);
      expect(dashCount).toContain(tableCount);
    });

    it('should verify that proper number of osds on dashboard', async () => {
      // Checks that dashboard id card for Hosts has the correct number of hosts
      // by checking the Hosts page and taking the count found at the bottom of the table
      await dashboard.navigateTo();
      // Grabs number of hosts from dashboard Hosts card
      const dashCount = (await dashboard.cardNumb(2)).slice(0, -17);
      await osds.navigateTo();
      // Grabs number of hosts from table footer
      const tableCount = (await osds.getTableCount().getText()).slice(13, -6);
      expect(dashCount).toContain(tableCount);
    });

    it('should verify that proper number of pools on dashboard', async () => {
      await dashboard.navigateTo();
      // Grabs number of hosts from dashboard Pools card
      const dashCount = (await dashboard.cardNumb(13)).slice(4);
      await pools.navigateTo();
      // Grabs number of pools from table footer
      const tableCount = (await pools.getTableCount().getText()).slice(13, -6);
      expect(dashCount).toContain(tableCount);
    });

    it('should verify that proper number of iscsi gateways on dashboard', async () => {
      // Checks that dashboard id card for iSCSI has the correct number of gateways
      // by checking the iSCSI page and taking the count found at the bottom of the table (first)
      await dashboard.navigateTo();
      // Grabs number of gateways from dashboard iSCSI card
      const dashCount = await dashboard.cardNumb(7);
      await iscsi.navigateTo();
      // Grabs number of monitors from table footer
      const tableCount = (await $$('.datatable-footer-inner')
        .first()
        .getText()).slice(0, -6);
      expect(dashCount).toContain(tableCount);
    });
  });
});
