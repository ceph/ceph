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

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('Check that all hyperlinks on cells lead to the correct page and fields exist', () => {
    beforeAll(() => {
      dashboard.navigateTo();
    });

    it('should check all linked cells lead to correct page', () => {
      // Grabs cell and then clicks the hyperlink, some cells require different
      // methods as stated below

      // Monitors Cell
      expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      dashboard.cellLink('Monitors');
      expect(dashboard.getBreadcrumbText()).toEqual('Monitors');

      // OSDs Cell
      browser.navigate().back();
      expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      dashboard.cellLink('OSDs');
      expect(dashboard.getBreadcrumbText()).toEqual('OSDs');

      // Hosts Cell
      browser.navigate().back();
      expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      dashboard.cellLink('Hosts');
      expect(dashboard.getBreadcrumbText()).toEqual('Hosts');

      // Object Gateways Cell
      browser.navigate().back();
      expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      element
        .all(by.partialLinkText('Object'))
        .last()
        .click(); // Since there is a space and there are 2 occurances of
      expect(dashboard.getBreadcrumbText()).toEqual('Daemons'); // 'Object Gateways', this method was used to grab the link

      // iSCSI Gateways Cell
      browser.navigate().back();
      expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      dashboard.partialCellLink('iSCSI'); // Since there is a space between iSCSI and Gateways this method was
      expect(dashboard.getBreadcrumbText()).toEqual('Overview'); // used to grab and click the link

      // Pools Cell
      browser.navigate().back();
      expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      dashboard.cellLink('Pools');
      expect(dashboard.getBreadcrumbText()).toEqual('Pools');
    });

    it('should verify that cells exist on dashboard in proper order', () => {
      // Ensures that info cards are all displayed on the dashboard tab while being
      // in the proper order, checks for card title and position via indexing into
      // a list of all info cards
      dashboard.navigateTo();
      expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      expect(dashboard.dashContain(0, 'Cluster Status'));
      expect(dashboard.dashContain(1, 'Monitors'));
      expect(dashboard.dashContain(2, 'OSDs'));
      expect(dashboard.dashContain(3, 'Manager Daemons'));
      expect(dashboard.dashContain(4, 'Hosts'));
      expect(dashboard.dashContain(5, 'Object Gateways'));
      expect(dashboard.dashContain(6, 'Metadata Servers'));
      expect(dashboard.dashContain(7, 'iSCSI Gateways'));
      expect(dashboard.dashContain(8, 'Client IOPS'));
      expect(dashboard.dashContain(9, 'Client Throughput'));
      expect(dashboard.dashContain(10, 'Clinet Read/Write'));
      expect(dashboard.dashContain(11, 'Recovery Throughput'));
      expect(dashboard.dashContain(12, 'Scrub'));
      expect(dashboard.dashContain(13, 'Pools'));
      expect(dashboard.dashContain(14, 'Raw Capacity'));
      expect(dashboard.dashContain(15, 'Objects'));
      expect(dashboard.dashContain(16, 'PGs per OSD'));
      expect(dashboard.dashContain(17, 'PG Status'));
    });

    it('should verify that info card group titles are present and in the right order', () => {
      // Checks that the group titles on the dashboard are correct and in the right order
      dashboard.navigateTo();
      expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      expect(dashboard.checkGroupTitles(0, 'Status'));
      expect(dashboard.checkGroupTitles(1, 'Peformance'));
      expect(dashboard.checkGroupTitles(2, 'Capacity'));
    });
  });

  describe('Should check that dashboard cells have correct information', () => {
    beforeAll(() => {
      dashboard.navigateTo();
    });

    it('should verify that proper number of object gateway daemons on dashboard', () => {
      // Checks that dashboard id card for Object Gateway has the correct number of Daemons
      // by checking the Daemons page and taking the count found at the bottom of the table
      dashboard.navigateTo();
      let dashCount = '';
      dashboard.cardNumb(5).then((num) => {
        dashCount = num; // Grabs number of daemons from dashboard Object Gateway card
      });
      daemons.navigateTo();
      daemons
        .getTableCount()
        .getText()
        .then((tableCount) => {
          tableCount = tableCount.slice(13); // Grabs number of daemons from table footer
          expect(dashCount).toContain(tableCount);
        });
    });

    it('should verify that proper number of monitors on dashboard', () => {
      // Checks that dashboard id card for Monitors has the correct number of Monitors
      // by checking the Monitors page and taking the count found at the bottom of the table
      dashboard.navigateTo();
      let dashCount = '';
      dashboard.cardNumb(1).then((num) => {
        dashCount = num; // Grabs number of monitors from dashboard Monitor card
      });
      monitors.navigateTo();
      $$('.datatable-footer-inner')
        .first()
        .getText()
        .then((tableCount) => {
          tableCount = tableCount.slice(0, -6); // Grabs number of monitors from table footer
          expect(dashCount).toContain(tableCount);
        });
    });

    it('should verify that proper number of hosts on dashboard', () => {
      // Checks that dashboard id card for Hosts has the correct number of hosts
      // by checking the Hosts page and taking the count found at the bottom of the table
      dashboard.navigateTo();
      let dashCount = '';
      dashboard.cardNumb(4).then((num) => {
        dashCount = num; // Grabs number of hosts from dashboard Hosts card
      });
      hosts.navigateTo();
      hosts
        .getTableCount()
        .getText()
        .then((tableCount) => {
          tableCount = tableCount.slice(13, -6); // Grabs number of hosts from table footer
          expect(dashCount).toContain(tableCount);
        });
    });

    it('should verify that proper number of osds on dashboard', () => {
      // Checks that dashboard id card for Hosts has the correct number of hosts
      // by checking the Hosts page and taking the count found at the bottom of the table
      dashboard.navigateTo();
      let dashCount = '';
      dashboard.cardNumb(2).then((num) => {
        dashCount = num.slice(0, -17); // Grabs number of hosts from dashboard Hosts card
      });
      osds.navigateTo();
      osds
        .getTableCount()
        .getText()
        .then((tableCount) => {
          tableCount = tableCount.slice(13, -6); // Grabs number of hosts from table footer
          expect(dashCount).toContain(tableCount);
        });
    });

    it('should verify that proper number of pools on dashboard', () => {
      dashboard.navigateTo();
      let dashCount = '';
      dashboard.cardNumb(13).then((num) => {
        dashCount = num.slice(4); // Grabs number of hosts from dashboard Pools card
        pools.navigateTo();
        pools
          .getTableCount()
          .getText()
          .then((tableCount) => {
            tableCount = tableCount.slice(13, -6); // Grabs number of pools from table footer
            expect(dashCount).toContain(tableCount);
          });
      });
    });

    it('should verify that proper number of iscsi gateways on dashboard', () => {
      // Checks that dashboard id card for iSCSI has the correct number of gateways
      // by checking the iSCSI page and taking the count found at the bottom of the table (first)
      dashboard.navigateTo();
      let dashCount = '';
      dashboard.cardNumb(7).then((num) => {
        dashCount = num; // Grabs number of gateways from dashboard iSCSI card
      });
      iscsi.navigateTo();
      $$('.datatable-footer-inner')
        .first()
        .getText()
        .then((tableCount) => {
          tableCount = tableCount.slice(0, -6); // Grabs number of monitors from table footer
          expect(dashCount).toContain(tableCount);
        });
    });
  });
});
