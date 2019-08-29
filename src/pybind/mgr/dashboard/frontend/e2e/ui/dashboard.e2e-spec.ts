import { browser } from 'protractor';
import { IscsiPageHelper } from '../block/iscsi.po';
import { HostsPageHelper } from '../cluster/hosts.po';
import { MonitorsPageHelper } from '../cluster/monitors.po';
import { OSDsPageHelper } from '../cluster/osds.po';
import { PageHelper } from '../page-helper.po';
import { PoolPageHelper } from '../pools/pools.po';
import { DaemonsPageHelper } from '../rgw/daemons.po';
import { DashboardPageHelper } from './dashboard.po';

describe('Dashboard Main Page', () => {
  let dashboard: DashboardPageHelper;
  let daemons: DaemonsPageHelper;
  let hosts: HostsPageHelper;
  let osds: OSDsPageHelper;
  let pools: PoolPageHelper;
  let monitors: MonitorsPageHelper;
  let iscsi: IscsiPageHelper;

  beforeAll(() => {
    dashboard = new DashboardPageHelper();
    daemons = new DaemonsPageHelper();
    hosts = new HostsPageHelper();
    osds = new OSDsPageHelper();
    pools = new PoolPageHelper();
    monitors = new MonitorsPageHelper();
    iscsi = new IscsiPageHelper();
  });

  afterEach(async () => {
    await DashboardPageHelper.checkConsole();
  });

  describe('Check that all hyperlinks on info cards lead to the correct page and fields exist', () => {
    beforeEach(async () => {
      await dashboard.navigateTo();
    });

    it('should ensure that all linked info cards lead to correct page', async () => {
      const expectationMap = {
        Monitors: 'Monitors',
        OSDs: 'OSDs',
        Hosts: 'Hosts',
        'Object Gateways': 'Daemons',
        'iSCSI Gateways': 'Overview',
        Pools: 'Pools'
      };

      for (const [linkText, breadcrumbText] of Object.entries(expectationMap)) {
        await expect(browser.getCurrentUrl()).toContain('/#/dashboard');
        await dashboard.clickInfoCardLink(linkText);
        await dashboard.waitTextToBePresent(dashboard.getBreadcrumb(), breadcrumbText);
        await dashboard.navigateBack();
      }
    });

    it('should verify that info cards exist on dashboard in proper order', async () => {
      // Ensures that info cards are all displayed on the dashboard tab while being in the proper
      // order, checks for card title and position via indexing into a list of all info cards.
      const order = [
        'Cluster Status',
        'Monitors',
        'OSDs',
        'Manager Daemons',
        'Hosts',
        'Object Gateways',
        'Metadata Servers',
        'iSCSI Gateways',
        'Client IOPS',
        'Client Throughput',
        'Client Read/Write',
        'Recovery Throughput',
        'Scrub',
        'Pools',
        'Raw Capacity',
        'Objects',
        'PGs per OSD',
        'PG Status'
      ];

      for (let i = 0; i < order.length; i++) {
        await expect((await dashboard.infoCard(i)).getText()).toContain(
          order[i],
          `Order of ${order[i]} seems to be wrong`
        );
      }
    });

    it('should verify that info card group titles are present and in the right order', async () => {
      await expect(browser.getCurrentUrl()).toContain('/#/dashboard');
      await expect(dashboard.infoGroupTitle(0)).toBe('Status');
      await expect(dashboard.infoGroupTitle(1)).toBe('Performance');
      await expect(dashboard.infoGroupTitle(2)).toBe('Capacity');
    });
  });

  it('Should check that dashboard cards have correct information', async () => {
    interface TestSpec {
      cardName: string;
      regexMatcher?: RegExp;
      pageObject: PageHelper;
    }

    const testSpecs: TestSpec[] = [
      { cardName: 'Object Gateways', regexMatcher: /(\d+)\s+total/, pageObject: daemons },
      { cardName: 'Monitors', regexMatcher: /(\d+)\s+\(quorum/, pageObject: monitors },
      { cardName: 'Hosts', regexMatcher: /(\d+)\s+total/, pageObject: hosts },
      { cardName: 'OSDs', regexMatcher: /(\d+)\s+total/, pageObject: osds },
      { cardName: 'Pools', pageObject: pools },
      { cardName: 'iSCSI Gateways', regexMatcher: /(\d+)\s+total/, pageObject: iscsi }
    ];

    for (let i = 0; i < testSpecs.length; i++) {
      const spec = testSpecs[i];
      await dashboard.navigateTo();
      const infoCardBodyText = await dashboard.infoCardBodyText(spec.cardName);
      let dashCount = 0;
      if (spec.regexMatcher) {
        const match = infoCardBodyText.match(new RegExp(spec.regexMatcher));
        if (match && match.length > 1) {
          dashCount = Number(match[1]);
        } else {
          return Promise.reject(
            `Regex ${spec.regexMatcher} did not find a match for card with name ` +
              `${spec.cardName}`
          );
        }
      } else {
        dashCount = Number(infoCardBodyText);
      }
      await spec.pageObject.navigateTo();
      const tableCount = await spec.pageObject.getTableTotalCount();
      await expect(dashCount).toBe(
        tableCount,
        `Text of card "${spec.cardName}" and regex "${spec.regexMatcher}" resulted in ${dashCount} ` +
          `but did not match table count ${tableCount}`
      );
    }
  });
});
