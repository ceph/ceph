import { IscsiPageHelper } from '../block/iscsi.po';
import { HostsPageHelper } from '../cluster/hosts.po';
import { ManagerModulesPageHelper } from '../cluster/mgr-modules.po';
import { MonitorsPageHelper } from '../cluster/monitors.po';
import { OSDsPageHelper } from '../cluster/osds.po';
import { PageHelper } from '../page-helper.po';
import { PoolPageHelper } from '../pools/pools.po';
import { DaemonsPageHelper } from '../rgw/daemons.po';
import { DashboardPageHelper } from './dashboard.po';

describe('Dashboard Main Page', () => {
  const dashboard = new DashboardPageHelper();
  const daemons = new DaemonsPageHelper();
  const hosts = new HostsPageHelper();
  const osds = new OSDsPageHelper();
  const pools = new PoolPageHelper();
  const monitors = new MonitorsPageHelper();
  const iscsi = new IscsiPageHelper();
  const mgrmodules = new ManagerModulesPageHelper();

  before(() => {
    cy.login();
    mgrmodules.navigateTo();
    mgrmodules.navigateEdit('dashboard');
    cy.get('#FEATURE_TOGGLE_DASHBOARD').uncheck();
    cy.contains('button', 'Update').click();
  });

  beforeEach(() => {
    cy.login();
    dashboard.navigateTo();
  });

  describe('Check that all hyperlinks on info cards lead to the correct page and fields exist', () => {
    it('should ensure that all linked info cards lead to correct page', () => {
      const expectationMap = {
        Monitors: 'Monitors',
        OSDs: 'OSDs',
        Hosts: 'Hosts',
        'Object Gateways': 'Gateways',
        'iSCSI Gateways': 'Overview',
        Pools: 'Pools'
      };

      for (const [linkText, breadcrumbText] of Object.entries(expectationMap)) {
        cy.location('hash').should('eq', '#/dashboard');
        dashboard.clickInfoCardLink(linkText);
        dashboard.expectBreadcrumbText(breadcrumbText);
        dashboard.navigateBack();
      }
    });

    it('should verify that info cards exist on dashboard in proper order', () => {
      // Ensures that info cards are all displayed on the dashboard tab while being in the proper
      // order, checks for card title and position via indexing into a list of all info cards.
      const order = [
        'Cluster Status',
        'Hosts',
        'Monitors',
        'OSDs',
        'Managers',
        'Object Gateways',
        'Metadata Servers',
        'iSCSI Gateways',
        'Raw Capacity',
        'Objects',
        'PG Status',
        'Pools',
        'PGs per OSD',
        'Client Read/Write',
        'Client Throughput',
        'Recovery Throughput',
        'Scrubbing'
      ];

      for (let i = 0; i < order.length; i++) {
        dashboard.infoCard(i).should('contain.text', order[i]);
      }
    });

    it('should verify that info card group titles are present and in the right order', () => {
      cy.location('hash').should('eq', '#/dashboard');
      dashboard.infoGroupTitle(0).should('eq', 'Status');
      dashboard.infoGroupTitle(1).should('eq', 'Capacity');
      dashboard.infoGroupTitle(2).should('eq', 'Performance');
    });
  });

  it('Should check that dashboard cards have correct information', () => {
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
      dashboard.navigateTo();

      dashboard.infoCardBodyText(spec.cardName).then((infoCardBodyText: string) => {
        let dashCount = 0;

        if (spec.regexMatcher) {
          const match = infoCardBodyText.match(new RegExp(spec.regexMatcher));
          expect(match).to.length.gt(
            1,
            `Regex ${spec.regexMatcher} did not find a match for card with name ` +
              `${spec.cardName}`
          );
          dashCount = Number(match[1]);
        } else {
          dashCount = Number(infoCardBodyText);
        }

        spec.pageObject.navigateTo();
        spec.pageObject.getTableCount('total').then((tableCount) => {
          expect(tableCount).to.eq(
            dashCount,
            `Text of card "${spec.cardName}" and regex "${spec.regexMatcher}" resulted in ${dashCount} ` +
              `but did not match table count ${tableCount}`
          );
        });
      });
    }
  });

  after(() => {
    cy.login();
    mgrmodules.navigateTo();
    mgrmodules.navigateEdit('dashboard');
    cy.get('#FEATURE_TOGGLE_DASHBOARD').click();
    cy.contains('button', 'Update').click();
  });
});
