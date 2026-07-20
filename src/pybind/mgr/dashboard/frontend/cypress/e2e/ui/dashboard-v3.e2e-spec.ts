import { ManagerModulesPageHelper } from '../cluster/mgr-modules.po';
import { DashboardV3PageHelper } from './dashboard-v3.po';

describe('Dashboard-v3 Main Page', () => {
  const overview = new DashboardV3PageHelper();
  const mgrmodules = new ManagerModulesPageHelper();

  before(() => {
    cy.login();
    mgrmodules.navigateTo();
    mgrmodules.navigateEdit('dashboard');
    cy.get('#FEATURE_TOGGLE_DASHBOARD').check();
    cy.contains('button', 'Update').click();
  });

  beforeEach(() => {
    cy.login();
    overview.navigateTo();
  });

  describe('Check that all hyperlinks on inventory card lead to the correct page and fields exist', () => {
    it('should ensure that all linked pages in the inventory card lead to correct page', () => {
      const expectationMap = {
        Host: 'Hosts',
        Monitor: 'Monitors',
        OSDs: 'OSDs',
        Pool: 'Pools',
        'Object Gateway': 'Gateways'
      };

      for (const [linkText, breadcrumbText] of Object.entries(expectationMap)) {
        cy.location('hash').should('eq', '#/overview');
        overview.clickInventoryCardLink(linkText);
        overview.expectBreadcrumbText(breadcrumbText);
        overview.navigateBack();
      }
    });

    it('should verify that cards exist on overview in proper order', () => {
      // Ensures that cards are all displayed on the overview tab while being in the proper
      // order, checks for card title and position via indexing into a list of all cards.
      const order = ['Details', 'Inventory', 'Status', 'Capacity', 'Cluster Utilization'];

      for (let i = 0; i < order.length; i++) {
        overview.card(i).should('contain.text', order[i]);
      }
    });
  });
});
