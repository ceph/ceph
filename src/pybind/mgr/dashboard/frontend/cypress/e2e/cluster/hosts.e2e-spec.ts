import { HostsPageHelper } from './hosts.po';

describe('Hosts page', () => {
  const hosts = new HostsPageHelper();

  beforeEach(() => {
    cy.login();
    hosts.navigateTo();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', () => {
      hosts.expectBreadcrumbText('Hosts');
    });

    it('should show two tabs', () => {
      hosts.getTabsCount().should('eq', 2);
    });

    it('should show hosts list tab at first', () => {
      hosts.getTabText(0).should('eq', 'Hosts List');
    });

    it('should show overall performance as a second tab', () => {
      hosts.getTabText(1).should('eq', 'Overall Performance');
    });
  });

  describe('services link test', () => {
    it('should check at least one host is present', () => {
      hosts.check_for_host();
    });
  });
});
