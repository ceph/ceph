import { HostsPageHelper } from '../cluster/hosts.po';

describe('Hosts page', () => {
  const hosts = new HostsPageHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    hosts.navigateTo();
  });

  describe('when Orchestrator is available', () => {
    beforeEach(function () {
      cy.fixture('orchestrator/inventory.json').as('hosts');
      cy.fixture('orchestrator/services.json').as('services');
    });

    it('should force enter host into maintenance', function () {
      const hostname = Cypress._.sample(this.hosts).name;
      const serviceList = new Array();
      this.services.forEach((service: any) => {
        if (hostname === service.hostname) {
          serviceList.push(service.daemon_type);
        }
      });

      let enterMaintenance = true;
      serviceList.forEach((service: string) => {
        if (service === 'mgr' || service === 'alertmanager') {
          enterMaintenance = false;
        }
      });

      if (enterMaintenance) {
        hosts.maintenance(hostname, true, true);
      }
    });
  });
});
