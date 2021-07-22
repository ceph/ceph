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
      const hostnameList = new Array();
      this.hosts.array.forEach((host: any) => {
        hostnameList.push(host.hostname);
      });
      const serviceList = new Array();
      this.services.forEach((service: any) => {
        if (hostname === service.hostname) {
          serviceList.push(service.daemon_type);
        }
      });

      let enterMaintenance = true;

      // Force maintenance might throw out error if host are less than 3.
      if (hostnameList.length < 3) {
        enterMaintenance = false;
      }

      serviceList.forEach((service: string) => {
        if (service !== 'rgw' && (service === 'mgr' || service === 'alertmanager')) {
          enterMaintenance = false;
        }
      });

      if (enterMaintenance) {
        hosts.maintenance(hostname, true, true);
      }
    });
  });
});
