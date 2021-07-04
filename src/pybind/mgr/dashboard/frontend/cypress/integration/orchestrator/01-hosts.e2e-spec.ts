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

    it('should not add an exsiting host', function () {
      const hostname = Cypress._.sample(this.hosts).name;
      hosts.navigateTo('add');
      hosts.add(hostname, true);
    });

    it('should delete a host and add it back', function () {
      const host = Cypress._.last(this.hosts)['name'];
      hosts.delete(host);

      // add it back
      hosts.navigateTo('add');
      hosts.add(host);
      hosts.checkExist(host, true);
    });

    it('should display inventory', function () {
      for (const host of this.hosts) {
        hosts.clickHostTab(host.name, 'Physical Disks');
        cy.get('cd-host-details').within(() => {
          hosts.getTableCount('total').should('be.gte', 0);
        });
      }
    });

    it('should display daemons', function () {
      for (const host of this.hosts) {
        hosts.clickHostTab(host.name, 'Daemons');
        cy.get('cd-host-details').within(() => {
          hosts.getTableCount('total').should('be.gte', 0);
        });
      }
    });

    it('should edit host labels', function () {
      const hostname = Cypress._.sample(this.hosts).name;
      const labels = ['foo', 'bar'];
      hosts.editLabels(hostname, labels, true);
      hosts.editLabels(hostname, labels, false);
    });

    it('should enter host into maintenance', function () {
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
        hosts.maintenance(hostname);
      }
    });

    it('should exit host from maintenance', function () {
      const hostname = Cypress._.sample(this.hosts).name;
      hosts.maintenance(hostname, true);
    });
  });
});
