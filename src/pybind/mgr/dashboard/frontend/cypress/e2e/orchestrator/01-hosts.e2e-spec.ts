import { HostsPageHelper } from '../cluster/hosts.po';

describe('Hosts page', () => {
  const hosts = new HostsPageHelper();

  beforeEach(() => {
    cy.login();
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

    it('should drain and remove a host and then add it back', function () {
      const hostname = Cypress._.last(this.hosts)['name'];

      // should drain the host first before deleting
      hosts.drain(hostname);
      hosts.remove(hostname);

      // add it back
      hosts.navigateTo('add');
      hosts.add(hostname);
      hosts.checkExist(hostname, true);
    });

    it('should display inventory', function () {
      for (const host of this.hosts) {
        hosts.clickTab('cd-host-details', host.name, 'Physical Disks');
        cy.get('cd-host-details').within(() => {
          hosts.expectTableCount('total', host.devices.length);
        });
      }
    });

    it('should display daemons', function () {
      for (const host of this.hosts) {
        hosts.clickTab('cd-host-details', host.name, 'Daemons');
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
  });
});
