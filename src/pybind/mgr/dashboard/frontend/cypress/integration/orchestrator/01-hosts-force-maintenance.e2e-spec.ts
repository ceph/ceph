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
    });

    it('should force enter host into maintenance', function () {
      const hostname = Cypress._.sample(this.hosts).name;
      hosts.maintenance(hostname, true, true);
    });
  });
});
