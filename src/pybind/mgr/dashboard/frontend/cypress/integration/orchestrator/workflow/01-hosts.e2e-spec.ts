import { HostsPageHelper } from '../../cluster/hosts.po';

describe('Hosts page', () => {
  const hosts = new HostsPageHelper();
  const hostnames = ['ceph-node-00.cephlab.com', 'ceph-node-02.cephlab.com'];
  const addHost = (hostname: string, exist?: boolean) => {
    hosts.navigateTo('create');
    hosts.add(hostname, exist);
    hosts.checkExist(hostname, true);
  };

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    hosts.navigateTo();
  });

  describe('when Orchestrator is available', () => {
    it('should display inventory', function () {
      hosts.clickHostTab(hostnames[0], 'Inventory');
      cy.get('cd-host-details').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should display daemons', function () {
      hosts.clickHostTab(hostnames[0], 'Daemons');
      cy.get('cd-host-details').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should edit host labels', function () {
      const labels = ['foo', 'bar'];
      hosts.editLabels(hostnames[0], labels, true);
      hosts.editLabels(hostnames[0], labels, false);
    });

    it('should not add an existing host', function () {
      hosts.navigateTo('create');
      hosts.add(hostnames[0], true);
    });

    it('should add a host', function () {
      addHost(hostnames[1], false);
    });

    it('should delete a host and add it back', function () {
      hosts.delete(hostnames[1]);
      addHost(hostnames[1], false);
    });
  });
});
