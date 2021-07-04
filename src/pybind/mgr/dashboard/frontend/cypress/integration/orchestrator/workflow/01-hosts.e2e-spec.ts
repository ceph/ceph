import { HostsPageHelper } from 'cypress/integration/cluster/hosts.po';
import { ServicesPageHelper } from 'cypress/integration/cluster/services.po';

describe('Hosts page', () => {
  const hosts = new HostsPageHelper();
  const services = new ServicesPageHelper();

  const serviceName = 'rgw.foo';
  const hostnames = [
    'ceph-node-00.cephlab.com',
    'ceph-node-01.cephlab.com',
    'ceph-node-02.cephlab.com'
  ];
  const addHost = (hostname: string, exist?: boolean, maintenance?: boolean) => {
    hosts.navigateTo('add');
    hosts.add(hostname, exist, maintenance);
    hosts.checkExist(hostname, true);
  };

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    hosts.navigateTo();
  });

  describe('when Orchestrator is available', () => {
    it('should add a host', () => {
      addHost(hostnames[2], false, false);
    });

    it('should display inventory', function () {
      hosts.clickHostTab(hostnames[0], 'Physical Disks');
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
      hosts.navigateTo('add');
      hosts.add(hostnames[0], true);
    });

    it('should add a host in maintenance mode', function () {
      addHost(hostnames[1], false, true);
    });

    it('should delete a host and add it back', function () {
      hosts.delete(hostnames[1]);
      addHost(hostnames[1], false, true);
    });

    it('should exit host from maintenance', function () {
      hosts.maintenance(hostnames[1], true);
    });

    it('should check if mon service is running', () => {
      hosts.clickHostTab(hostnames[1], 'Daemons');
      cy.get('cd-host-details').within(() => {
        services.checkServiceStatus('mon');
      });
    });

    it('should create rgw services', () => {
      services.navigateTo('create');
      services.addService('rgw', false, '3');
      services.checkExist(serviceName, true);
      hosts.navigateTo();
      hosts.clickHostTab(hostnames[1], 'Daemons');
      cy.get('cd-host-details').within(() => {
        services.checkServiceStatus('rgw');
      });
    });

    it('should force maintenance', () => {
      hosts.maintenance(hostnames[1], true, true);
    });
  });
});
