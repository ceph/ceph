import { HostsPageHelper } from 'cypress/integration/cluster/hosts.po';
import { ServicesPageHelper } from 'cypress/integration/cluster/services.po';

describe('Host Page', () => {
  const hosts = new HostsPageHelper();
  const services = new ServicesPageHelper();

  const hostnames = ['ceph-node-00', 'ceph-node-01', 'ceph-node-02', 'ceph-node-03'];

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    hosts.navigateTo();
  });

  // rgw is needed for testing the force maintenance
  it('should create rgw services', () => {
    services.navigateTo('create');
    services.addService('rgw', false, '4');
    services.checkExist('rgw.foo', true);
  });

  it('should check if rgw daemon is running on all hosts', () => {
    for (const hostname of hostnames) {
      hosts.clickTab('cd-host-details', hostname, 'Daemons');
      cy.get('cd-host-details').within(() => {
        services.checkServiceStatus('rgw');
      });
    }
  });

  it('should force maintenance and exit', { retries: 2 }, () => {
    hosts.maintenance(hostnames[1], true, true);
  });

  it('should drain, remove and add the host back', () => {
    hosts.drain(hostnames[1]);
    hosts.remove(hostnames[1]);
    hosts.navigateTo('add');
    hosts.add(hostnames[1]);
    hosts.checkExist(hostnames[1], true);
  });
});
