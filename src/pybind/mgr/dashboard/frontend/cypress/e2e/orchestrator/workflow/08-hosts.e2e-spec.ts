/* tslint:disable*/
import { HostsPageHelper } from '../../cluster/hosts.po';
import { ServicesPageHelper } from '../../cluster/services.po';
/* tslint:enable*/

describe('Host Page', () => {
  const hosts = new HostsPageHelper();
  const services = new ServicesPageHelper();

  const hostnames = ['ceph-node-00', 'ceph-node-01', 'ceph-node-02', 'ceph-node-03'];

  beforeEach(() => {
    cy.login();
    hosts.navigateTo();
  });

  // rgw is needed for testing the force maintenance
  it('should create rgw services', () => {
    services.navigateTo('create');
    services.addService('rgw', false, 4);
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

  it('should force maintenance and exit', () => {
    hosts.maintenance(hostnames[3], true, true);
  });

  it('should drain, remove and add the host back', () => {
    hosts.drain(hostnames[3]);
    hosts.remove(hostnames[3]);
    hosts.navigateTo('add');
    hosts.add(hostnames[3]);
    hosts.checkExist(hostnames[3], true);
  });

  it('should show the exact count of daemons', () => {
    hosts.checkServiceInstancesExist(hostnames[0], ['mgr: 1', 'prometheus: 1']);
  });
});
