/* tslint:disable*/
import { HostsPageHelper } from '../../cluster/hosts.po';
import { ServicesPageHelper } from '../../cluster/services.po';
/* tslint:enable*/

describe('Host Page', () => {
  const hosts = new HostsPageHelper();
  const services = new ServicesPageHelper();

  const hostnames = ['ceph-node-00', 'ceph-node-01', 'ceph-node-02', 'ceph-node-03'];

  // rgw is needed for testing force maintenance. Keep it in a separate describe
  // with its own beforeEach so the hosts-page navigation does not block it.
  describe('rgw service creation', () => {
    beforeEach(() => {
      cy.login();
    });

    it('should create rgw services', () => {
      services.navigateTo('create');
      services.addService('rgw', false, 4);
      services.navigateTo('index');
      services.checkExist('rgw.foo', true);
    });
  });

  describe('host operations', () => {
    beforeEach(() => {
      cy.login();
      hosts.navigateTo();
    });

    describe('should have all host details', () => {
      it('should have hostname', () => {
        cy.get('[data-testid="hostname"]').should('not.be.empty');
      });
      it('should have IP address', () => {
        cy.get('[data-testid="ip-address"]').should('not.be.empty');
      });
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
      hosts.checkExist(hostnames[3], true, true);
    });
  });
});
