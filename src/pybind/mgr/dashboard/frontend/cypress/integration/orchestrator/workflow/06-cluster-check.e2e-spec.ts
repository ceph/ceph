import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';
import { HostsPageHelper } from 'cypress/integration/cluster/hosts.po';
import { OSDsPageHelper } from 'cypress/integration/cluster/osds.po';
import { ServicesPageHelper } from 'cypress/integration/cluster/services.po';

describe('when cluster creation is completed', () => {
  const createCluster = new CreateClusterWizardHelper();
  const services = new ServicesPageHelper();
  const serviceName = 'rgw.foo';

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
  });

  it('should redirect to dashboard landing page after cluster creation', () => {
    createCluster.navigateTo();
    createCluster.createCluster();

    cy.get('.nav-link').contains('Review').click();
    cy.get('button[aria-label="Next"]').click();
    cy.get('cd-dashboard').should('exist');
  });

  describe('Hosts page', () => {
    const hosts = new HostsPageHelper();
    const hostnames = [
      'ceph-node-00.cephlab.com',
      'ceph-node-01.cephlab.com',
      'ceph-node-02.cephlab.com',
      'ceph-node-03.cephlab.com'
    ];

    beforeEach(() => {
      hosts.navigateTo();
    });

    it('should have removed "_no_schedule" label', () => {
      for (const hostname of hostnames) {
        hosts.checkLabelExists(hostname, ['_no_schedule'], false);
      }
    });

    it('should display inventory', () => {
      hosts.clickTab('cd-host-details', hostnames[1], 'Physical Disks');
      cy.get('cd-host-details').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should display daemons', () => {
      hosts.clickTab('cd-host-details', hostnames[1], 'Daemons');
      cy.get('cd-host-details').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should check if rgw service is running', () => {
      hosts.clickTab('cd-host-details', hostnames[3], 'Daemons');
      cy.get('cd-host-details').within(() => {
        services.checkServiceStatus('rgw');
      });
    });

    it('should force maintenance and exit', { retries: 1 }, () => {
      hosts.maintenance(hostnames[3], true, true);
    });

    it('should drain, remove and add the host back', () => {
      hosts.drain(hostnames[1]);
      hosts.remove(hostnames[1]);
      hosts.navigateTo('add');
      hosts.add(hostnames[1]);
      hosts.checkExist(hostnames[1], true);
    });
  });

  describe('OSDs page', () => {
    const osds = new OSDsPageHelper();

    beforeEach(() => {
      osds.navigateTo();
    });

    it('should check if osds are created', { retries: 1 }, () => {
      osds.getTableCount('total').should('be.gte', 1);
    });
  });

  describe('Services page', () => {
    beforeEach(() => {
      services.navigateTo();
    });

    it('should check if services are created', () => {
      services.checkExist(serviceName, true);
    });
  });
});
