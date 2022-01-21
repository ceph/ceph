import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';
import { HostsPageHelper } from 'cypress/integration/cluster/hosts.po';
import { ServicesPageHelper } from 'cypress/integration/cluster/services.po';

describe('when cluster creation is completed', () => {
  const createCluster = new CreateClusterWizardHelper();
  const services = new ServicesPageHelper();
  const hosts = new HostsPageHelper();

  const hostnames = ['ceph-node-00', 'ceph-node-01', 'ceph-node-02', 'ceph-node-03'];

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

    it('should check if mon daemon is running on all hosts', () => {
      for (const hostname of hostnames) {
        hosts.clickTab('cd-host-details', hostname, 'Daemons');
        cy.get('cd-host-details').within(() => {
          services.checkServiceStatus('mon');
        });
      }
    });
  });
});
