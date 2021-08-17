import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';
import { HostsPageHelper } from 'cypress/integration/cluster/hosts.po';
import { OSDsPageHelper } from 'cypress/integration/cluster/osds.po';

describe('when cluster creation is completed', () => {
  const createCluster = new CreateClusterWizardHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
  });

  it('should redirect to dashboard landing page after cluster creation', () => {
    createCluster.navigateTo();
    createCluster.createCluster();

    cy.get('button[aria-label="Next"]').click();
    cy.get('button[aria-label="Next"]').click();
    cy.get('button[aria-label="Next"]').click();

    cy.get('cd-dashboard').should('exist');
  });

  describe('Hosts page', () => {
    const hosts = new HostsPageHelper();
    const hostnames = ['ceph-node-00.cephlab.com', 'ceph-node-02.cephlab.com'];

    beforeEach(() => {
      hosts.navigateTo();
    });
    it('should have removed "_no_schedule" label', () => {
      for (let host = 0; host < hostnames.length; host++) {
        cy.get('datatable-row-wrapper').should('not.have.text', '_no_schedule');
      }
    });

    it('should display inventory', () => {
      hosts.clickHostTab(hostnames[1], 'Physical Disks');
      cy.get('cd-host-details').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });

    it('should display daemons', () => {
      hosts.clickHostTab(hostnames[1], 'Daemons');
      cy.get('cd-host-details').within(() => {
        hosts.getTableCount('total').should('be.gte', 0);
      });
    });
  });

  describe('OSDs page', () => {
    const osds = new OSDsPageHelper();

    beforeEach(() => {
      osds.navigateTo();
    });

    it('should check if osds are created', { retries: 1 }, () => {
      osds.expectTableCount('total', 2);
    });
  });
});
