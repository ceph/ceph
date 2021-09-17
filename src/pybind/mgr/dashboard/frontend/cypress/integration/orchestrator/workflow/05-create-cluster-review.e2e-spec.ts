import {
  CreateClusterHostPageHelper,
  CreateClusterWizardHelper
} from 'cypress/integration/cluster/create-cluster.po';

describe('Create Cluster Review page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const createClusterHostPage = new CreateClusterHostPageHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();

    cy.get('.nav-link').contains('Review').click();
  });

  describe('navigation link test', () => {
    it('should check if active nav-link is of Review section', () => {
      cy.get('.nav-link.active').should('contain.text', 'Review');
    });
  });

  describe('fields check', () => {
    it('should check cluster resources table is present', () => {
      // check for table header 'Cluster Resources'
      createCluster.getLegends().its(0).should('have.text', 'Cluster Resources');

      // check for fields in table
      createCluster.getStatusTables().should('contain.text', 'Hosts');
      createCluster.getStatusTables().should('contain.text', 'Storage Capacity');
      createCluster.getStatusTables().should('contain.text', 'CPUs');
      createCluster.getStatusTables().should('contain.text', 'Memory');
    });

    it('should check Hosts by Services and Host Details tables are present', () => {
      // check for there to be two tables
      createCluster.getDataTables().should('have.length', 2);

      // check for table header 'Hosts by Services'
      createCluster.getLegends().its(1).should('have.text', 'Hosts by Services');

      // check for table header 'Host Details'
      createCluster.getLegends().its(2).should('have.text', 'Host Details');

      // verify correct columns on Hosts by Services table
      createCluster.getDataTableHeaders(0).contains('Services');

      createCluster.getDataTableHeaders(0).contains('Number of Hosts');

      // verify correct columns on Host Details table
      createCluster.getDataTableHeaders(1).contains('Hostname');

      createCluster.getDataTableHeaders(1).contains('Labels');

      createCluster.getDataTableHeaders(1).contains('CPUs');

      createCluster.getDataTableHeaders(1).contains('Cores');

      createCluster.getDataTableHeaders(1).contains('Total Memory');

      createCluster.getDataTableHeaders(1).contains('Raw Capacity');

      createCluster.getDataTableHeaders(1).contains('HDDs');

      createCluster.getDataTableHeaders(1).contains('Flash');

      createCluster.getDataTableHeaders(1).contains('NICs');
    });

    it('should check default host name is present', () => {
      createClusterHostPage.check_for_host();
    });
  });
});
