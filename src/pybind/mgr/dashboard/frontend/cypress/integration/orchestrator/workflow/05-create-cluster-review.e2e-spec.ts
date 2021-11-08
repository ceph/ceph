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

    it('should check Host Details table is present', () => {
      // check for there to be two tables
      createCluster.getDataTables().should('have.length', 1);

      // verify correct columns on Host Details table
      createCluster.getDataTableHeaders(0).contains('Hostname');

      createCluster.getDataTableHeaders(0).contains('Labels');

      createCluster.getDataTableHeaders(0).contains('CPUs');

      createCluster.getDataTableHeaders(0).contains('Cores');

      createCluster.getDataTableHeaders(0).contains('Total Memory');

      createCluster.getDataTableHeaders(0).contains('Raw Capacity');

      createCluster.getDataTableHeaders(0).contains('HDDs');

      createCluster.getDataTableHeaders(0).contains('Flash');

      createCluster.getDataTableHeaders(0).contains('NICs');
    });

    it('should check default host name is present', () => {
      createClusterHostPage.check_for_host();
    });
  });
});
