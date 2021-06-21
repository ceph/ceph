import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';

describe('Create Cluster Review page', () => {
  const createCluster = new CreateClusterWizardHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();

    cy.get('button[aria-label="Next"]').click();
  });

  describe('navigation link and title test', () => {
    it('should check if nav-link and title contains Review', () => {
      cy.get('.nav-link').should('contain.text', 'Review');

      cy.get('.title').should('contain.text', 'Review');
    });
  });

  describe('fields check', () => {
    it('should check cluster resources table is present', () => {
      // check for table header 'Cluster Resources'
      createCluster.getLegends().its(0).should('have.text', 'Cluster Resources');

      // check for fields in table
      createCluster.getStatusTables().should('contain.text', 'Hosts');
    });

    it('should check Hosts by Label and Host Details tables are present', () => {
      // check for there to be two tables
      createCluster.getDataTables().should('have.length', 2);

      // check for table header 'Hosts by Label'
      createCluster.getLegends().its(1).should('have.text', 'Hosts by Label');

      // check for table header 'Host Details'
      createCluster.getLegends().its(2).should('have.text', 'Host Details');

      // verify correct columns on Hosts by Label table
      createCluster.getDataTableHeaders(0).contains('Label');

      createCluster.getDataTableHeaders(0).contains('Number of Hosts');

      // verify correct columns on Host Details table
      createCluster.getDataTableHeaders(1).contains('Host Name');

      createCluster.getDataTableHeaders(1).contains('Labels');
    });

    it('should check hosts count and default host name are present', () => {
      createCluster.getStatusTables().contains(2);

      createCluster.check_for_host();
    });
  });
});
