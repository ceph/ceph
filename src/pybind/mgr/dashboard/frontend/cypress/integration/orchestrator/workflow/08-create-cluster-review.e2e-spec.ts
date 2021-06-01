import { CreateClusterWelcomePageHelper } from 'cypress/integration/cluster/cluster-welcome-page.po';
import { CreateClusterReviewPageHelper } from 'cypress/integration/cluster/create-cluster-review.po';

describe('Create Cluster Review page', () => {
  const reviewPage = new CreateClusterReviewPageHelper();
  const createCluster = new CreateClusterWelcomePageHelper();

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
      // check for table header 'Status'
      reviewPage.getLegends().its(0).should('have.text', 'Cluster Resources');

      // check for fields in table
      reviewPage.getStatusTables().should('contain.text', 'Hosts');
    });

    it('should check Hosts Per Label and Host Details tables are present', () => {
      // check for there to be two tables
      reviewPage.getDataTables().should('have.length', 2);

      // check for table header 'Hosts Per Label'
      reviewPage.getLegends().its(1).should('have.text', 'Hosts Per Label');

      // check for table header 'Host Details'
      reviewPage.getLegends().its(2).should('have.text', 'Host Details');

      // verify correct columns on Hosts Per Label table
      reviewPage.getDataTableHeaders(0).contains('Label');

      reviewPage.getDataTableHeaders(0).contains('Number of Hosts');

      // verify correct columns on Host Details table
      reviewPage.getDataTableHeaders(1).contains('Host Name');

      reviewPage.getDataTableHeaders(1).contains('Labels');
    });

    it('should check hosts count and default host name are present', () => {
      reviewPage.getStatusTables().should('contain.text', '1');

      reviewPage.checkDefaultHostName();
    });
  });
});
