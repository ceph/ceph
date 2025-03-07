/* tslint:disable*/
import {
  CreateClusterHostPageHelper,
  CreateClusterWizardHelper
} from '../../cluster/create-cluster.po';
/* tslint:enable*/

describe('Create Cluster Review page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const createClusterHostPage = new CreateClusterHostPageHelper();

  beforeEach(() => {
    cy.login();
    createCluster.navigateTo();
    createCluster.createCluster();

    cy.get('cd-wizard').within(() => {
      cy.get('button').contains('Review').click();
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
      createCluster.getDataTableHeaders().contains('Hostname');

      createCluster.getDataTableHeaders().contains('Labels');

      createCluster.getDataTableHeaders().contains('CPUs');

      createCluster.getDataTableHeaders().contains('Cores');

      createCluster.getDataTableHeaders().contains('Total Memory');

      createCluster.getDataTableHeaders().contains('Raw Capacity');

      createCluster.getDataTableHeaders().contains('HDDs');

      createCluster.getDataTableHeaders().contains('Flash');

      createCluster.getDataTableHeaders().contains('NICs');
    });

    it('should check default host name is present', () => {
      createClusterHostPage.check_for_host();
    });
  });
});
