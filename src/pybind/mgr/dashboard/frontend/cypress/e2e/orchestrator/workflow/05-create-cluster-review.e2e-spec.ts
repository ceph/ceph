/* tslint:disable*/
import { CreateClusterHostPageHelper, OnboardingHelper } from '../../cluster/create-cluster.po';
/* tslint:enable*/

describe('Create Cluster Review page', () => {
  const onboarding = new OnboardingHelper();
  const createClusterHostPage = new CreateClusterHostPageHelper();

  beforeEach(() => {
    cy.login();
    onboarding.navigateTo();
    onboarding.onboarding();

    cy.get('cd-wizard').within(() => {
      cy.get('button').contains('Review').click();
    });
  });

  describe('fields check', () => {
    it('should check cluster resources table is present', () => {
      // check for table header 'Cluster Resources'
      onboarding.getLegends().its(0).should('have.text', 'Cluster Resources');

      // check for fields in table
      onboarding.getStatusTables().should('contain.text', 'Hosts');
      onboarding.getStatusTables().should('contain.text', 'Storage Capacity');
      onboarding.getStatusTables().should('contain.text', 'CPUs');
      onboarding.getStatusTables().should('contain.text', 'Memory');
    });

    it('should check Host Details table is present', () => {
      // check for there to be two tables
      onboarding.getDataTables().should('have.length', 1);

      // verify correct columns on Host Details table
      onboarding.getDataTableHeaders().contains('Hostname');

      onboarding.getDataTableHeaders().contains('Labels');

      onboarding.getDataTableHeaders().contains('CPUs');

      onboarding.getDataTableHeaders().contains('Cores');

      onboarding.getDataTableHeaders().contains('Total Memory');

      onboarding.getDataTableHeaders().contains('Raw Capacity');

      onboarding.getDataTableHeaders().contains('HDDs');

      onboarding.getDataTableHeaders().contains('Flash');

      onboarding.getDataTableHeaders().contains('NICs');
    });

    it('should check default host name is present', () => {
      createClusterHostPage.check_for_host();
    });
  });
});
