import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';

describe('Create cluster page', () => {
  const createCluster = new CreateClusterWizardHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
  });

  it('should fail to create cluster', () => {
    createCluster.createCluster();
  });

  it('should skip to dashboard landing page', () => {
    createCluster.doSkip();
  });
});
