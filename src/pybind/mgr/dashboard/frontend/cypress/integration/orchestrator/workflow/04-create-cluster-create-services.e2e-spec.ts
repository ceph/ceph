import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';

describe('Create cluster create services page', () => {
  const createCluster = new CreateClusterWizardHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();
    cy.get('button[aria-label="Next"]').click();
    cy.get('button[aria-label="Next"]').click();
  });

  it('should check if nav-link and title contains Create Services', () => {
    cy.get('.nav-link').should('contain.text', 'Create Services');

    cy.get('.title').should('contain.text', 'Create Services');
  });

  describe('when Orchestrator is available', () => {
    it('should create an rgw service', () => {
      createCluster.addService('rgw');

      createCluster.checkExist('rgw.rgw', true);
    });

    it('should create and delete an ingress service', () => {
      createCluster.addService('ingress');

      createCluster.checkExist('ingress.rgw.rgw', true);

      createCluster.deleteService('ingress.rgw.rgw', 60000);
    });
  });
});
