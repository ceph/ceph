import {
  CreateClusterServicePageHelper,
  CreateClusterWizardHelper
} from 'cypress/integration/cluster/create-cluster.po';

describe('Create cluster create services page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const createClusterServicePage = new CreateClusterServicePageHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();
    cy.get('.nav-link').contains('Create Services').click();
  });

  it('should check if title contains Create Services', () => {
    cy.get('.title').should('contain.text', 'Create Services');
  });

  describe('when Orchestrator is available', () => {
    const serviceName = 'rgw.foo';

    it('should create an rgw service', () => {
      cy.get('.btn.btn-accent').first().click({ force: true });

      createClusterServicePage.addService('rgw', false, '2');
      createClusterServicePage.checkExist(serviceName, true);
    });

    it('should edit a service', () => {
      const count = '3';
      createClusterServicePage.editService(serviceName, count);
      createClusterServicePage.expectPlacementCount(serviceName, count);
    });

    it('should create and delete an ingress service', () => {
      cy.get('.btn.btn-accent').first().click({ force: true });

      createClusterServicePage.addService('ingress');
      createClusterServicePage.checkExist('ingress.rgw.foo', true);
      createClusterServicePage.deleteService('ingress.rgw.foo');
    });
  });
});
