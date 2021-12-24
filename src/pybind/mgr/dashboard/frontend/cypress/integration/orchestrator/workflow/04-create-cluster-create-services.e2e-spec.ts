import {
  CreateClusterServicePageHelper,
  CreateClusterWizardHelper
} from 'cypress/integration/cluster/create-cluster.po';

describe('Create cluster create services page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const createClusterServicePage = new CreateClusterServicePageHelper();

  const createService = (serviceType: string, serviceName: string, count?: string) => {
    cy.get('.btn.btn-accent').first().click({ force: true });
    createClusterServicePage.addService(serviceType, false, count);
    createClusterServicePage.checkExist(serviceName, true);
  };

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
      createService('rgw', serviceName, '2');
    });

    it('should delete the service and add it back', () => {
      createClusterServicePage.deleteService(serviceName);

      createService('rgw', serviceName, '2');
    });

    it('should edit a service', () => {
      const count = '3';
      createClusterServicePage.editService(serviceName, count);
      createClusterServicePage.expectPlacementCount(serviceName, count);
    });

    it('should create an ingress service', () => {
      createService('ingress', 'ingress.rgw.foo', '2');
    });
  });
});
