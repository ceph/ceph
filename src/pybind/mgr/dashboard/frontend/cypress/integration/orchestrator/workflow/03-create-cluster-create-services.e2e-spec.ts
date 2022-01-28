import {
  CreateClusterServicePageHelper,
  CreateClusterWizardHelper
} from 'cypress/integration/cluster/create-cluster.po';

describe('Create cluster create services page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const createClusterServicePage = new CreateClusterServicePageHelper();

  const createService = (serviceType: string, serviceName: string, count = '1') => {
    cy.get('button[data-testid=table-action-button]').click();
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
    const serviceName = 'mds.test';

    it('should create an mds service', () => {
      createService('mds', serviceName, '1');
    });

    it('should edit a service', () => {
      const daemonCount = '2';
      createClusterServicePage.editService(serviceName, daemonCount);
      createClusterServicePage.expectPlacementCount(serviceName, daemonCount);
    });

    it('should delete mds service', () => {
      createClusterServicePage.deleteService('mds.test');
    });
  });
});
