/* tslint:disable*/
import {
  CreateClusterServicePageHelper,
  CreateClusterWizardHelper
} from '../../cluster/create-cluster.po';
/* tslint:enable*/

describe('Create cluster create services page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const createClusterServicePage = new CreateClusterServicePageHelper();

  const createService = (serviceType: string, serviceName: string, count = 1) => {
    cy.get('[aria-label=Create]').first().click();
    createClusterServicePage.addService(serviceType, false, count);
    createClusterServicePage.checkExist(serviceName, true);
  };

  beforeEach(() => {
    cy.login();
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
      createService('mds', serviceName);
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
