/* tslint:disable*/
import { CreateClusterServicePageHelper, OnboardingHelper } from '../../cluster/create-cluster.po';
/* tslint:enable*/

describe('Create cluster create services page', () => {
  const onboardingPage = new OnboardingHelper();
  const createClusterServicePage = new CreateClusterServicePageHelper();

  const createService = (serviceType: string, serviceName: string, count = 1) => {
    cy.get('[aria-label=Create]').first().click();
    createClusterServicePage.addService(serviceType, false, count);
    createClusterServicePage.checkExist(serviceName, true);
  };

  beforeEach(() => {
    cy.login();
    onboardingPage.navigateTo();
    onboardingPage.onboarding();

    onboardingPage.selectStep('Create Services');
  });

  it('should check if title contains Create Services', () => {
    cy.get('.tearsheet-body .tearsheet-content h4').should('contain.text', 'Create Services');
  });

  describe('when Orchestrator is available', () => {
    const serviceName = 'mds.test';

    it('should create an mds service', () => {
      createService('mds', serviceName);
    });

    it('should edit a service', { retries: 2 }, () => {
      const daemonCount = '2';
      createClusterServicePage.editService(serviceName, daemonCount);
      // Navigate to another step and back to trigger a fresh service list load
      onboardingPage.selectStep('Review');
      onboardingPage.selectStep('Create Services');
      createClusterServicePage.expectPlacementCount(serviceName, daemonCount);
    });

    it('should delete mds service', () => {
      createClusterServicePage.deleteService('mds.test');
    });
  });
});
