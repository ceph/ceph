/* tslint:disable*/
import { OnboardingHelper } from '../../cluster/create-cluster.po';
import { OSDsPageHelper } from '../../cluster/osds.po';
/* tslint:enable*/

const osds = new OSDsPageHelper();

describe('Add storage - create osds page', () => {
  const onboarding = new OnboardingHelper();

  beforeEach(() => {
    cy.login();
    onboarding.navigateTo();
    onboarding.onboarding();
    onboarding.selectStep('Create OSDs');
  });

  it('should check if title contains Create OSDs', () => {
    cy.get('.tearsheet-body .tearsheet-content h4').should('contain.text', 'Create OSDs');
  });

  describe('when Orchestrator is available', () => {
    it('should create OSDs', () => {
      const hostnames = ['ceph-node-00', 'ceph-node-01'];
      for (const hostname of hostnames) {
        cy.get('cd-create-cluster-step-2 cd-osd-list [aria-label="Create"]').click();
        osds.create('hdd', hostname);

        // Complete storage setup after creating this host's OSDs.
        onboarding.selectStep('Review');
        onboarding.submitStorage();
        cy.get('cd-overview').should('exist');
        onboarding.navigateTo();
        onboarding.onboarding();
        onboarding.selectStep('Create OSDs');
      }
    });
  });
});
