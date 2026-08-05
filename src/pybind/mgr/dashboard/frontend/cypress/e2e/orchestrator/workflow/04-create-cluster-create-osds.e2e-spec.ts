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
    cy.get('cd-wizard').within(() => {
      cy.get('button').contains('Create OSDs').click();
    });
  });

  it('should check if title contains Create OSDs', () => {
    cy.get('.title').should('contain.text', 'Create OSDs');
  });

  describe('when Orchestrator is available', () => {
    it('should create OSDs', () => {
      const hostnames = ['ceph-node-00', 'ceph-node-01'];
      for (const hostname of hostnames) {
        osds.create('hdd', hostname, true);

        // Go to the Review section and Expand the cluster
        // because the drive group spec is only stored
        // in frontend and will be lost when refreshed
        cy.get('cd-wizard').within(() => {
          cy.get('button').contains('Review').click();
        });
        cy.get('button[aria-label="Next"]').click();
        cy.get('cd-overview').should('exist');
        onboarding.navigateTo();
        onboarding.onboarding();
        cy.get('cd-wizard').within(() => {
          cy.get('button').contains('Create OSDs').click();
        });
      }
    });
  });
});
