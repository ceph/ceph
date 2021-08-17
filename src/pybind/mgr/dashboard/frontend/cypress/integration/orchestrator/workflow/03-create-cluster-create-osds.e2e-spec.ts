import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';
import { OSDsPageHelper } from 'cypress/integration/cluster/osds.po';

const osds = new OSDsPageHelper();

describe('Create cluster create osds page', () => {
  const createCluster = new CreateClusterWizardHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();
    cy.get('button[aria-label="Next"]').click();
  });

  it('should check if nav-link and title contains Create OSDs', () => {
    cy.get('.nav-link').should('contain.text', 'Create OSDs');

    cy.get('.title').should('contain.text', 'Create OSDs');
  });

  describe('when Orchestrator is available', () => {
    it('should create OSDs', () => {
      createCluster.create('hdd');

      cy.get('button[aria-label="Next"]').click();
      cy.get('button[aria-label="Next"]').click();

      cy.wait(3000);

      osds.navigateTo();
      osds.getTableCount('total').as('initOSDCount');

      cy.get('@newOSDCount').then((newCount) => {
        cy.get('@initOSDCount').then((oldCount) => {
          const expectedCount = Number(oldCount) + Number(newCount);

          // check total rows
          createCluster.expectTableCount('total', expectedCount);

          cy.wait(30000);
          expect(Number(newCount)).to.be.gte(1);
        });
      });
    });
  });
});
