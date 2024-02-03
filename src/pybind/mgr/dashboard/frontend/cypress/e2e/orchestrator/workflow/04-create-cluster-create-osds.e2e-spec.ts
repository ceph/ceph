/* tslint:disable*/
import { CreateClusterWizardHelper } from '../../cluster/create-cluster.po';
import { OSDsPageHelper } from '../../cluster/osds.po';
/* tslint:enable*/

const osds = new OSDsPageHelper();

describe('Create cluster create osds page', () => {
  const createCluster = new CreateClusterWizardHelper();

  beforeEach(() => {
    cy.login();
    createCluster.navigateTo();
    createCluster.createCluster();
    cy.get('.nav-link').contains('Create OSDs').click();
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
        cy.get('.nav-link').contains('Review').click();
        cy.get('button[aria-label="Next"]').click();
        cy.get('cd-dashboard').should('exist');
        createCluster.navigateTo();
        createCluster.createCluster();
        cy.get('.nav-link').contains('Create OSDs').click();
      }
    });
  });
});
