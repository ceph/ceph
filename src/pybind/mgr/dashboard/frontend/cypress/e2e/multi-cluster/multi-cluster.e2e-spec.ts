import { DashboardPageHelper } from '../ui/dashboard.po';
import { MultiClusterPageHelper } from './multi-cluster.po';

describe('Muti-cluster management page', () => {
  const multiCluster = new MultiClusterPageHelper();
  const dashboard = new DashboardPageHelper();

  const hubName = 'local-cluster';
  const url = Cypress.env('CEPH2_URL');
  const alias = 'ceph2';
  const username = 'admin';
  const password = 'admin';

  const editedAlias = 'ceph2-edited';

  beforeEach(() => {
    cy.login();
    multiCluster.navigateTo('manage-clusters');
  });

  it('should authenticate the second cluster', () => {
    multiCluster.auth(url, alias, username, password);
    multiCluster.existTableCell(alias);
  });

  it('should switch to the second cluster and back to hub', () => {
    dashboard.navigateTo();
    cy.get('button[title="Selected Cluster:"]').click();
    cy.get('.text-secondary').contains(alias).click();
    cy.get('button[title="Selected Cluster:"]').contains(alias);
    cy.get('cd-dashboard-v3').should('exist');

    // now switch back to the hub cluster
    cy.get('button[title="Selected Cluster:"]').click();
    cy.get('.text-secondary').contains(hubName).click();
    cy.get('button[title="Selected Cluster:"]').contains(hubName);
    cy.get('cd-dashboard-v3').should('exist');
  });

  it('should reconnect the second cluster', () => {
    multiCluster.reconnect(alias, password);
    multiCluster.existTableCell(alias);
  });

  it('should edit the second cluster', () => {
    multiCluster.edit(alias, editedAlias);
    multiCluster.existTableCell(editedAlias);
  });

  it('should disconnect the second cluster', () => {
    multiCluster.disconnect(editedAlias);
    multiCluster.existTableCell(editedAlias, false);
  });
});
