import { MultiClusterPageHelper } from './multi-cluster.po';

describe('Muti-cluster management page', () => {
  const multiCluster = new MultiClusterPageHelper();
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
    multiCluster.checkConnectionStatus(alias, 'CONNECTED');
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
