/* tslint:disable*/
import { PageHelper } from '../../../page-helper.po';
/* tslint:enable*/

const pages = {
  cephfs_index: { url: '#cephfs/nfs', id: 'cd-nfs-cluster' },
  cephfs_create: { url: '#cephfs/nfs/create', id: 'cd-nfs-form' },
  cephfs_cluster_create: { url: '#cephfs/nfs/cluster/create', id: 'cd-nfs-cluster-form' },
  rgw_index: { url: '#rgw/nfs', id: 'cd-nfs-cluster' },
  rgw_create: { url: '#rgw/nfs/create', id: 'cd-nfs-form' }
};

export class NFSPageHelper extends PageHelper {
  pages = pages;

  createCluster(clusterId: string, host?: string) {
    cy.get('#cluster_id').clear().type(clusterId);
    cy.contains('button', 'Show service deployment settings').click({ force: true });
    if (host) {
      this.selectOption('placement', 'Hosts');
      // Host combo-box selection is environment-specific; leave default when omitted.
    }
    cy.get('cd-submit-button').click();
  }

  create(backend: string, squash: string, client: object, pseudo: string, rgwPath?: string) {
    this.selectOption('cluster_id', 'testnfs');
    if (backend === 'CephFS') {
      this.selectOption('fs_name', 'myfs');
      cy.get('#security_label').click({ force: true });
    } else {
      cy.get('input[id=path]').type(rgwPath);
    }

    cy.get('input[name=pseudo]').type(pseudo);
    this.selectOption('squash', squash);

    // Add clients
    cy.get('button[name=add_client]').click({ force: true });
    cy.get('input[name=addresses]').type(client['addresses']);

    // Check if we can remove clients and add it again
    cy.get('[data-testid=remove_client]').click({ force: true });
    cy.get('button[name=add_client]').click({ force: true });
    cy.get('input[name=addresses]').type(client['addresses']);

    cy.get('cd-submit-button').click();
  }

  editExport(pseudo: string, editPseudo: string) {
    this.navigateEdit(pseudo, true, true);

    cy.get('input[name=pseudo]').clear().type(editPseudo);

    cy.get('cd-submit-button').click();

    // Click the export and check its details table for updated content
    this.getExpandCollapseElement(editPseudo).click();
    cy.get('.active.tab-pane').should('contain.text', editPseudo);
  }
}
