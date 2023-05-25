/* tslint:disable*/
import { PageHelper } from '../../../page-helper.po';
/* tslint:enable*/

const pages = {
  index: { url: '#/nfs', id: 'cd-nfs-list' },
  create: { url: '#/nfs/create', id: 'cd-nfs-form' }
};

export class NFSPageHelper extends PageHelper {
  pages = pages;

  @PageHelper.restrictTo(pages.create.url)
  create(backend: string, squash: string, client: object, pseudo: string, rgwPath?: string) {
    this.selectOption('cluster_id', 'testnfs');
    // select a storage backend
    this.selectOption('name', backend);
    if (backend === 'CephFS') {
      this.selectOption('fs_name', 'myfs');

      cy.get('#security_label').click({ force: true });
    } else {
      cy.get('input[data-testid=rgw_path]').type(rgwPath);
    }

    cy.get('input[name=pseudo]').type(pseudo);
    this.selectOption('squash', squash);

    // Add clients
    cy.get('button[name=add_client]').click({ force: true });
    cy.get('input[name=addresses]').type(client['addresses']);

    // Check if we can remove clients and add it again
    cy.get('span[name=remove_client]').click({ force: true });
    cy.get('button[name=add_client]').click({ force: true });
    cy.get('input[name=addresses]').type(client['addresses']);

    cy.get('cd-submit-button').click();
  }

  editExport(pseudo: string, editPseudo: string) {
    this.navigateEdit(pseudo);

    cy.get('input[name=pseudo]').clear().type(editPseudo);

    cy.get('cd-submit-button').click();

    // Click the export and check its details table for updated content
    this.getExpandCollapseElement(editPseudo).click();
    cy.get('.active.tab-pane').should('contain.text', editPseudo);
  }
}
