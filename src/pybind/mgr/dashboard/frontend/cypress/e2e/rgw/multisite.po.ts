import { PageHelper } from '../page-helper.po';

const WAIT_TIMER = 1000;
const pages = {
  index: { url: '#/rgw/multisite', id: 'cd-rgw-multisite-details' },
  create: { url: '#/rgw/multisite/sync-policy/create', id: 'cd-rgw-multisite-sync-policy-form' },
  edit: { url: '#/rgw/multisite/sync-policy/edit', id: 'cd-rgw-multisite-sync-policy-form' }
};
export class MultisitePageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    status: 3
  };

  @PageHelper.restrictTo(pages.create.url)
  create(group_id: string, status: string) {
    // Enter in group_id
    cy.get('#group_id').type(group_id);
    // Show Status
    this.selectOption('status', status);
    cy.get('#status').should('have.class', 'ng-valid');

    // Click the create button and wait for policy to be made
    cy.contains('button', 'Create Sync Policy Group').wait(WAIT_TIMER).click();
    this.getFirstTableCell(group_id).should('exist');
  }

  @PageHelper.restrictTo(pages.index.url)
  edit(group_id: string, status: string) {
    cy.visit(`${pages.edit.url}/${group_id}`);

    // Change the status field
    this.selectOption('status', status);
    cy.contains('button', 'Edit Sync Policy Group').click();

    this.searchTable(group_id);
    cy.get(`datatable-body-cell:nth-child(${this.columnIndex.status})`)
      .find('.badge-warning')
      .should('contain', status);
  }
}
