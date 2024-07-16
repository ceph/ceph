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
    status: 4
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

  @PageHelper.restrictTo(pages.index.url)
  createSymmetricalFlow(flow_id: string, zones: string[]) {
    cy.get('cd-rgw-multisite-sync-policy-details').should('exist');
    this.getTab('Flow').should('exist');
    this.getTab('Flow').click();
    cy.request({
      method: 'GET',
      url: '/api/rgw/daemon',
      headers: { Accept: 'application/vnd.ceph.api.v1.0+json' }
    });
    cy.get('cd-rgw-multisite-sync-policy-details .table-actions button').first().click();
    cy.get('cd-rgw-multisite-sync-flow-modal').should('exist');

    // Enter in flow_id
    cy.get('#flow_id').type(flow_id);
    // Select zone
    cy.get('a[data-testid=select-menu-edit]').click();
    for (const zone of zones) {
      cy.get('.popover-body div.select-menu-item-content').contains(zone).click();
    }

    cy.get('button.tc_submitButton').click();

    cy.get('cd-rgw-multisite-sync-policy-details .datatable-body-cell-label').should(
      'contain',
      flow_id
    );

    cy.get('cd-rgw-multisite-sync-policy-details')
      .first()
      .find('[aria-label=search]')
      .first()
      .clear({ force: true })
      .type(flow_id);
  }

  @PageHelper.restrictTo(pages.index.url)
  editSymFlow(flow_id: string, zoneToAdd: string) {
    cy.get('cd-rgw-multisite-sync-policy-details').should('exist');
    this.getTab('Flow').should('exist');
    this.getTab('Flow').click();
    cy.request({
      method: 'GET',
      url: '/api/rgw/daemon',
      headers: { Accept: 'application/vnd.ceph.api.v1.0+json' }
    });

    cy.get('cd-rgw-multisite-sync-policy-details').within(() => {
      cy.get('.datatable-body-cell-label').should('contain', flow_id);
      cy.get('[aria-label=search]').first().clear({ force: true }).type(flow_id);
      cy.get('input.cd-datatable-checkbox').first().check();
      cy.get('.table-actions button').first().click();
    });
    cy.get('cd-rgw-multisite-sync-flow-modal').should('exist');

    // Enter in flow_id
    cy.get('#flow_id').wait(100).should('contain.value', flow_id);
    // Select zone
    cy.get('a[data-testid=select-menu-edit]').click();

    cy.get('.popover-body div.select-menu-item-content').contains(zoneToAdd).click();

    cy.get('button.tc_submitButton').click();

    this.getNestedTableCell('cd-rgw-multisite-sync-policy-details', 3, zoneToAdd, true);
  }

  getTableCellWithContent(nestedClass: string, content: string) {
    return cy.contains(`${nestedClass} .datatable-body-cell-label`, content);
  }

  @PageHelper.restrictTo(pages.index.url)
  deleteSymFlow(flow_id: string) {
    cy.get('cd-rgw-multisite-sync-policy-details').should('exist');
    this.getTab('Flow').should('exist');
    this.getTab('Flow').click();
    cy.get('cd-rgw-multisite-sync-policy-details').within(() => {
      cy.get('.datatable-body-cell-label').should('contain', flow_id);
      cy.get('[aria-label=search]').first().clear({ force: true }).type(flow_id);
    });

    const getRow = this.getTableCellWithContent.bind(this);
    getRow('cd-rgw-multisite-sync-policy-details', flow_id).click();

    cy.get('cd-rgw-multisite-sync-policy-details').within(() => {
      cy.get('.table-actions button.dropdown-toggle').first().click(); // open submenu
      cy.get(`button.delete`).first().click();
    });

    cy.get('cd-modal .custom-control-label').click();
    cy.get('[aria-label="Delete Flow"]').click();
    cy.get('cd-modal').should('not.exist');

    cy.get('cd-rgw-multisite-sync-policy-details')
      .first()
      .within(() => {
        cy.get('[aria-label=search]').first().clear({ force: true }).type(flow_id);
      });
    // Waits for item to be removed from table
    getRow(flow_id).should('not.exist');
  }

  createDirectionalFlow(flow_id: string, source_zones: string[], dest_zones: string[]) {
    cy.get('cd-rgw-multisite-sync-policy-details').should('exist');
    this.getTab('Flow').should('exist');
    this.getTab('Flow').click();
    cy.request({
      method: 'GET',
      url: '/api/rgw/daemon',
      headers: { Accept: 'application/vnd.ceph.api.v1.0+json' }
    });
    cy.get('cd-rgw-multisite-sync-policy-details cd-table')
      .eq(1)
      .find('.table-actions button')
      .first()
      .click();
    cy.get('cd-rgw-multisite-sync-flow-modal').should('exist');
    cy.wait(WAIT_TIMER);
    // Enter in flow_id
    cy.get('#flow_id').type(flow_id);
    // Select source zone
    cy.get('a[data-testid=select-menu-edit]').first().click();
    for (const zone of source_zones) {
      cy.get('.popover-body div.select-menu-item-content').contains(zone).click();
    }
    cy.get('cd-rgw-multisite-sync-flow-modal').click();

    // Select destination zone
    cy.get('a[data-testid=select-menu-edit]').eq(1).click();
    for (const dest_zone of dest_zones) {
      cy.get('.popover-body').find('input[type="text"]').type(`${dest_zone}{enter}`);
    }
    cy.get('button.tc_submitButton').click();

    cy.get('cd-rgw-multisite-sync-policy-details cd-table')
      .eq(1)
      .find('[aria-label=search]')
      .first()
      .clear({ force: true })
      .type(dest_zones[0]);
    cy.get('cd-rgw-multisite-sync-policy-details cd-table')
      .eq(1)
      .find('.datatable-body-cell-label')
      .should('contain', dest_zones[0]);
  }
}
