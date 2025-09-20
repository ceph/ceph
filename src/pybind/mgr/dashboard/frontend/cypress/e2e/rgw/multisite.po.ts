import { PageHelper } from '../page-helper.po';

const WAIT_TIMER = 1000;
const pages = {
  index: { url: '#/rgw/multisite/sync-policy', id: 'cd-rgw-multisite-sync-policy' },
  create: {
    url: '#/rgw/multisite/sync-policy/(modal:create)',
    id: 'cd-rgw-multisite-sync-policy-form'
  },
  edit: { url: '#/rgw/multisite/sync-policy/(modal:edit', id: 'cd-rgw-multisite-sync-policy-form' },
  topology: { url: '#/rgw/multisite/configuration', id: 'cd-rgw-multisite-details' },
  wizard: {
    url: '#/rgw/multisite/configuration/(modal:setup-multisite-replication)',
    id: 'cd-rgw-multisite-wizard'
  }
};

enum WizardSteps {
  CreateRealmZonegroup = 'Create Realm & Zonegroup',
  CreateZone = 'Create Zone',
  Review = 'Review'
}

type Step = keyof typeof WizardSteps;

export class MultisitePageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    status: 4
  };

  tableExist() {
    cy.get('cd-rgw-multisite-sync-policy cd-table').should('exist');
    cy.get('cd-rgw-multisite-sync-policy cd-table-actions').should('exist');
  }

  @PageHelper.restrictTo(pages.create.url)
  create(group_id: string, status: string, bucket_name: string) {
    // Enter in group_id
    cy.get('#group_id').type(group_id);
    // Show Status
    this.selectOption('status', status);
    cy.get('#status').should('have.class', 'ng-valid');
    // Enter the bucket_name
    cy.get('#bucket_name').type(bucket_name);
    // Click the create button and wait for policy to be made
    cy.contains('button', 'Create Sync Policy Group').wait(WAIT_TIMER).click();
    this.getFirstTableCell(group_id).should('exist');
  }

  @PageHelper.restrictTo(pages.index.url)
  edit(group_id: string, status: string, bucket_name: string) {
    cy.visit(`${pages.edit.url}/${group_id}/${bucket_name})`);

    // Change the status field
    this.selectOption('status', status);
    cy.contains('button', 'Edit Sync Policy Group').click();

    this.searchTable(group_id);
    cy.get(`[cdstabledata]:nth-child(${this.columnIndex.status})`)
      .find('.badge-warning')
      .should('contain', status);
  }

  @PageHelper.restrictTo(pages.index.url)
  createSymmetricalFlow(flow_id: string, zones: string[]) {
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

    cy.get('cd-rgw-multisite-sync-policy-details .[cdstabledata]').should('contain', flow_id);

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
      cy.get('.[cdstabledata]').should('contain', flow_id);
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
    return cy.contains(`${nestedClass} .[cdstabledata]`, content);
  }

  @PageHelper.restrictTo(pages.index.url)
  deleteSymFlow(flow_id: string) {
    cy.get('cd-rgw-multisite-sync-policy-details').should('exist');
    this.getTab('Flow').should('exist');
    this.getTab('Flow').click();
    cy.get('cd-rgw-multisite-sync-policy-details').within(() => {
      cy.get('.[cdstabledata]').should('contain', flow_id);
      cy.get('[aria-label=search]').first().clear({ force: true }).type(flow_id);
    });

    const getRow = this.getTableCellWithContent.bind(this);
    getRow('cd-rgw-multisite-sync-policy-details', flow_id).click();

    cy.get('cd-rgw-multisite-sync-policy-details').within(() => {
      cy.get('.table-actions button.dropdown-toggle').first().click(); // open submenu
      cy.get(`button.delete`).first().click();
    });

    cy.get('cds-modal .custom-control-label').click();
    cy.get('[aria-label="Delete Flow"]').click();
    cy.get('cds-modal').should('not.exist');

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
      .find('.[cdstabledata]')
      .should('contain', dest_zones[0]);
  }

  @PageHelper.restrictTo(pages.index.url)
  createPipe(pipe_id: string, source_zones: string[], dest_zones: string[]) {
    cy.get('cd-rgw-multisite-sync-policy-details').should('exist');
    this.getTab('Pipe').should('exist');
    this.getTab('Pipe').click();
    cy.request({
      method: 'GET',
      url: '/api/rgw/daemon',
      headers: { Accept: 'application/vnd.ceph.api.v1.0+json' }
    });
    cy.get('cd-rgw-multisite-sync-policy-details .table-actions button').first().click();
    cy.get('cd-rgw-multisite-sync-pipe-modal').should('exist');

    // Enter in pipe_id
    cy.get('#pipe_id').type(pipe_id);
    cy.wait(WAIT_TIMER);
    // Select zone
    cy.get('a[data-testid=select-menu-edit]').eq(0).click();
    for (const zone of source_zones) {
      cy.get('.popover-body div.select-menu-item-content').contains(zone).click();
    }
    cy.get('cd-rgw-multisite-sync-pipe-modal').click();
    cy.get('a[data-testid=select-menu-edit]').eq(1).click();
    for (const zone of dest_zones) {
      cy.get('.popover-body input').type(`${zone}{enter}`);
    }
    cy.get('button.tc_submitButton').click();

    cy.get('cd-rgw-multisite-sync-policy-details .[cdstabledata]').should('contain', pipe_id);

    cy.get('cd-rgw-multisite-sync-policy-details')
      .first()
      .find('[aria-label=search]')
      .first()
      .clear({ force: true })
      .type(pipe_id);
  }

  @PageHelper.restrictTo(pages.index.url)
  editPipe(pipe_id: string, zoneToAdd: string) {
    cy.get('cd-rgw-multisite-sync-policy-details').should('exist');
    this.getTab('Pipe').should('exist');
    this.getTab('Pipe').click();
    cy.request({
      method: 'GET',
      url: '/api/rgw/daemon',
      headers: { Accept: 'application/vnd.ceph.api.v1.0+json' }
    });

    cy.get('cd-rgw-multisite-sync-policy-details').within(() => {
      cy.get('.[cdstabledata]').should('contain', pipe_id);
      cy.get('[aria-label=search]').first().clear({ force: true }).type(pipe_id);
      cy.get('input.cd-datatable-checkbox').first().check();
      cy.get('.table-actions button').first().click();
    });
    cy.get('cd-rgw-multisite-sync-pipe-modal').should('exist');

    cy.wait(WAIT_TIMER);
    // Enter in pipe_id
    cy.get('#pipe_id').should('contain.value', pipe_id);
    // Select zone
    cy.get('a[data-testid=select-menu-edit]').eq(1).click();

    cy.get('.popover-body input').type(`${zoneToAdd}{enter}`);

    cy.get('button.tc_submitButton').click();

    this.getNestedTableCell('cd-rgw-multisite-sync-policy-details', 4, zoneToAdd, true);
  }

  @PageHelper.restrictTo(pages.index.url)
  deletePipe(pipe_id: string) {
    cy.get('cd-rgw-multisite-sync-policy-details').should('exist');
    this.getTab('Pipe').should('exist');
    this.getTab('Pipe').click();
    cy.get('cd-rgw-multisite-sync-policy-details').within(() => {
      cy.get('.[cdstabledata]').should('contain', pipe_id);
      cy.get('[aria-label=search]').first().clear({ force: true }).type(pipe_id);
    });

    const getRow = this.getTableCellWithContent.bind(this);
    getRow('cd-rgw-multisite-sync-policy-details', pipe_id).click();

    cy.get('cd-rgw-multisite-sync-policy-details').within(() => {
      cy.get('.table-actions button.dropdown-toggle').first().click(); // open submenu
      cy.get(`button.delete`).first().click();
    });

    cy.get('cd-modal .custom-control-label').click();
    cy.get('[aria-label="Delete Pipe"]').click();
    cy.get('cd-modal').should('not.exist');

    cy.get('cd-rgw-multisite-sync-policy-details')
      .first()
      .within(() => {
        cy.get('[aria-label=search]').first().clear({ force: true }).type(pipe_id);
      });
    // Waits for item to be removed from table
    getRow(pipe_id).should('not.exist');
  }

  @PageHelper.restrictTo(pages.topology.url)
  topologyViewerExist() {
    cy.get(pages.topology.id).should('be.visible');
    cy.get('[data-testid=rgw-multisite-details-header]').should('have.text', 'Topology Viewer');
  }

  @PageHelper.restrictTo(pages.wizard.url)
  replicationWizardExist() {
    cy.get('cds-modal').then(() => {
      cy.get('[data-testid=rgw-multisite-wizard-header]').should(
        'have.text',
        'Set up Multi-site Replication'
      );
    });
  }

  @PageHelper.restrictTo(pages.index.url)
  verifyWizardContents(step: Step) {
    cy.get('cds-modal').then(() => {
      this.gotoStep(step);
      if (step === 'CreateRealmZonegroup') {
        this.typeValueToField('realmName', 'test-realm');
        this.typeValueToField('zonegroupName', 'test-zg');
      } else if (step === 'CreateZone') {
        this.typeValueToField('zoneName', 'test-zone');
      } else {
        this.gotoStep('Review');
        cy.get('.form-group.row').then(() => {
          cy.get('#realmName').invoke('text').should('eq', 'test-realm');
          cy.get('#zonegroupName').invoke('text').should('eq', 'test-zg');
          cy.get('#zoneName').invoke('text').should('eq', 'test-zone');
        });
      }
    });
  }

  typeValueToField(fieldID: string, value: string) {
    cy.get(`#${fieldID}`).clear().type(value).should('have.value', value);
  }

  gotoStep(step: Step) {
    cy.get('cd-wizard').then(() => {
      cy.get('form').should('be.visible');
      cy.get('button').contains(WizardSteps[step]).click();
    });
  }
}
