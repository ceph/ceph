import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/cephfs/mirroring', id: 'cd-cephfs-mirroring-list' },
  fsCreate: { url: '#/cephfs/fs/create', id: 'cd-cephfs-form' },
  fsList: { url: '#/cephfs/fs', id: 'cd-cephfs-list' }
};

export class CephfsMirroringPageHelper extends PageHelper {
  pages = pages;

  createFilesystem(fsName: string) {
    this.navigateTo('fsCreate');
    cy.get('#name').clear().type(fsName);
    cy.get('cd-submit-button').click();
    cy.get('cd-cephfs-list').should('exist');
    this.existTableCell(fsName, true);
  }

  /**
   * Carbon keeps `id="filesystem"` on the cds-select host. The native
   * <select> is a child. Do not use a one-letter option with cy.select().
   */
  selectFilesystem(container: string, fsName: string) {
    cy.get(`${container} cds-select[id=filesystem] option[value="${fsName}"]`).should('exist');
    cy.get(`${container} cds-select[id=filesystem] select`).select(fsName, { force: true });
  }

  @PageHelper.restrictTo(pages.index.url)
  openSetupMirroring() {
    cy.contains('cd-clickable-tile', 'Set up mirroring').click();
    cy.get('cd-cephfs-setup-mirroring').should('be.visible');
  }

  @PageHelper.restrictTo(pages.index.url)
  importToken(fsName: string, token: string) {
    this.openSetupMirroring();
    this.selectFilesystem('cd-cephfs-setup-mirroring', fsName);
    cy.get('textarea[id=secureToken]')
      .clear()
      .type(token, { delay: 0, parseSpecialCharSequences: false });
    cy.get('[data-testid=submitBtn]').click();
    cy.get('cd-cephfs-setup-mirroring').should('not.exist');
  }

  @PageHelper.restrictTo(pages.index.url)
  expectMirroredFilesystem(fsName: string) {
    cy.contains('[cdstablerow] [cdstabledata]', fsName).should('be.visible');
  }

  /**
   * Snapshot schedules need the snap_schedule mgr module. The e2e runner only
   * enables mirroring; do this from the test instead of changing scripts.
   */
  enableSnapScheduleModule() {
    cy.request({
      method: 'GET',
      url: 'api/mgr/module',
      headers: this.apiHeaders()
    }).then((resp) => {
      const enabled = (resp.body || []).some(
        (mod: { name?: string; enabled?: boolean }) =>
          mod.name === 'snap_schedule' && mod.enabled
      );
      if (enabled) {
        return;
      }
      cy.request({
        method: 'POST',
        url: 'api/mgr/module/snap_schedule/enable',
        headers: this.apiHeaders(),
        failOnStatusCode: false
      });
    });
  }

  createDirectory(fsName: string, path: string) {
    this.getFilesystemId(fsName).then((fsId) => {
      cy.request({
        method: 'POST',
        url: `api/cephfs/${fsId}/tree`,
        qs: { path },
        body: { path },
        headers: this.apiHeaders()
      });
    });
  }

  createSubvolumeGroup(fsName: string, groupName: string) {
    cy.request({
      method: 'POST',
      url: 'api/cephfs/subvolume/group',
      body: { vol_name: fsName, group_name: groupName },
      headers: this.apiHeaders()
    });
    cy.request({
      method: 'GET',
      url: `api/cephfs/subvolume/group/${fsName}/info`,
      qs: { group_name: groupName },
      headers: this.apiHeaders()
    })
      .its('status')
      .should('eq', 200);
  }

  createSubvolume(fsName: string, subvolName: string, groupName: string) {
    cy.request({
      method: 'POST',
      url: 'api/cephfs/subvolume',
      body: {
        vol_name: fsName,
        subvol_name: subvolName,
        group_name: groupName
      },
      headers: this.apiHeaders()
    });
    cy.request({
      method: 'GET',
      url: `api/cephfs/subvolume/${fsName}/info`,
      qs: { subvol_name: subvolName, group_name: groupName },
      headers: this.apiHeaders()
    })
      .its('status')
      .should('eq', 200);
  }

  openAddMirrorPath(fsName: string) {
    this.interceptAddPathApis();
    cy.intercept('GET', '**/ui-api/cephfs/*/ls_dir*').as('lsDir');
    cy.visit(`#/cephfs/mirroring/${fsName}/mirror-paths`);
    cy.get('cd-cephfs-mirroring-fs-mirror-paths').should('exist');
    cy.get('[data-testid="primary-action"][aria-label="Add mirror path"]').click();
    cy.get('cd-cephfs-add-mirroring-path').should('be.visible');
    cy.wait('@lsDir');
  }

  openAddMirrorPathFromList(fsName: string) {
    this.interceptAddPathApis();
    cy.intercept('GET', '**/api/cephfs/mirror/daemon/status').as('daemonStatus');
    cy.intercept('GET', '**/ui-api/cephfs/*/ls_dir*').as('lsDir');
    this.navigateTo();
    cy.wait('@daemonStatus');
    this.clickRowOverflowAction('cd-cephfs-mirroring-list', fsName, 'Add mirror path');
    cy.get('cd-cephfs-add-mirroring-path').should('be.visible');
    cy.wait('@lsDir');
  }

  selectWizardPath(tileIndex: number, segments: string[]) {
    const tile = () => cy.get('cd-mirroring-paths-step form > .cds--tile').eq(tileIndex);
    segments.forEach((segment, level) => {
      tile().find('cds-loading').should('not.exist');
      tile()
        .find('cds-select')
        .eq(level)
        .find(`select option[value="${segment}"]`)
        .should('exist');
      tile().find('cds-select').eq(level).find('select').select(segment, { force: true });
    });
    tile().find('cds-loading').should('not.exist');
    tile().should('contain', `/${segments.join('/')}`);
  }

  addAnotherWizardPath() {
    cy.contains('cd-mirroring-paths-step button', 'Add another path')
      .should('not.be.disabled')
      .click();
    cy.get('cd-mirroring-paths-step form > .cds--tile').should('have.length.at.least', 2);
  }

  addPathWithHourlySchedule(dirName: string, fullPath: string) {
    this.selectWizardPath(0, [dirName]);
    this.completeHourlyScheduleAndSubmit([fullPath]);
  }

  addPathsWithHourlySchedule(paths: string[][], fsName: string) {
    paths.forEach((segments, index) => {
      if (index > 0) {
        this.addAnotherWizardPath();
      }
      this.selectWizardPath(index, segments);
    });
    this.completeHourlyScheduleAndSubmit(
      paths.map((segments) => `/${segments.join('/')}`),
      fsName
    );
  }

  expectMirroredPathWithSchedule(fsName: string, path: string) {
    this.openMirrorPathsPage(fsName);
    cy.contains('cd-cephfs-mirroring-fs-mirror-paths [cdstablerow]', path).should('be.visible');
    this.openPathSidePanel(path);
    cy.wait('@pathSchedule');
    cy.get('cd-side-panel').within(() => {
      this.clickSidePanelTab('Schedule policy');
      cy.contains('Every hour').should('be.visible');
    });
  }

  expectMirroredPaths(fsName: string, paths: string[]) {
    this.openMirrorPathsPage(fsName);
    paths.forEach((path) => {
      cy.contains('cd-cephfs-mirroring-fs-mirror-paths [cdstablerow]', path).should('be.visible');
    });
  }

  expectSidePanelTabs(fsName: string, path: string) {
    this.openMirrorPathsPage(fsName);
    cy.contains('cd-cephfs-mirroring-fs-mirror-paths [cdstablerow]', path).should('be.visible');
    this.openPathSidePanel(path);
    cy.get('cd-side-panel').should('contain', path);

    cy.get('cd-side-panel').within(() => {
      this.clickSidePanelTab('Details');
      cy.contains('Replication status').should('be.visible');
      cy.contains('Sync status').should('be.visible');
      cy.contains('Current snapshot').should('be.visible');
      cy.contains('Last replicated snapshot').should('be.visible');

      this.clickSidePanelTab('Snapshots');
      cy.contains('Total snapshots').should('be.visible');
      cy.contains('Checkpoints').should('be.visible');
      cy.contains('Pending').should('be.visible');

      this.clickSidePanelTab('Schedule policy');
      cy.contains('Total policies applied to path').should('be.visible');
    });
  }

  removeMirrorPath(fsName: string, path: string) {
    cy.intercept('DELETE', '**/api/cephfs/mirror/directory*').as('removeMirrorPath');
    cy.intercept('GET', '**/api/cephfs/mirror/*/status*').as('mirrorStatus');
    cy.visit(`#/cephfs/mirroring/${fsName}/mirror-paths`);
    cy.get('cd-cephfs-mirroring-fs-mirror-paths').should('exist');
    cy.wait('@mirrorStatus');
    this.clickRowOverflowAction('cd-cephfs-mirroring-fs-mirror-paths', path, 'Remove path');
    cy.get('cds-modal').should('be.visible');
    cy.get('[aria-label="confirmation"]').click({ force: true });
    cy.contains('cds-modal button', 'Remove mirror path').click();
    cy.wait('@removeMirrorPath').its('response.statusCode').should('be.oneOf', [200, 201, 202, 204]);
    cy.get('cds-modal').should('not.exist');
    cy.contains('cds-toast', `mirror path '${path}'`).should('be.visible');
    cy.contains('cd-cephfs-mirroring-fs-mirror-paths [cdstablerow]', path).should('not.exist');
  }

  disableMirroring(fsName: string) {
    cy.intercept('POST', '**/api/cephfs/mirror/disable').as('disableMirror');
    cy.intercept('GET', '**/api/cephfs/mirror/daemon/status').as('daemonStatus');
    this.navigateTo();
    cy.wait('@daemonStatus');
    cy.contains('[cdstablerow] [cdstabledata]', fsName).should('be.visible');
    this.clickRowOverflowAction('cd-cephfs-mirroring-list', fsName, 'Disable mirroring');
    cy.get('cds-modal').should('be.visible');
    cy.get('cds-modal input#resource_name').type(fsName);
    cy.contains('cds-modal button', 'Disable mirroring').click();
    cy.wait('@disableMirror').its('response.statusCode').should('be.oneOf', [200, 201, 202]);
    cy.get('cds-modal').should('not.exist');
    cy.contains('cds-toast', `mirroring for '${fsName}'`).should('be.visible');
    cy.contains('[cdstablerow] [cdstabledata]', fsName).should('not.exist');
  }

  private apiHeaders() {
    return { Accept: 'application/vnd.ceph.api.v1.0+json' };
  }

  private getFilesystemId(fsName: string) {
    return cy
      .request({
        method: 'GET',
        url: 'api/cephfs',
        headers: this.apiHeaders()
      })
      .then((resp) => {
        const fs = (resp.body || []).find(
          (item: { id?: number; mdsmap?: { fs_name?: string } }) =>
            item.mdsmap?.fs_name === fsName
        );
        expect(fs?.id, `filesystem id for ${fsName}`).to.exist;
        return cy.wrap(fs.id as number);
      });
  }

  /**
   * Same flow as roles.po / clickRowActionButton: select the row, open the
   * kebab, then click the overflow option. Overflow menus attach to document
   * body. Do not use [aria-label] on a bare button — Carbon's inactive
   * batch-actions bar reuses those labels with pointer-events: none.
   */
  private clickRowOverflowAction(table: string, rowText: string, action: string) {
    cy.get(table).within(() => {
      cy.get('table[cdstable] tbody').should('exist');
      cy.contains('Loading').should('not.exist');
      cy.get('.cds--search-input').first().clear({ force: true }).type(rowText, { delay: 0 });
      cy.contains('[cdstablerow]', rowText).should('be.visible');
      cy.contains('[cdstablerow]', rowText).find('[cdstabledata]:nth-child(2)').click();
      cy.contains('[cdstablerow]', rowText)
        .find('[data-testid="table-action-btn"]')
        .should('exist')
        .click();
    });
    cy.get(`cds-overflow-menu-option[aria-label="${action}"]`)
      .filter(':visible')
      .should('exist')
      .click();
  }

  private clickSidePanelTab(heading: string) {
    cy.contains('cds-tab-headers button[role="tab"]', heading).click();
  }

  private interceptAddPathApis() {
    cy.intercept('POST', '**/api/cephfs/mirror/directory').as('addMirrorPath');
    cy.intercept('POST', '**/api/cephfs/snapshot/schedule').as('createSchedule');
  }

  private openMirrorPathsPage(fsName: string) {
    cy.intercept('GET', '**/api/cephfs/snapshot/schedule/**').as('pathSchedule');
    cy.visit(`#/cephfs/mirroring/${fsName}/mirror-paths`);
    cy.get('cd-cephfs-mirroring-fs-mirror-paths').should('exist');
  }

  private openPathSidePanel(path: string) {
    cy.contains('cd-cephfs-mirroring-fs-mirror-paths a', path).click();
    cy.get('cd-side-panel').should('be.visible');
  }

  private completeHourlyScheduleAndSubmit(paths: string[], fsName?: string) {
    cy.get('cd-mirroring-paths-step form').should('have.class', 'ng-valid');
    this.clickTearsheetNext();

    cy.get('cd-cephfs-snapshotschedule-form').should('be.visible');
    cy.get('cd-cephfs-snapshotschedule-form cds-loading').should('not.exist');
    cy.get(
      'cd-cephfs-snapshotschedule-form cds-select[id=repeatFrequency] option[value="h"]'
    ).should('exist');
    cy.get('cd-cephfs-snapshotschedule-form cds-select[id=repeatFrequency] select').select('h', {
      force: true
    });
    cy.get('cd-cephfs-snapshotschedule-form form').should('have.class', 'ng-valid');
    this.clickTearsheetNext();

    cy.get('cd-mirroring-review-step').should('be.visible');
    paths.forEach((path) => {
      cy.get('cd-mirroring-review-step li').should('contain', path);
    });
    cy.get('cd-mirroring-review-step').should('contain', '1 hour');
    if (paths.length > 1) {
      cy.contains('cd-mirroring-review-step p', 'Total paths selected')
        .parent()
        .should('contain', String(paths.length));
    }
    cy.contains('cds-modal-footer button', 'Add mirror path').click();

    paths.forEach(() => {
      cy.wait('@addMirrorPath').its('response.statusCode').should('be.oneOf', [200, 201, 202]);
    });
    paths.forEach(() => {
      cy.wait('@createSchedule').its('response.statusCode').should('be.oneOf', [200, 201, 202]);
    });

    const toast =
      paths.length === 1
        ? `Mirroring path '${paths[0]}' added`
        : `Added ${paths.length} mirroring paths to ${fsName}`;
    cy.contains('cds-toast', toast).should('be.visible');
    cy.get('cd-cephfs-add-mirroring-path').should('not.exist');
  }

  private clickTearsheetNext() {
    cy.contains('cds-modal-footer button', 'Next').should('be.visible').click();
  }
}
