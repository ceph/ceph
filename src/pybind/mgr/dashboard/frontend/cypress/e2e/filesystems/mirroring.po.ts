import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/cephfs/mirroring', id: 'cd-cephfs-mirroring-list' },
  fsCreate: { url: '#/cephfs/fs/create', id: 'cd-cephfs-form' },
  fsList: { url: '#/cephfs/fs', id: 'cd-cephfs-list' }
};

export class CephfsMirroringPageHelper extends PageHelper {
  pages = pages;

  /**
   * Create a volume from the Filesystems form. Idempotent for Cypress retries.
   * Pass clusterOrigin to run the same UI on the secondary cluster (cy.origin).
   */
  createFilesystem(fsName: string, clusterOrigin = '') {
    if (clusterOrigin) {
      cy.origin(clusterOrigin, { args: { fsName } }, ({ fsName }) => {
        cy.visit('/');
        cy.visit('#/cephfs/fs');
        cy.get('cd-login, cd-cephfs-list').should('exist');
        cy.get('body').then(($body) => {
          if ($body.find('cd-login').length) {
            cy.get('#username').type('admin', { delay: 0 });
            cy.get('#password').type('admin', { delay: 0 });
            cy.get('[type=submit]').click();
            cy.get('cd-login').should('not.exist');
            cy.visit('#/cephfs/fs');
          }
        });
        cy.get('cd-cephfs-list').should('exist');
        cy.get('table[cdstable] tbody').should('exist');
        cy.contains('Loading').should('not.exist');
        cy.get('cd-cephfs-list').then(($list) => {
          if ($list.find('[cdstablerow]').text().includes(fsName)) {
            return;
          }
          cy.visit('#/cephfs/fs/create');
          cy.get('cd-cephfs-form').should('exist');
          cy.get('#name').clear().type(fsName);
          cy.get('cd-submit-button').click();
          cy.get('cd-cephfs-list').should('exist');
          cy.contains('[cdstablerow]', fsName).should('be.visible');
        });
      });
      return;
    }

    this.navigateTo('fsList');
    cy.get('cd-cephfs-list').should('exist');
    cy.get('table[cdstable] tbody').should('exist');
    cy.contains('Loading').should('not.exist');
    cy.get('cd-cephfs-list').then(($list) => {
      if ($list.find('[cdstablerow]').text().includes(fsName)) {
        cy.log(`filesystem ${fsName} already exists`);
        return;
      }
      this.navigateTo('fsCreate');
      cy.get('#name').clear().type(fsName);
      cy.get('cd-submit-button').click();
      cy.get('cd-cephfs-list').should('exist');
      this.existTableCell(fsName, true);
    });
  }

  /**
   * True when the filesystem already has a peer in daemon status (or
   * mirror_info). Used to skip bootstrap when e2e_mirror is already set up.
   */
  isFilesystemMirrored(fsName: string, clusterUrl = '') {
    const prefix = this.clusterUrlPrefix(clusterUrl);
    return cy
      .request({
        method: 'GET',
        url: `${prefix}api/cephfs/mirror/daemon/status`,
        headers: this.apiHeaders(),
        failOnStatusCode: false
      })
      .then((daemonResp) => {
        const daemons = Array.isArray(daemonResp.body) ? daemonResp.body : [];
        const fromDaemon = daemons.some(
          (daemon: { filesystems?: Array<{ name?: string; peers?: unknown[] }> }) =>
            (daemon.filesystems || []).some(
              (fs) => fs.name === fsName && Array.isArray(fs.peers) && fs.peers.length > 0
            )
        );
        if (fromDaemon) {
          return cy.wrap(true);
        }
        return cy
          .request({
            method: 'GET',
            url: `${prefix}api/cephfs`,
            headers: this.apiHeaders(),
            failOnStatusCode: false
          })
          .then((fsResp) => {
            const filesystems = Array.isArray(fsResp.body) ? fsResp.body : [];
            const fromFs = filesystems.some(
              (item: {
                mdsmap?: { fs_name?: string };
                mirror_info?: { peers?: Record<string, unknown> };
                cephfs?: { mirror_info?: { peers?: Record<string, unknown> } };
              }) => {
                if (item.mdsmap?.fs_name !== fsName) {
                  return false;
                }
                const peers = item.mirror_info?.peers ?? item.cephfs?.mirror_info?.peers;
                return !!peers && Object.keys(peers).length > 0;
              }
            );
            return cy.wrap(fromFs);
          });
      });
  }

  /**
   * After cy.origin the AUT iframe is on the secondary cluster. Hash-only
   * cy.visit() stays there and the global afterEach then crashes. Visit the
   * primary dashboard with an absolute URL first.
   */
  visitPrimary(hash: string = pages.index.url) {
    const baseUrl = String(Cypress.config('baseUrl') || '').replace(/\/$/, '');
    cy.visit(`${baseUrl}/${hash}`);
    cy.get(this.pages.index.id);
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

  createDirectory(fsName: string, path: string) {
    this.getFilesystemId(fsName).then((fsId) => {
      this.directoryExists(fsId, path).then((exists) => {
        if (exists) {
          cy.log(`directory ${path} already exists`);
          this.waitForDirectory(fsId, path);
          return;
        }
        const mkdir = (attemptsLeft: number) => {
          cy.request({
            method: 'POST',
            url: `api/cephfs/${fsId}/tree`,
            qs: { path },
            body: { path },
            headers: this.apiHeaders(),
            failOnStatusCode: false
          }).then((resp) => {
            this.directoryExists(fsId, path).then((created) => {
              if (created || [200, 201, 202, 204].includes(resp.status)) {
                this.waitForDirectory(fsId, path);
                return;
              }
              if (attemptsLeft > 0) {
                cy.wait(2000).then(() => mkdir(attemptsLeft - 1));
                return;
              }
              expect(resp.status, `mkdir ${path}`).to.be.oneOf([200, 201, 202, 204]);
            });
          });
        };
        mkdir(10);
      });
    });
  }

  createSubvolumeGroup(fsName: string, groupName: string) {
    this.openFilesystemExpandedTab(fsName, 'Subvolume groups');
    this.filesystemExpandedRow().within(() => {
      cy.get('cd-cephfs-subvolume-group').should('exist');
      cy.get('table[cdstable] tbody').should('exist');
      cy.contains('Loading').should('not.exist');
    });
    this.filesystemExpandedRow().then(($detail) => {
      if ($detail.find('[cdstablerow]').text().includes(groupName)) {
        cy.log(`subvolume group ${groupName} already exists`);
        return;
      }
      this.filesystemExpandedRow()
        .find('[data-testid="primary-action"][aria-label="Create"]')
        .click();
      cy.get('cds-modal').should('be.visible');
      cy.get('cds-modal input#subvolumegroupName').should('be.visible').clear().type(groupName);
      cy.get('cds-modal [data-testid=submitBtn]').should('not.be.disabled').click();
      cy.get('cds-modal').should('not.exist');
      this.filesystemExpandedRow().within(() => {
        this.typeTableSearch(groupName);
        cy.contains('[cdstablerow]', groupName).should('be.visible');
      });
    });
  }

  createSubvolume(fsName: string, subvolName: string, groupName: string) {
    this.openFilesystemExpandedTab(fsName, 'Subvolumes');
    this.filesystemExpandedRow().within(() => {
      cy.get('cd-cephfs-subvolume-list').should('exist');
      cy.get('table[cdstable] tbody').should('exist');
      cy.contains('Loading').should('not.exist');
      cy.contains('cd-vertical-navigation a.nav-link', groupName).should('be.visible').click();
      cy.contains('Loading').should('not.exist');
    });
    this.filesystemExpandedRow().then(($detail) => {
      if ($detail.find('[cdstablerow]').text().includes(subvolName)) {
        cy.log(`subvolume ${subvolName} already exists`);
        return;
      }
      this.filesystemExpandedRow()
        .find('[data-testid="primary-action"][aria-label="Create"]')
        .click();
      cy.get('cds-modal').should('be.visible');
      cy.get('cds-modal input#subvolumeName').should('be.visible').clear().type(subvolName);
      cy.get(`cds-modal cds-select[id=subvolumeGroupName] option[value="${groupName}"]`).should(
        'exist'
      );
      cy.get('cds-modal cds-select[id=subvolumeGroupName] select').select(groupName, {
        force: true
      });
      cy.get('cds-modal [data-testid=submitBtn]').should('not.be.disabled').click();
      cy.get('cds-modal').should('not.exist');
      this.filesystemExpandedRow().within(() => {
        cy.contains('cd-vertical-navigation a.nav-link', groupName).click();
        this.typeTableSearch(subvolName);
        cy.contains('[cdstablerow]', subvolName).should('be.visible');
      });
    });
  }

  openAddMirrorPath(fsName: string) {
    cy.visit(`#/cephfs/mirroring/${fsName}/mirror-paths`);
    cy.get('cd-cephfs-mirroring-fs-mirror-paths').should('exist');
    cy.get('[data-testid="primary-action"][aria-label="Add mirror path"]').click();
    cy.get('cd-cephfs-add-mirroring-path').should('be.visible');
    this.waitForPathSelectReady(0);
  }

  openAddMirrorPathFromList(fsName: string) {
    this.navigateTo();
    cy.hash().should('eq', '#/cephfs/mirroring');
    this.selectMirroredFilesystemRow(fsName);
    this.clickRowKebabAction('cd-cephfs-mirroring-list', fsName, 'Add mirror path');
    cy.get('cd-cephfs-add-mirroring-path').should('be.visible');
    this.waitForPathSelectReady(0);
  }

  selectWizardPath(tileIndex: number, segments: string[]) {
    segments.forEach((segment, level) => {
      this.waitForPathSelectReady(tileIndex);
      const pathSelect = () =>
        this.wizardPathEntry(tileIndex).find('[data-testid="path-level-select"]').eq(level);
      // Carbon cds-select: wait on the native option, then select the inner <select>.
      pathSelect().find(`option[value="${segment}"]`).should('exist');
      pathSelect().find('select').select(segment, { force: true });
    });
    this.waitForPathSelectReady(tileIndex);
    this.wizardPathEntry(tileIndex).should('contain', `/${segments.join('/')}`);
  }

  addAnotherWizardPath() {
    cy.contains('cd-mirroring-paths-step button', 'Add another path')
      .should('not.be.disabled')
      .click();
    cy.get('[data-testid="mirroring-path-entry"]').should('have.length.at.least', 2);
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
    cy.get('cd-side-panel').within(() => {
      this.clickSidePanelTab('Schedule policy');
      cy.root().should('not.contain', 'Loading schedule policies');
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

  /**
   * Already-mirrored dirs are hidden from the wizard dropdown. Remove via
   * the table when the row is there. The table is daemon metrics and can
   * omit a path that snapshot_mirror_ls still tracks, so untrack that case
   * the same way the Remove path action does.
   */
  ensureMirrorPathAbsent(fsName: string, path: string) {
    this.openMirrorPathsPage(fsName);
    cy.get('cd-cephfs-mirroring-fs-mirror-paths').then(($page) => {
      if ($page.find('[cdstablerow]').text().includes(path)) {
        this.removeVisibleMirrorPath(path);
        return;
      }
      cy.request({
        method: 'DELETE',
        url: 'api/cephfs/mirror/directory',
        qs: { fs_name: fsName, path },
        headers: this.apiHeaders(),
        failOnStatusCode: false
      });
    });
  }

  removeMirrorPath(fsName: string, path: string) {
    this.openMirrorPathsPage(fsName);
    this.removeVisibleMirrorPath(path);
  }

  disableMirroring(fsName: string) {
    this.isFilesystemMirrored(fsName).then((mirrored) => {
      if (!mirrored) {
        cy.log(`mirroring for ${fsName} already disabled`);
        this.navigateTo();
        cy.contains('[cdstablerow] [cdstabledata]', fsName).should('not.exist');
        return;
      }
      this.navigateTo();
      this.selectMirroredFilesystemRow(fsName);
      this.clickRowOverflowAction('cd-cephfs-mirroring-list', fsName, 'Disable mirroring');
      cy.get('cds-modal input#resource_name').should('exist').type(fsName, { force: true });
      cy.contains('cds-modal button', 'Disable mirroring')
        .should('not.be.disabled')
        .click({ force: true });
      cy.get('cds-modal input#resource_name').should('not.exist');
      cy.contains('cds-toast', `mirroring for '${fsName}'`).should('be.visible');
      cy.contains('[cdstablerow] [cdstabledata]', fsName).should('not.exist');
    });
  }

  private apiHeaders() {
    return { Accept: 'application/vnd.ceph.api.v1.0+json' };
  }

  private clusterUrlPrefix(clusterUrl: string) {
    return clusterUrl ? `${String(clusterUrl).replace(/\/$/, '')}/` : '';
  }

  private directoryLsNames(fsId: number) {
    return cy
      .request({
        method: 'GET',
        url: `ui-api/cephfs/${fsId}/ls_dir`,
        qs: { depth: 1, path: '/' },
        headers: this.apiHeaders(),
        failOnStatusCode: false
      })
      .then((resp) => {
        const names = Array.isArray(resp.body)
          ? resp.body.map((dir: { name?: string }) => dir.name)
          : [];
        return cy.wrap(names);
      });
  }

  private directoryExists(fsId: number, path: string) {
    const dirName = path.replace(/^\//, '').split('/')[0];
    return this.directoryLsNames(fsId).then((names) => cy.wrap(names.includes(dirName)));
  }

  private openFilesystemExpandedTab(fsName: string, tabName: string) {
    this.navigateTo('fsList');
    cy.get('cd-cephfs-list').should('exist');
    cy.contains('cd-cephfs-list cds-tab-headers button[role="tab"]', 'File systems').click({
      force: true
    });
    cy.get('cd-cephfs-list').within(() => {
      cy.get('table[cdstable] tbody').should('exist');
      cy.contains('Loading').should('not.exist');
    });
    cy.get('cd-cephfs-list').then(($list) => {
      if ($list.find('[data-testid="datatable-row-detail"]').length) {
        return;
      }
      this.getExpandCollapseElement(fsName).click();
    });
    cy.get('[data-testid="datatable-row-detail"]').should('be.visible');
    this.filesystemExpandedRow().within(() => {
      this.getTab(tabName).click();
    });
  }

  private filesystemExpandedRow() {
    return cy.get('[data-testid="datatable-row-detail"]');
  }

  /** aria-label sits on cds-table-toolbar-search; Cypress can only clear/type the inner input. */
  private typeTableSearch(text: string) {
    cy.get('[aria-label="search"]')
      .first()
      .find('input')
      .clear({ force: true })
      .type(text, { delay: 0 });
  }

  private waitForDirectory(fsId: number, path: string) {
    const dirName = path.replace(/^\//, '').split('/')[0];
    const deadline = Date.now() + 60000;
    const poll = () => {
      this.directoryLsNames(fsId).then((names) => {
        if (!names.includes(dirName)) {
          expect(Date.now(), `directory ${path} in ls_dir`).to.be.lessThan(deadline);
          cy.wait(2000).then(() => poll());
        }
      });
    };
    poll();
  }

  private getFilesystemId(fsName: string) {
    return cy
      .request({
        method: 'GET',
        url: 'api/cephfs',
        headers: this.apiHeaders()
      })
      .then((resp) => {
        const fs = (Array.isArray(resp.body) ? resp.body : []).find(
          (item: { id?: number; mdsmap?: { fs_name?: string } }) => item.mdsmap?.fs_name === fsName
        );
        expect(fs?.id, `filesystem id for ${fsName}`).to.exist;
        return cy.wrap(fs.id as number);
      });
  }

  /**
   * Select a mirrored FS row without following the filesystem redirect
   * (that cell is a routerLink to overview). Nested [cdstabledata] matches
   * inside that cell, so index-based clicks can navigate away and the
   * list toolbar disappears.
   */
  private selectMirroredFilesystemRow(fsName: string) {
    cy.get('cd-cephfs-mirroring-list').within(() => {
      cy.get('table[cdstable] tbody').should('exist');
      cy.contains('Loading').should('not.exist');
      this.typeTableSearch(fsName);
      cy.contains('[cdstablerow]', fsName).should('be.visible');
    });
    cy.contains('cd-cephfs-mirroring-list [cdstablerow]', fsName).then(($row) => {
      const $safeCells = $row
        .find('td')
        .filter(
          (_, el) => !el.querySelector('a') && !el.querySelector('[data-testid="table-action-btn"]')
        );
      if ($safeCells.length) {
        cy.wrap($safeCells.first()).click({ force: true });
      } else {
        cy.wrap($row).click('left', { force: true });
      }
    });
    cy.hash().should('eq', '#/cephfs/mirroring');
    cy.get('cd-cephfs-mirroring-list').should('exist');
  }

  /**
   * Open the row kebab once, then click the overflow item.
   * Do not re-click the kebab while waiting: that toggles the menu closed.
   * Carbon portals options to document.body, so query the option host
   * (aria-label is set on cds-overflow-menu-option) rather than the row.
   */
  private clickRowKebabAction(table: string, rowText: string, action: string) {
    cy.contains(`${table} [cdstablerow]`, rowText)
      .find('[data-testid="table-action-btn"]')
      .then(($menu) => {
        const $trigger = $menu.find('button').first();
        cy.wrap($trigger.length ? $trigger : $menu).click({ force: true });
      });
    cy.get(`cds-overflow-menu-option[aria-label="${action}"]`, { timeout: 10000 })
      .should('exist')
      .click({ force: true });
  }

  /**
   * Open the row kebab and click an overflow option. Carbon teleports the
   * menu to document.body, and table auto-reload can unmount it, so reopen
   * until the option is present. Force-click: the option is often treated
   * as hidden while the overlay is opening.
   */
  private clickRowOverflowAction(table: string, rowText: string, action: string) {
    cy.get(table).within(() => {
      cy.get('table[cdstable] tbody').should('exist');
      cy.contains('Loading').should('not.exist');
      this.typeTableSearch(rowText);
      cy.contains('[cdstablerow]', rowText).should('be.visible');
    });
    this.clickRowKebabAction(table, rowText, action);
  }

  private clickSidePanelTab(heading: string) {
    cy.contains('cds-tab-headers button[role="tab"]', heading).click();
  }

  private wizardPathEntry(index: number) {
    return cy.get('[data-testid="mirroring-path-entry"]').eq(index);
  }

  private waitForPathSelectReady(index: number) {
    this.wizardPathEntry(index).within(() => {
      cy.get('[data-testid="path-level-select-skeleton"]', { timeout: 60000 }).should('not.exist');
      cy.get('[data-testid="path-level-select"]', { timeout: 60000 }).should('exist');
    });
  }

  private removeVisibleMirrorPath(path: string) {
    this.clickRowOverflowAction('cd-cephfs-mirroring-fs-mirror-paths', path, 'Remove path');
    cy.get('cds-modal [aria-label="confirmation"]').should('exist').click({ force: true });
    cy.contains('cds-modal button', 'Remove mirror path')
      .should('not.be.disabled')
      .click({ force: true });
    cy.get('cds-modal [aria-label="confirmation"]').should('not.exist');
    cy.contains('cds-toast', `mirror path '${path}'`).should('be.visible');
    cy.contains('cd-cephfs-mirroring-fs-mirror-paths [cdstablerow]', path).should('not.exist');
  }

  private openMirrorPathsPage(fsName: string) {
    cy.visit(`#/cephfs/mirroring/${fsName}/mirror-paths`);
    cy.get('cd-cephfs-mirroring-fs-mirror-paths').should('exist');
    cy.get('cd-cephfs-mirroring-fs-mirror-paths').within(() => {
      cy.get('table[cdstable] tbody').should('exist');
      cy.contains('Loading').should('not.exist');
    });
  }

  private openPathSidePanel(path: string) {
    cy.contains('cd-cephfs-mirroring-fs-mirror-paths a', path).click();
    cy.get('cd-side-panel').should('be.visible');
  }

  private completeHourlyScheduleAndSubmit(paths: string[], fsName?: string) {
    this.clickTearsheetNext();

    cy.get('cd-cephfs-snapshotschedule-form').should('be.visible');
    cy.get('cd-cephfs-snapshotschedule-form cds-loading').should('not.exist');
    cy.get(
      'cd-cephfs-snapshotschedule-form cds-select[id=repeatFrequency] option[value="h"]'
    ).should('exist');
    cy.get('cd-cephfs-snapshotschedule-form cds-select[id=repeatFrequency] select').select('h', {
      force: true
    });
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

    const toast =
      paths.length === 1
        ? `Mirroring path '${paths[0]}' added`
        : `Added ${paths.length} mirroring paths to ${fsName}`;
    cy.contains('cds-toast', toast, { timeout: 120000 }).should('be.visible');
    cy.get('cd-cephfs-add-mirroring-path').should('not.exist');
  }

  private clickTearsheetNext() {
    cy.contains('cds-modal-footer button', 'Next').should('be.visible').click();
  }
}
