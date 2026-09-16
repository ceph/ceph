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
   * Create a volume through the API. Used as bootstrap setup so cy.origin
   * only has to drive the generate-token UI. Idempotent for Cypress retries.
   */
  createFilesystemApi(fsName: string, clusterUrl = '') {
    const prefix = this.clusterUrlPrefix(clusterUrl);
    cy.request({
      method: 'GET',
      url: `${prefix}api/cephfs`,
      headers: this.apiHeaders()
    }).then((resp) => {
      const filesystems = Array.isArray(resp.body) ? resp.body : [];
      const exists = filesystems.some(
        (item: { mdsmap?: { fs_name?: string } }) => item.mdsmap?.fs_name === fsName
      );
      if (!exists) {
        cy.request({
          method: 'POST',
          url: `${prefix}api/cephfs`,
          headers: this.apiHeaders(),
          failOnStatusCode: false,
          body: {
            name: fsName,
            service_spec: { placement: {}, unmanaged: true }
          }
        });
      }
      this.waitForFilesystem(fsName, prefix);
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

  listMirroredPaths(fsName: string) {
    return cy
      .request({
        method: 'GET',
        url: `api/cephfs/mirror/directory/${fsName}`,
        headers: this.apiHeaders(),
        failOnStatusCode: false
      })
      .then((resp) => {
        const paths = Array.isArray(resp.body) ? (resp.body as string[]) : [];
        return cy.wrap(paths);
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

  enableSnapScheduleModule() {
    cy.request({
      method: 'GET',
      url: 'api/mgr/module',
      headers: this.apiHeaders()
    }).then((resp) => {
      const enabled = (Array.isArray(resp.body) ? resp.body : []).some(
        (mod: { name?: string; enabled?: boolean }) => mod.name === 'snap_schedule' && mod.enabled
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
      this.directoryExists(fsId, path).then((exists) => {
        if (exists) {
          cy.log(`directory ${path} already exists`);
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
            if ([200, 201, 202, 204].includes(resp.status)) {
              this.waitForDirectory(fsId, path);
              return;
            }
            this.directoryExists(fsId, path).then((created) => {
              if (created) {
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
    this.subvolumeGroupExists(fsName, groupName).then((exists) => {
      if (exists) {
        cy.log(`subvolume group ${groupName} already exists`);
        return;
      }
      cy.request({
        method: 'POST',
        url: 'api/cephfs/subvolume/group',
        body: { vol_name: fsName, group_name: groupName },
        headers: this.apiHeaders(),
        failOnStatusCode: false
      }).then(() => {
        this.subvolumeGroupExists(fsName, groupName).then((created) => {
          expect(created, `subvolume group ${groupName}`).to.eq(true);
        });
      });
    });
  }

  createSubvolume(fsName: string, subvolName: string, groupName: string) {
    this.subvolumeExists(fsName, subvolName, groupName).then((exists) => {
      if (exists) {
        cy.log(`subvolume ${subvolName} already exists`);
        return;
      }
      cy.request({
        method: 'POST',
        url: 'api/cephfs/subvolume',
        body: {
          vol_name: fsName,
          subvol_name: subvolName,
          group_name: groupName
        },
        headers: this.apiHeaders(),
        failOnStatusCode: false
      }).then(() => {
        this.subvolumeExists(fsName, subvolName, groupName).then((created) => {
          expect(created, `subvolume ${subvolName}`).to.eq(true);
        });
      });
    });
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
    cy.intercept('GET', '**/ui-api/cephfs/*/ls_dir*').as('lsDir');
    this.navigateTo();
    cy.hash().should('eq', '#/cephfs/mirroring');
    this.selectMirroredFilesystemRow(fsName);
    this.clickRowKebabAction('cd-cephfs-mirroring-list', fsName, 'Add mirror path');
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
        .find('option')
        .should(($opts) => {
          const values = [...$opts].map((opt) => (opt as HTMLOptionElement).value);
          expect(values, `wizard path options at level ${level}`).to.include(segment);
        });
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
    this.listMirroredPaths(fsName).then((paths) => {
      if (!paths.includes(path)) {
        cy.log(`mirror path ${path} already removed`);
        this.openMirrorPathsPage(fsName);
        cy.contains('cd-cephfs-mirroring-fs-mirror-paths [cdstablerow]', path).should('not.exist');
        return;
      }
      cy.intercept('DELETE', '**/api/cephfs/mirror/directory*').as('removeMirrorPath');
      cy.intercept('GET', '**/api/cephfs/mirror/*/status*').as('mirrorStatus');
      cy.visit(`#/cephfs/mirroring/${fsName}/mirror-paths`);
      cy.get('cd-cephfs-mirroring-fs-mirror-paths').should('exist');
      cy.wait('@mirrorStatus');
      this.clickRowOverflowAction('cd-cephfs-mirroring-fs-mirror-paths', path, 'Remove path');
      cy.get('cds-modal [aria-label="confirmation"]').should('exist').click({ force: true });
      cy.contains('cds-modal button', 'Remove mirror path')
        .should('not.be.disabled')
        .click({ force: true });
      cy.wait('@removeMirrorPath')
        .its('response.statusCode')
        .should('be.oneOf', [200, 201, 202, 204]);
      cy.get('cds-modal [aria-label="confirmation"]').should('not.exist');
      cy.contains('cds-toast', `mirror path '${path}'`).should('be.visible');
      cy.contains('cd-cephfs-mirroring-fs-mirror-paths [cdstablerow]', path).should('not.exist');
    });
  }

  disableMirroring(fsName: string) {
    this.isFilesystemMirrored(fsName).then((mirrored) => {
      if (!mirrored) {
        cy.log(`mirroring for ${fsName} already disabled`);
        this.navigateTo();
        cy.contains('[cdstablerow] [cdstabledata]', fsName).should('not.exist');
        return;
      }
      cy.intercept('POST', '**/api/cephfs/mirror/disable').as('disableMirror');
      cy.intercept('GET', '**/api/cephfs/mirror/daemon/status').as('daemonStatus');
      this.navigateTo();
      cy.wait('@daemonStatus');
      this.selectMirroredFilesystemRow(fsName);
      this.clickRowOverflowAction('cd-cephfs-mirroring-list', fsName, 'Disable mirroring');
      cy.get('cds-modal input#resource_name').should('exist').type(fsName, { force: true });
      cy.contains('cds-modal button', 'Disable mirroring')
        .should('not.be.disabled')
        .click({ force: true });
      cy.wait('@disableMirror').its('response.statusCode').should('be.oneOf', [200, 201, 202]);
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

  private subvolumeGroupExists(fsName: string, groupName: string) {
    return cy
      .request({
        method: 'GET',
        url: `api/cephfs/subvolume/group/${fsName}/info`,
        qs: { group_name: groupName },
        headers: this.apiHeaders(),
        failOnStatusCode: false
      })
      .then((resp) => cy.wrap(resp.status === 200));
  }

  private subvolumeExists(fsName: string, subvolName: string, groupName: string) {
    return cy
      .request({
        method: 'GET',
        url: `api/cephfs/subvolume/${fsName}/info`,
        qs: { subvol_name: subvolName, group_name: groupName },
        headers: this.apiHeaders(),
        failOnStatusCode: false
      })
      .then((resp) => cy.wrap(resp.status === 200));
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

  private waitForFilesystem(fsName: string, prefix: string) {
    const deadline = Date.now() + 60000;
    const poll = () => {
      cy.request({
        method: 'GET',
        url: `${prefix}api/cephfs`,
        headers: this.apiHeaders()
      }).then((resp) => {
        const found = (Array.isArray(resp.body) ? resp.body : []).some(
          (item: { mdsmap?: { fs_name?: string } }) => item.mdsmap?.fs_name === fsName
        );
        if (!found) {
          expect(Date.now(), `filesystem ${fsName} did not appear`).to.be.lessThan(deadline);
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
      cy.get('.cds--search-input').first().clear({ force: true }).type(fsName, { delay: 0 });
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
   * Open the row kebab once, then click the Carbon overflow item.
   * Do not re-click the kebab while waiting: that toggles the menu closed.
   * Options are portaled to document.body as .cds--overflow-menu-options__btn.
   */
  private clickRowKebabAction(table: string, rowText: string, action: string) {
    cy.contains(`${table} [cdstablerow]`, rowText)
      .find('[data-testid="table-action-btn"]')
      .then(($menu) => {
        const $trigger = $menu.find('button').first();
        cy.wrap($trigger.length ? $trigger : $menu).click({ force: true });
      });
    cy.get('.cds--overflow-menu-options', { timeout: 10000 }).should('exist');
    cy.contains('.cds--overflow-menu-options__btn', action).click({ force: true });
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
      cy.get('.cds--search-input').first().clear({ force: true }).type(rowText, { delay: 0 });
      cy.contains('[cdstablerow]', rowText).should('be.visible');
    });
    this.clickRowKebabAction(table, rowText, action);
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
