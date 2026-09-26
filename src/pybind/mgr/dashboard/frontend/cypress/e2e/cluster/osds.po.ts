import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/osd', id: 'cd-osd-list' },
  create: { url: '#/osd/create', id: 'cd-osd-form' }
};

export class OSDsPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    id: 3,
    status: 5
  };

  create(deviceType: 'hdd' | 'ssd', hostname?: string, expandCluster = false) {
    // The Create OSDs wizard step shows cd-osd-list by default.
    // Click Create to switch to cd-osd-form.
    cy.get('[aria-label=Create]').first().click();
    cy.get('cd-osd-form').should('exist');

    // The redesigned form uses a deployment-mode radio group.
    // Select "Manual selection" to expose the per-device filter step.
    cy.get('cd-osd-form').within(() => {
      cy.get('cds-radio[value="manual"] input[type="radio"]').click({ force: true });
      // Navigate to the device-selection tearsheet step inside the OSD form.
      cy.contains('button', 'Next').click();
    });

    // Apply the device-type filter inline.
    // Changing the select triggers onFilterFieldChange → applySelectionResult →
    // selected.emit → osd-form.onDevicesSelected → emitDriveGroup.emit, so the
    // drive group is automatically stored in the cluster wizard without any
    // explicit OSD-form submit.
    cy.get('cd-osd-devices-selection-groups[name="Primary"]').within(() => {
      cy.get(
        'cds-select[data-testid="device-filter-human_readable_type"] select'
      ).select(deviceType, { force: true });
      if (hostname) {
        cy.get('cds-select[data-testid="device-filter-hostname"] select').then(($sel) => {
          if ($sel.find(`option[value="${hostname}"]`).length > 0) {
            cy.wrap($sel).select(hostname, { force: true });
          }
        });
      }
    });

    if (!expandCluster) {
      // Navigate to the review step and submit the OSD form.
      cy.get('cd-osd-form').within(() => {
        cy.contains('button', 'Next').click();
        cy.get('.tearsheet-footer-submit').click();
      });
    }
  }

  @PageHelper.restrictTo(pages.index.url)
  checkStatus(id: number, status: string[]) {
    this.searchTable(id.toString());
    cy.wait(10 * 1000);
    cy.get(`[cdstablerow] [cdstabledata]:nth-child(${this.columnIndex.status}) cds-tag`, {
      timeout: 30000
    }).should(($ele) => {
      const allStatus = $ele.toArray().map((v) => v.innerText);
      for (const s of status) {
        expect(allStatus).to.include(s);
      }
    });
  }

  @PageHelper.restrictTo(pages.index.url)
  ensureNoOsd(id: number) {
    this.searchTable(`id:${id}`);
    this.expectTableCount('found', 0);
    this.clearTableSearchInput();
  }

  @PageHelper.restrictTo(pages.index.url)
  deleteByIDs(osdIds: number[], replace?: boolean) {
    this.getTableRows().each(($el) => {
      const rowOSD = Number(
        $el.find('[cdstabledata][cdstablerow]').get(this.columnIndex.id - 1).textContent
      );
      if (osdIds.includes(rowOSD)) {
        cy.wrap($el).click();
      }
    });
    this.clickActionButton('delete');
    if (replace) {
      cy.get('cd-modal label[for="preserve"]').click();
    }
    cy.get('cd-modal label[for="confirmation"]').click();
    cy.contains('cd-modal button', 'Delete').click();
    cy.get('cd-modal').should('not.exist');
  }
}
