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
    // Create-cluster wizard shows cd-osd-list first; standalone create already
    // has cd-osd-form mounted.
    cy.get('body').then(($body) => {
      if ($body.find('cd-osd-form').length === 0) {
        cy.get('[aria-label=Create]').first().click();
      }
    });
    cy.get('cd-osd-form').should('exist');

    // Select "Manual selection" and advance to the device-selection step.
    cy.get('cd-osd-form').within(() => {
      cy.get('cds-radio[value="manual"] input[type="radio"]').click({ force: true });
      cy.contains('button', 'Next').click();
    });

    // Inline filters emit the drive group into the cluster wizard automatically.
    cy.get('cd-osd-devices-selection-groups[name="Primary"]').within(() => {
      cy.get('cds-select[data-testid="device-filter-human_readable_type"] select').select(
        deviceType,
        { force: true }
      );
      if (hostname) {
        cy.get('cds-select[data-testid="device-filter-hostname"] select').then(($sel) => {
          if ($sel.find(`option[value="${hostname}"]`).length > 0) {
            cy.wrap($sel).select(hostname, { force: true });
          }
        });
      }
      cy.get('.selection-summary', { timeout: 15000 }).should('exist');
      this.getTableCount('total').as('newOSDCount');
      if (expandCluster) {
        this.getTableCount('total').should('be.gte', 1);
      }
    });

    if (expandCluster) {
      // Nested OSD tearsheet also has a "Review" step. Close it so the outer
      // create-cluster wizard can navigate to Review and submit storage.
      // Drive group + osdDevices are already stored on the parent step/service.
      cy.get('cd-osd-form .tearsheet-footer-cancel').click({ force: true });
      cy.get('cd-osd-form').should('not.exist');
      cy.get('cd-osd-list').should('exist');
    } else {
      // Manual mode steps: Data -> DB/WAL -> Review -> Create OSDs
      cy.get('cd-osd-form').within(() => {
        cy.contains('button', 'Next').click();
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
