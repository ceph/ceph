import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/osd', id: 'cd-osd-list' },
  create: { url: '#/osd/(modal:create)', id: 'cd-osd-form' }
};

export class OSDsPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    id: 3,
    status: 5
  };

  create(deviceType: 'hdd' | 'ssd', hostname?: string) {
    cy.get('cd-osd-form').within(() => {
      cy.get('cds-radio[value="manual"] input[type="radio"]').check({ force: true });
      cy.get('cds-progress-indicator').contains('Select data devices').click();

      cy.get('cd-osd-devices-selection-groups[name="Primary"]').within(() => {
        cy.get('[data-testid="device-filter-human_readable_type"] select').select(
          deviceType === 'ssd' ? 'nvme/ssd' : deviceType
        );
        if (hostname) {
          cy.get('[data-testid="device-filter-hostname"] select').select(hostname);
        }
        this.getTableCount('total').should('be.gte', 1).as('newOSDCount');
      });

      cy.get('cds-progress-indicator').contains('Review').click();
      cy.get('.tearsheet-footer-submit').click();
    });
    cy.get('cd-osd-form').should('not.exist');
  }

  @PageHelper.restrictTo(pages.index.url)
  checkStatus(id: number, status: string[]) {
    this.searchTable(id.toString());
    cy.wait(5 * 1000);
    cy.get(`[cdstablerow] [cdstabledata]:nth-child(${this.columnIndex.status}) cds-tag`).should(
      ($ele) => {
        const allStatus = $ele.toArray().map((v) => v.innerText);
        for (const s of status) {
          expect(allStatus).to.include(s);
        }
      }
    );
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
