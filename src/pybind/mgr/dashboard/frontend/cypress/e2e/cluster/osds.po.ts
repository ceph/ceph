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
    cy.get('[aria-label="toggle advanced mode"]').click();
    // Click Primary devices Add button
    cy.get('cd-osd-devices-selection-groups[name="Primary"]').as('primaryGroups');
    cy.get('@primaryGroups').find('button').click();

    // Select all devices with `deviceType`
    cy.get('cd-osd-devices-selection-modal').within(() => {
      cy.get('.modal-footer .tc_submitButton').as('addButton').should('be.disabled');
      this.filterTable('Type', deviceType);
      if (hostname) {
        this.filterTable('Hostname', hostname);
      }

      if (expandCluster) {
        this.getTableCount('total').should('be.gte', 1);
      }
      cy.get('@addButton').click();
    });

    if (!expandCluster) {
      cy.get('@primaryGroups').within(() => {
        this.getTableCount('total').as('newOSDCount');
      });

      cy.get(`${pages.create.id} .card-footer .tc_submitButton`).click();
      cy.get(`cd-osd-creation-preview-modal .modal-footer .tc_submitButton`).click();
    }
  }

  @PageHelper.restrictTo(pages.index.url)
  checkStatus(id: number, status: string[]) {
    this.searchTable(`id:${id}`);
    this.expectTableCount('found', 1);
    cy.get(`datatable-body-cell:nth-child(${this.columnIndex.status}) .badge`).should(($ele) => {
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
        $el.find('datatable-body-cell .datatable-body-cell-label').get(this.columnIndex.id - 1)
          .textContent
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
