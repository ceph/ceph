import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/block/mirroring', id: 'cd-mirroring' }
};

export class MirroringPageHelper extends PageHelper {
  pages = pages;

  poolsColumnIndex = {
    name: 1,
    health: 6
  };

  /**
   * Goes to the mirroring page and edits a pool in the Pool table. Clicks on the
   * pool and chooses an option (either pool, image, or disabled)
   */
  @PageHelper.restrictTo(pages.index.url)
  editMirror(name: string, option: string) {
    // Clicks the pool in the table
    this.clickRowActionButton(name, 'edit-mode');

    // Clicks the drop down in the edit pop-up, then clicks the Update button
    cy.get('cds-modal').should('be.visible');
    this.selectOption('mirrorMode', option);

    // Clicks update button and checks if the mode has been changed
    cy.contains('button', 'Update').click();
    cy.contains('cds-modal').should('not.exist');
    const val = option.toLowerCase(); // used since entries in table are lower case
    this.getFirstTableCell(val).should('be.visible');
  }

  @PageHelper.restrictTo(pages.index.url)
  generateToken(poolName: string) {
    cy.get('[aria-label="Create Bootstrap Token"]').click();
    cy.get('cd-bootstrap-create-modal').within(() => {
      cy.get(`input[name=${poolName}]`).click({ force: true });
      cy.get('button[type=submit]').click();
      cy.get('textarea[id=token]').wait(200).invoke('val').as('token');
      cy.get('[aria-label="Back"]').click();
    });
  }

  @PageHelper.restrictTo(pages.index.url)
  checkPoolHealthStatus(poolName: string, status: string) {
    cy.get('cd-mirroring-pools').within(() => {
      this.getTableCell(this.poolsColumnIndex.name, poolName)
        .parent()
        .find(`[cdstabledata]:nth-child(${this.poolsColumnIndex.health}) .badge`)
        .should(($ele) => {
          const newLabels = $ele.toArray().map((v) => v.innerText);
          expect(newLabels).to.include(status);
        });
    });
  }
}
