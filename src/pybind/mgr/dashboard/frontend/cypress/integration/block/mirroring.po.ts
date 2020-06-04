import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/block/mirroring', id: 'cd-mirroring' }
};

export class MirroringPageHelper extends PageHelper {
  pages = pages;

  /**
   * Goes to the mirroring page and edits a pool in the Pool table. Clicks on the
   * pool and chooses an option (either pool, image, or disabled)
   */
  @PageHelper.restrictTo(pages.index.url)
  editMirror(name: string, option: string) {
    // Clicks the pool in the table
    this.getFirstTableCell(name).click();

    // Clicks the Edit Mode button
    cy.contains('button', 'Edit Mode').click();

    // Clicks the drop down in the edit pop-up, then clicks the Update button
    cy.get('.modal-content').should('be.visible');
    this.selectOption('mirrorMode', option);

    // Clicks update button and checks if the mode has been changed
    cy.contains('button', 'Update').click();
    cy.contains('.modal-dialog', 'Edit pool mirror mode').should('not.exist');
    const val = option.toLowerCase(); // used since entries in table are lower case
    this.getFirstTableCell(val).should('be.visible');
  }
}
