import { $, browser, by, element } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

export class MirroringPageHelper extends PageHelper {
  pages = { index: '/#/block/mirroring' };

  // Goes to the mirroring page and edits a pool in the Pool table. Clicks on the
  // pool and chooses a option (either pool, image, or disabled)
  editMirror(name, option) {
    this.navigateTo();
    // Clicks the pool in the table
    browser
      .wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), Helper.TIMEOUT)
      .then(() => {
        this.getTableCell(name).click();
      });

    // Clicks the Edit Mode button
    const editModeButton = element(by.cssContainingText('button', 'Edit Mode'));
    browser.wait(Helper.EC.elementToBeClickable(editModeButton), Helper.TIMEOUT).then(() => {
      editModeButton.click();
    });
    // Clicks the drop down in the edit pop-up, then clicks the Update button
    browser.wait(Helper.EC.visibilityOf($('.modal-content')), Helper.TIMEOUT).then(() => {
      const mirrorDrop = element(by.id('mirrorMode'));
      this.moveClick(mirrorDrop);
      element(by.cssContainingText('select[name=mirrorMode] option', option)).click();
    });
    // Clicks update button and checks if the mode has been changed
    element(by.cssContainingText('button', 'Update'))
      .click()
      .then(() => {
        browser
          .wait(
            Helper.EC.stalenessOf(
              element(by.cssContainingText('.modal-dialog', 'Edit pool mirror mode'))
            ),
            Helper.TIMEOUT
          )
          .then(() => {
            const val = option.toLowerCase(); // used since entries in table are lower case
            browser
              .wait(Helper.EC.visibilityOf(this.getTableCell(val)), Helper.TIMEOUT)
              .then(() => {});
          });
      });
  }
}
