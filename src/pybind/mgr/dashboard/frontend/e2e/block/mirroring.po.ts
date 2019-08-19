import { $, browser, by, element } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

export class MirroringPageHelper extends PageHelper {
  pages = { index: '/#/block/mirroring' };

  // Goes to the mirroring page and edits a pool in the Pool table. Clicks on the
  // pool and chooses a option (either pool, image, or disabled)
  async editMirror(name, option) {
    // Clicks the pool in the table
    await browser.wait(
      Helper.EC.elementToBeClickable(this.getFirstTableCellWithText(name)),
      Helper.TIMEOUT
    );
    await this.getFirstTableCellWithText(name).click();

    // Clicks the Edit Mode button
    const editModeButton = element(by.cssContainingText('button', 'Edit Mode'));
    await browser.wait(Helper.EC.elementToBeClickable(editModeButton), Helper.TIMEOUT);
    await editModeButton.click();
    // Clicks the drop down in the edit pop-up, then clicks the Update button
    await browser.wait(Helper.EC.visibilityOf($('.modal-content')), Helper.TIMEOUT);
    await element(by.id('mirrorMode')).click(); // Mode select box
    await element(by.cssContainingText('select[name=mirrorMode] option', option)).click();

    // Clicks update button and checks if the mode has been changed
    await element(by.cssContainingText('button', 'Update')).click();
    await browser.wait(
      Helper.EC.stalenessOf(
        element(by.cssContainingText('.modal-dialog', 'Edit pool mirror mode'))
      ),
      Helper.TIMEOUT
    );
    const val = option.toLowerCase(); // used since entries in table are lower case
    await browser.wait(Helper.EC.visibilityOf(this.getFirstTableCellWithText(val)), Helper.TIMEOUT);
  }
}
