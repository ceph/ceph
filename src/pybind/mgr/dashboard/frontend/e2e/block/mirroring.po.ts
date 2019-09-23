import { $, by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

const pages = { index: '/#/block/mirroring' };

export class MirroringPageHelper extends PageHelper {
  pages = pages;

  /**
   * Goes to the mirroring page and edits a pool in the Pool table. Clicks on the
   * pool and chooses an option (either pool, image, or disabled)
   */
  @PageHelper.restrictTo(pages.index)
  async editMirror(name, option) {
    // Clicks the pool in the table
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name));

    // Clicks the Edit Mode button
    const editModeButton = element(by.cssContainingText('button', 'Edit Mode'));
    await this.waitClickableAndClick(editModeButton);
    // Clicks the drop down in the edit pop-up, then clicks the Update button
    await this.waitVisibility($('.modal-content'));
    await element(by.id('mirrorMode')).click(); // Mode select box
    await element(by.cssContainingText('select[name=mirrorMode] option', option)).click();

    // Clicks update button and checks if the mode has been changed
    await element(by.cssContainingText('button', 'Update')).click();
    await this.waitStaleness(
      element(by.cssContainingText('.modal-dialog', 'Edit pool mirror mode'))
    );
    const val = option.toLowerCase(); // used since entries in table are lower case
    await this.waitVisibility(this.getFirstTableCellWithText(val));
  }
}
