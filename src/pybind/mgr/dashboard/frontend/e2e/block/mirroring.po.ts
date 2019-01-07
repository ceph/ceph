import { browser } from 'protractor';

export class MirroringPage {
  navigateTo() {
    return browser.get('/#/block/mirroring');
  }
}
