import { browser } from 'protractor';

export class IscsiPage {
  navigateTo() {
    return browser.get('/#/block/iscsi');
  }
}
