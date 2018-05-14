import { browser } from 'protractor';

export class OSDsPage {
  navigateTo() {
    return browser.get('/#/osd');
  }
}
