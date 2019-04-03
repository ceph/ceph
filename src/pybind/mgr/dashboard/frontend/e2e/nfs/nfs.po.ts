import { browser } from 'protractor';

export class NfsPage {
  navigateTo() {
    return browser.get('/#/nfs');
  }
}
