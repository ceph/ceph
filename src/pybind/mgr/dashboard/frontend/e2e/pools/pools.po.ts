import { browser } from 'protractor';

export class PoolsPage {
  navigateTo() {
    return browser.get('/#/pool');
  }
}
