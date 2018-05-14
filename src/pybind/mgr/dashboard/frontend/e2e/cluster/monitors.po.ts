import { browser } from 'protractor';

export class MonitorsPage {
  navigateTo() {
    return browser.get('/#/monitor');
  }
}
