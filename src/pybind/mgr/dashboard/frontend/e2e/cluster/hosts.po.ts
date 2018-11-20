import { browser } from 'protractor';

export class HostsPage {
  navigateTo() {
    return browser.get('/#/hosts');
  }
}
