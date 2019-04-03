import { browser } from 'protractor';

export class ManagerModulesPage {
  navigateTo() {
    return browser.get('/#/mgr-modules');
  }
}
