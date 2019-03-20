import { browser } from 'protractor';

export class AlertsPage {
  navigateTo() {
    return browser.get('/#/alerts');
  }
}
