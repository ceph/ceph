import { browser } from 'protractor';

browser.ignoreSynchronization = true;

export class LogsPage {
  navigateTo() {
    return browser.get('/#/logs');
  }
}
