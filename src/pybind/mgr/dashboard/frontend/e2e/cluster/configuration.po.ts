import { browser } from 'protractor';

export class ConfigurationPage {
  navigateTo() {
    return browser.get('/#/configuration');
  }
}
