import { browser } from 'protractor';

export class DaemonsPage {
  navigateTo() {
    return browser.get('/#/rgw/daemon');
  }
}
