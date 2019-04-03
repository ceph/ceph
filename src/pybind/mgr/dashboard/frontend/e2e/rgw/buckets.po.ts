import { browser } from 'protractor';

export class BucketsPage {
  navigateTo() {
    return browser.get('/#/rgw/bucket');
  }
}
