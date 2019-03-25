import { browser } from 'protractor';

export class UsersPage {
  navigateTo() {
    return browser.get('/#/rgw/user');
  }
}
