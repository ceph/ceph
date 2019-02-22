import { browser } from 'protractor';

export class FilesystemsPage {
  navigateTo() {
    return browser.get('/#/cephfs');
  }
}
