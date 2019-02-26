import { browser } from 'protractor';

export class ImagesPage {
  navigateTo() {
    return browser.get('/#/block/rbd');
  }
}
