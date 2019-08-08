import { $, $$ } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class CrushMapPageHelper extends PageHelper {
  pages = { index: '/#/crush-map' };

  getPageTitle() {
    return $('.card-header').getText();
  }

  getCrushNode(idx) {
    return $$('.node-name.ng-star-inserted').get(idx);
  }
}
