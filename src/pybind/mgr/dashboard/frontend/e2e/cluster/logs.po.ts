import { browser } from 'protractor';
import { PageHelper } from '../page-helper.po';

browser.ignoreSynchronization = true;

export class LogsPageHelper extends PageHelper {
  pages = { index: '/#/logs' };
}
