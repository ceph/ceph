import { $, $$, browser, by, element, protractor } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

browser.ignoreSynchronization = true;

export class LogsPageHelper extends PageHelper {
  pages = { index: '/#/logs' };

  async checkAuditForPoolFunction(poolname, poolfunction, hour, minute) {
    await this.navigateTo();

    // sometimes the modal from deleting pool is still present at this point.
    // This wait makes sure it isn't
    await browser.wait(
      Helper.EC.stalenessOf(element(by.cssContainingText('.modal-dialog', 'Delete Pool'))),
      Helper.TIMEOUT
    );

    // go to audit logs tab
    await element(by.cssContainingText('.nav-link', 'Audit Logs')).click();

    // Enter an earliest time so that no old messages with the same pool name show up
    await $$('.bs-timepicker-field')
      .get(0)
      .sendKeys(protractor.Key.chord(protractor.Key.CONTROL, 'a'));
    await $$('.bs-timepicker-field')
      .get(0)
      .sendKeys(protractor.Key.BACK_SPACE);
    if (hour < 10) {
      await $$('.bs-timepicker-field')
        .get(0)
        .sendKeys('0');
    }
    await $$('.bs-timepicker-field')
      .get(0)
      .sendKeys(hour);

    await $$('.bs-timepicker-field')
      .get(1)
      .sendKeys(protractor.Key.chord(protractor.Key.CONTROL, 'a'));
    await $$('.bs-timepicker-field')
      .get(1)
      .sendKeys(protractor.Key.BACK_SPACE);
    if (minute < 10) {
      await $$('.bs-timepicker-field')
        .get(1)
        .sendKeys('0');
    }
    await $$('.bs-timepicker-field')
      .get(1)
      .sendKeys(minute);

    // Enter the pool name into the filter box
    await $$('input.form-control.ng-valid')
      .first()
      .click();
    await $$('input.form-control.ng-valid')
      .first()
      .clear();
    await $$('input.form-control.ng-valid')
      .first()
      .sendKeys(poolname);

    const audit_logs_tab = $('.tab-pane.active');
    const audit_logs_body = audit_logs_tab.element(by.css('.card-body'));
    const logs = audit_logs_body.all(by.cssContainingText('.ng-star-inserted', poolname));

    expect(await logs.getText()).toMatch(poolname);
    expect(await logs.getText()).toMatch(`pool ${poolfunction}`);
  }

  async checkAuditForConfigChange(configname, setting, hour, minute) {
    await this.navigateTo();

    // go to audit logs tab
    await element(by.cssContainingText('.nav-link', 'Audit Logs')).click();

    // Enter an earliest time so that no old messages with the same config name show up
    await $$('.bs-timepicker-field')
      .get(0)
      .sendKeys(protractor.Key.chord(protractor.Key.CONTROL, 'a'));
    await $$('.bs-timepicker-field')
      .get(0)
      .sendKeys(protractor.Key.BACK_SPACE);
    if (hour < 10) {
      await $$('.bs-timepicker-field')
        .get(0)
        .sendKeys('0');
    }
    await $$('.bs-timepicker-field')
      .get(0)
      .sendKeys(hour);

    await $$('.bs-timepicker-field')
      .get(1)
      .sendKeys(protractor.Key.chord(protractor.Key.CONTROL, 'a'));
    await $$('.bs-timepicker-field')
      .get(1)
      .sendKeys(protractor.Key.BACK_SPACE);
    if (minute < 10) {
      await $$('.bs-timepicker-field')
        .get(1)
        .sendKeys('0');
    }
    await $$('.bs-timepicker-field')
      .get(1)
      .sendKeys(minute);

    // Enter the config name into the filter box
    await $$('input.form-control.ng-valid')
      .first()
      .click();
    await $$('input.form-control.ng-valid')
      .first()
      .clear();
    await $$('input.form-control.ng-valid')
      .first()
      .sendKeys(configname);

    const audit_logs_tab = $('.tab-pane.active');
    const audit_logs_body = audit_logs_tab.element(by.css('.card-body'));
    const logs = audit_logs_body.all(by.cssContainingText('.ng-star-inserted', configname));

    await browser.wait(Helper.EC.presenceOf(logs.first()), Helper.TIMEOUT);

    expect(await logs.getText()).toMatch(configname);
    expect(await logs.getText()).toMatch(setting);
  }
}
