import { $$, by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class DaemonsPageHelper extends PageHelper {
  pages = { index: '/#/rgw/daemon' };

  async checkTables() {
    await this.navigateTo();

    // click on a daemon so details table appears
    await $$('.datatable-body-cell-label')
      .first()
      .click();

    const tab_container = $$('.tab-container').last();
    const details_table = tab_container.all(by.css('cd-table')).get(0);
    const performance_counters_table = tab_container.all(by.css('cd-table')).get(1);

    // check details table is visible
    await expect(details_table.isDisplayed()).toBe(true);
    // check at least one field is present
    await expect(details_table.getText()).toMatch('ceph_version');
    // check performance counters table is not currently visible
    await expect(performance_counters_table.isDisplayed()).toBe(false);

    // click on performance counters tab and check table is loaded
    await element(by.cssContainingText('.nav-link', 'Performance Counters')).click();
    await expect(performance_counters_table.isDisplayed()).toBe(true);
    // check at least one field is present
    await expect(performance_counters_table.getText()).toMatch('objecter.op_r');
    // check details table is not currently visible
    await expect(details_table.isDisplayed()).toBe(false);

    // click on performance details tab
    await element(by.cssContainingText('.nav-link', 'Performance Details')).click();
    // checks the other tabs' content isn't visible
    await expect(details_table.isDisplayed()).toBe(false);
    await expect(performance_counters_table.isDisplayed()).toBe(false);
    // TODO: Expect Grafana iFrame
  }
}
