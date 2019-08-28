import { by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class HostsPageHelper extends PageHelper {
  pages = { index: '/#/hosts' };

  async check_for_host() {
    await this.navigateTo();

    await expect(this.getTableTotalCount()).not.toBe(0);
  }

  // function that checks all services links work for first
  // host in table
  async check_services_links() {
    await this.navigateTo();
    let links_tested = 0;

    // grab services column for first host
    const services = element.all(by.css('.datatable-body-cell')).get(1);
    // check is any services links are present
    const txt = await services.getText();
    // check that text (links) is present in services box
    await expect(txt.length).toBeGreaterThan(0, 'No services links exist on first host');
    if (txt.length === 0) {
      return;
    }
    const links = services.all(by.css('a.service-link'));
    const num_links = await links.count();
    for (let i = 0; i < num_links; i++) {
      // click link, check it worked by looking for changed breadcrumb,
      // navigate back to hosts page, repeat until all links checked
      await links.get(i).click();
      await expect(this.getBreadcrumbText()).toEqual('Performance Counters');
      await this.navigateTo();
      await expect(this.getBreadcrumbText()).toEqual('Hosts');
      links_tested++;
    }
    // check if any links were actually tested
    await expect(links_tested > 0).toBe(true, 'No links were tested. Test failed');
  }
}
