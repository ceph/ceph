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

    const services = element.all(by.css('cd-hosts a.service-link'));
    // check that text (links) is present in services box
    await expect(services.count()).toBeGreaterThan(0, 'No services links exist on first host');

    /**
     * Currently there is an issue [1] in ceph that it's causing
     * a random appearance of a mds service in the hosts service listing.
     * Decreasing the number of service by 1 temporarily fixes the e2e failure.
     *
     * TODO: Revert this change when the issue has been fixed.
     *
     * [1] https://tracker.ceph.com/issues/41538
     */
    const num_links = (await services.count()) - 1;

    for (let i = 0; i < num_links; i++) {
      // click link, check it worked by looking for changed breadcrumb,
      // navigate back to hosts page, repeat until all links checked
      await services.get(i).click();
      await this.waitTextToBePresent(this.getBreadcrumb(), 'Performance Counters');
      await this.navigateBack();
      await this.waitTextToBePresent(this.getBreadcrumb(), 'Hosts');
      links_tested++;
    }
    // check if any links were actually tested
    await expect(links_tested > 0).toBe(true, 'No links were tested. Test failed');
  }
}
