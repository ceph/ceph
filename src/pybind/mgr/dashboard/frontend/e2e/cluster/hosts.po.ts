import { by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class HostsPageHelper extends PageHelper {
  pages = { index: '/#/hosts' };

  check_for_host() {
    this.navigateTo();

    this.getTableCount()
      .getText()
      .then((hostcount) => {
        expect(hostcount.includes('0 total')).toBe(false);
      });
  }

  // function that checks all services links work for first
  // host in table
  check_services_links() {
    this.navigateTo();
    let links_tested = 0;

    // grab services column for first host
    const services = element.all(by.css('.datatable-body-cell')).get(1);
    // check is any services links are present
    services
      .getText()
      .then((txt) => {
        // check that text (links) is present in services box
        expect(txt.length).toBeGreaterThan(0, 'No services links exist on first host');
        if (txt.length === 0) {
          return;
        }
        const links = services.all(by.css('a.ng-star-inserted'));
        links.count().then((num_links) => {
          for (let i = 0; i < num_links; i++) {
            // click link, check it worked by looking for changed breadcrumb,
            // navigate back to hosts page, repeat until all links checked
            links.get(i).click();
            expect(this.getBreadcrumbText()).toEqual('Performance Counters');
            this.navigateTo();
            expect(this.getBreadcrumbText()).toEqual('Hosts');
            links_tested++;
          }
        });
      })
      .then(() => {
        // check if any links were actually tested
        expect(links_tested > 0).toBe(true, 'No links were tested. Test failed');
      });
  }
}
