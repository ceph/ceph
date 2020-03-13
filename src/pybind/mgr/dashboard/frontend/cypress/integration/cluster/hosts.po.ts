import { PageHelper } from '../page-helper.po';

export class HostsPageHelper extends PageHelper {
  pages = { index: { url: '#/hosts', id: 'cd-hosts' } };

  check_for_host() {
    this.getTableTotalCount().should('not.be.eq', 0);
  }

  // function that checks all services links work for first
  // host in table
  check_services_links() {
    // check that text (links) is present in services box
    let links_tested = 0;

    cy.get('cd-hosts a.service-link')
      .should('have.length.greaterThan', 0)
      .then(($elems) => {
        $elems.each((_i, $el) => {
          // click link, check it worked by looking for changed breadcrumb,
          // navigate back to hosts page, repeat until all links checked
          cy.contains('a', $el.innerText).should('exist').click();
          this.expectBreadcrumbText('Performance Counters');
          this.navigateTo();
          links_tested++;
        });
        // check if any links were actually tested
        expect(links_tested).gt(0);
      });
  }
}
