import { PageHeaderPageHelper } from './page-header.po';

describe('Page header component', () => {
  const pageHeader = new PageHeaderPageHelper();

  beforeEach(() => {
    cy.login();
    pageHeader.navigateToCephfsMirroring();
  });

  it('should display the page header on Filesystem Mirroring page', () => {
    pageHeader.getPageHeader().should('be.visible');
  });

  it('should show the expected title in the page header', () => {
    pageHeader.getHeaderTitle().then((text) => {
      expect(text.trim()).to.equal('Filesystem Mirroring');
    });
  });

  it('should show the expected description in the page header', () => {
    pageHeader.getHeaderDescription().then((text) => {
      expect(text.trim()).to.equal(
        'Configure mirroring between filesystems and monitor replication status.'
      );
    });
  });
});
