import { NfsPageHelper } from './nfs.po';

describe('Nfs page', () => {
  const nfs = new NfsPageHelper();

  beforeEach(() => {
    cy.login();
    nfs.navigateTo();
  });

  describe('breadcrumb test', () => {
    it('should open and show breadcrumb', () => {
      nfs.expectBreadcrumbText('NFS');
    });
  });
});
