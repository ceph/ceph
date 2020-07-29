import { FilesystemsPageHelper } from './filesystems.po';

describe('Filesystems page', () => {
  const filesystems = new FilesystemsPageHelper();

  beforeEach(() => {
    cy.login();
    filesystems.navigateTo();
  });

  describe('breadcrumb test', () => {
    it('should open and show breadcrumb', () => {
      filesystems.expectBreadcrumbText('Filesystems');
    });
  });
});
