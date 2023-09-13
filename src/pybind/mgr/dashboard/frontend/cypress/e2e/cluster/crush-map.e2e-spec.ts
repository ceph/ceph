import { CrushMapPageHelper } from './crush-map.po';

describe('CRUSH map page', () => {
  const crushmap = new CrushMapPageHelper();

  beforeEach(() => {
    cy.login();
    crushmap.navigateTo();
  });

  describe('breadcrumb test', () => {
    it('should open and show breadcrumb', () => {
      crushmap.expectBreadcrumbText('CRUSH map');
    });
  });

  describe('fields check', () => {
    it('should check that title & table appears', () => {
      // Check that title (CRUSH map viewer) appears
      crushmap.getPageTitle().should('equal', 'CRUSH map viewer');

      // Check that title appears once OSD is clicked
      crushmap.getCrushNode(0).click();

      crushmap
        .getLegends()
        .invoke('text')
        .then((legend) => {
          crushmap.getCrushNode(0).should('have.text', legend);
        });

      // Check that table appears once OSD is clicked
      crushmap.getDataTables().should('be.visible');
    });
  });
});
