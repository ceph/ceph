import { $, browser } from 'protractor';
import { Helper } from '../helper.po';

describe('CRUSH map page', () => {
  let crushmap: Helper['crushmap'];

  beforeAll(() => {
    crushmap = new Helper().crushmap;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      crushmap.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(crushmap.getBreadcrumbText()).toEqual('CRUSH map');
    });
  });
  describe('fields check', () => {
    beforeAll(() => {
      crushmap.navigateTo();
    });

    it('should check that title & table appears', () => {
      // Check that title (CRUSH map viewer) appears
      expect(crushmap.getPageTitle()).toMatch('CRUSH map viewer');

      // Check that title appears once OSD is clicked
      crushmap.getCrushNode(1).click();
      const label = $('legend').getText(); // Get table label
      expect(crushmap.getCrushNode(1).getText()).toEqual(label);

      // Check that table appears once OSD is clicked
      browser.wait(Helper.EC.visibilityOf($('.datatable-body'))).then(() => {
        expect($('.datatable-body').isDisplayed()).toBe(true);
      });
    });
  });
});
