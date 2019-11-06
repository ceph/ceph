import { by, element } from 'protractor';
import { OSDsPageHelper } from './osds.po';

describe('OSDs page', () => {
  let osds: OSDsPageHelper;

  beforeAll(() => {
    osds = new OSDsPageHelper();
  });

  afterEach(async () => {
    await OSDsPageHelper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
    beforeAll(async () => {
      await osds.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await osds.waitTextToBePresent(osds.getBreadcrumb(), 'OSDs');
    });

    it('should show two tabs', async () => {
      await expect(osds.getTabsCount()).toEqual(2);
    });

    it('should show OSDs list tab at first', async () => {
      await expect(osds.getTabText(0)).toEqual('OSDs List');
    });

    it('should show overall performance as a second tab', async () => {
      await expect(osds.getTabText(1)).toEqual('Overall Performance');
    });
  });

  describe('check existence of fields on OSD page', () => {
    it('should check that number of rows and count in footer match', async () => {
      await osds.navigateTo();
      await expect(osds.getTableTotalCount()).toEqual(osds.getTableRows().count());
    });

    it('should verify that selected footer increases when an entry is clicked', async () => {
      await osds.navigateTo();
      await osds.getFirstCell().click(); // clicks first osd
      await expect(osds.getTableSelectedCount()).toEqual(1);
    });

    it('should verify that buttons exist', async () => {
      await osds.navigateTo();
      await expect(element(by.cssContainingText('button', 'Scrub')).isPresent()).toBe(true);
      await expect(
        element(by.cssContainingText('button', 'Cluster-wide configuration')).isPresent()
      ).toBe(true);
    });

    it('should check the number of tabs when selecting an osd is correct', async () => {
      await osds.navigateTo();
      await osds.getFirstCell().click(); // clicks first osd
      await expect(osds.getTabsCount()).toEqual(8); // includes tabs at the top of the page
    });

    it('should show the correct text for the tab labels', async () => {
      await expect(osds.getTabText(2)).toEqual('Attributes (OSD map)');
      await expect(osds.getTabText(3)).toEqual('Metadata');
      await expect(osds.getTabText(4)).toEqual('Device health');
      await expect(osds.getTabText(5)).toEqual('Performance counter');
      await expect(osds.getTabText(6)).toEqual('Histogram');
      await expect(osds.getTabText(7)).toEqual('Performance Details');
    });
  });
});
