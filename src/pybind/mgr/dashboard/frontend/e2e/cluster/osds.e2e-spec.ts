import { $$, by, element } from 'protractor';
import { OSDsPageHelper } from './osds.po';

describe('OSDs page', () => {
  let osds: OSDsPageHelper;

  beforeAll(async () => {
    osds = new OSDsPageHelper();
    await osds.navigateTo();
  });

  afterEach(async () => {
    await OSDsPageHelper.checkConsole();
  });

  describe('breadcrumb and tab tests', () => {
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
      await expect(osds.getTableTotalCount()).toEqual(osds.getTableRows().count());
    });

    it('should verify that buttons exist', async () => {
      await expect(element(by.cssContainingText('button', 'Create')).isPresent()).toBe(true);
      await expect(
        element(by.cssContainingText('button', 'Cluster-wide configuration')).isPresent()
      ).toBe(true);
    });

    describe('by selecting one row in OSDs List', () => {
      beforeAll(async () => {
        await osds.getFirstCell().click();
      });

      it('should verify that selected footer increases', async () => {
        await expect(osds.getTableSelectedCount()).toEqual(1);
      });

      it('should show the correct text for the tab labels', async () => {
        const tabHeadings = $$('#tabset-osd-details > div > tab').map((e) =>
          e.getAttribute('heading')
        );
        await expect(tabHeadings).toEqual([
          'Devices',
          'Attributes (OSD map)',
          'Metadata',
          'Device health',
          'Performance counter',
          'Histogram',
          'Performance Details'
        ]);
      });
    });
  });
});
