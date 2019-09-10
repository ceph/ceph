import { $ } from 'protractor';
import { ConfigurationPageHelper } from './configuration.po';

describe('Configuration page', () => {
  let configuration: ConfigurationPageHelper;

  beforeAll(() => {
    configuration = new ConfigurationPageHelper();
  });

  afterEach(async () => {
    await ConfigurationPageHelper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await configuration.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      await configuration.waitTextToBePresent(configuration.getBreadcrumb(), 'Configuration');
    });
  });

  describe('fields check', () => {
    beforeAll(async () => {
      await configuration.navigateTo();
    });

    it('should verify that selected footer increases when an entry is clicked', async () => {
      await configuration.getFirstCell().click();
      const selectedCount = await configuration.getTableSelectedCount();
      await expect(selectedCount).toBe(1);
    });

    it('should check that details table opens and tab is correct', async () => {
      await configuration.getFirstCell().click();
      await expect($('.table.table-striped.table-bordered').isDisplayed());
      await expect(configuration.getTabsCount()).toEqual(1);
      await expect(configuration.getTabText(0)).toEqual('Details');
    });
  });

  describe('edit configuration test', () => {
    beforeAll(async () => {
      await configuration.navigateTo();
    });

    it('should click and edit a configuration and results should appear in the table', async () => {
      const configName = 'client_cache_size';

      await configuration.edit(
        configName,
        ['global', '1'],
        ['mon', '2'],
        ['mgr', '3'],
        ['osd', '4'],
        ['mds', '5'],
        ['client', '6']
      );
      await configuration.configClear(configName);
    });
  });
});
