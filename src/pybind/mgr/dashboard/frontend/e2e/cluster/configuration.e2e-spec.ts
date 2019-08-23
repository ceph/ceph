import { Helper } from '../helper.po';
import { ConfigurationPageHelper } from './configuration.po';

describe('Configuration page', () => {
  let configuration: ConfigurationPageHelper;

  beforeAll(() => {
    configuration = new ConfigurationPageHelper();
  });

  afterEach(async () => {
    await Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(async () => {
      await configuration.navigateTo();
    });

    it('should open and show breadcrumb', async () => {
      expect(await configuration.getBreadcrumbText()).toEqual('Configuration');
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
