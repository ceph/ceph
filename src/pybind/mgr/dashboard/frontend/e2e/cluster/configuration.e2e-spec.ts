import { Helper } from '../helper.po';

describe('Configuration page', () => {
  let configuration: Helper['configuration'];

  beforeAll(() => {
    configuration = new Helper().configuration;
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  describe('breadcrumb test', () => {
    beforeAll(() => {
      configuration.navigateTo();
    });

    it('should open and show breadcrumb', () => {
      expect(configuration.getBreadcrumbText()).toEqual('Configuration');
    });
  });
  describe('edit configuration test', () => {
    beforeAll(() => {
      configuration.navigateTo();
    });

    it('should click and edit a configuration and results should appear in the table', () => {
      const configName = 'client_cache_size';

      configuration.edit(
        configName,
        ['global', '1'],
        ['mon', '2'],
        ['mgr', '3'],
        ['osd', '4'],
        ['mds', '5'],
        ['client', '6']
      );
      configuration.configClear(configName);
    });
  });
});
