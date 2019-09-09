import { Helper } from '../helper.po';
import { ConfigurationPage } from './configuration.po';

describe('Configuration page', () => {
  let page: ConfigurationPage;

  beforeAll(() => {
    page = new ConfigurationPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumb', () => {
    page.navigateTo();
    Helper.waitTextToBePresent(Helper.getBreadcrumb(), 'Configuration');
  });
});
