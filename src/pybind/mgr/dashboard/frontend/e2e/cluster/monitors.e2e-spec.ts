import { Helper } from '../helper.po';
import { MonitorsPage } from './monitors.po';

describe('Monitors page', () => {
  let page: MonitorsPage;

  beforeAll(() => {
    page = new MonitorsPage();
  });

  afterEach(() => {
    Helper.checkConsole();
  });

  it('should open and show breadcrumnb', () => {
    page.navigateTo();
    expect(Helper.getBreadcrumbText()).toEqual('Monitors');
  });
});
