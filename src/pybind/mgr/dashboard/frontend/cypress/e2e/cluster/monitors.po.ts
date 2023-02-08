import { PageHelper } from '../page-helper.po';

export class MonitorsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/monitor', id: 'cd-monitor' }
  };
}
