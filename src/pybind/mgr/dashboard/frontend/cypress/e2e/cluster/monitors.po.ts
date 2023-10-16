import { PageHelper } from '../page-helper.po';

export class MonitorsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/cluster/monitor', id: 'cd-monitor' }
  };
}
