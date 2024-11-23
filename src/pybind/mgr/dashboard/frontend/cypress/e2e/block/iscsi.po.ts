import { PageHelper } from '../page-helper.po';

export class IscsiPageHelper extends PageHelper {
  pages = {
    index: { url: '#/block/iscsi/overview', id: 'cd-iscsi' }
  };
}
