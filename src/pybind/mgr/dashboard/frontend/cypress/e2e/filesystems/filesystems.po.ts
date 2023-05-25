import { PageHelper } from '../page-helper.po';

export class FilesystemsPageHelper extends PageHelper {
  pages = { index: { url: '#/cephfs', id: 'cd-cephfs-list' } };
}
