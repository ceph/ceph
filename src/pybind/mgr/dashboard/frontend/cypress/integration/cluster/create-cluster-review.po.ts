import { PageHelper } from '../page-helper.po';

export class CreateClusterReviewPageHelper extends PageHelper {
  pages = {
    index: { url: '#/create-cluster', id: 'cd-create-cluster-review' }
  };

  checkDefaultHostName() {
    this.getTableCell(1, 'ceph-node-00.cephlab.com').should('exist');
  }
}
