import { Pipe, PipeTransform } from '@angular/core';
import { PgCategoryService } from '~/app/ceph/shared/pg-category.service';
import { PgStateCount } from '~/app/shared/models/health.interface';

@Pipe({
<<<<<<< HEAD
  name: 'pgSummary',
  standalone: false
=======
  name: 'pgSummary'
>>>>>>> 781278cd500 (mgr/dashboard: fix backport issues from PR #68052)
})
export class PgSummaryPipe implements PipeTransform {
  constructor(private pgCategoryService: PgCategoryService) {}

  transform(value: any): any {
    if (!value) return null;
    const categoryPgAmount: Record<string, number> = {};
    value.statuses.forEach((status: PgStateCount) => {
      const categoryType = this.pgCategoryService.getTypeByStates(status?.state_name);
      if (!categoryPgAmount?.[categoryType]) {
        categoryPgAmount[categoryType] = 0;
      }
      categoryPgAmount[categoryType] += status?.count;
    });
    return {
      categoryPgAmount,
      total: value.total
    };
  }
}
