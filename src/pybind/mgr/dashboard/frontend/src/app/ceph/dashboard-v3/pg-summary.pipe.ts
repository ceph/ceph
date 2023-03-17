import { Pipe, PipeTransform } from '@angular/core';
import _ from 'lodash';
import { PgCategoryService } from '~/app/ceph/shared/pg-category.service';

@Pipe({
  name: 'pgSummary'
})
export class PgSummaryPipe implements PipeTransform {
  constructor(private pgCategoryService: PgCategoryService) {}

  transform(value: any): any {
    const categoryPgAmount: Record<string, number> = {};
    let total = 0;
    _.forEach(value.statuses, (pgAmount, pgStatesText) => {
      const categoryType = this.pgCategoryService.getTypeByStates(pgStatesText);
      if (_.isUndefined(categoryPgAmount[categoryType])) {
        categoryPgAmount[categoryType] = 0;
      }
      categoryPgAmount[categoryType] += pgAmount;
      total += pgAmount;
    });
    return {
      categoryPgAmount,
      total
    };
  }
}
