import { RbdFormModel } from './rbd-form.model';
import { RbdParentModel } from './rbd-parent.model';

export class RbdFormResponseModel extends RbdFormModel {
  features_name: string[];
  parent: RbdParentModel;
}
