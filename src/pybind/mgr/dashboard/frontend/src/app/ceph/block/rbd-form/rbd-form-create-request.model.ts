import { RbdFormModel } from './rbd-form.model';

export class RbdFormCreateRequestModel extends RbdFormModel {
  schedule_interval: string;
  features: Array<string> = [];
}
