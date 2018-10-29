import { RbdConfigurationEntry } from '../../../shared/models/configuration';

export class RbdFormCloneRequestModel {
  child_pool_name: string;
  child_image_name: string;
  obj_size: number;
  features: Array<string> = [];
  stripe_unit: number;
  stripe_count: number;
  data_pool: string;
  configuration?: RbdConfigurationEntry[];
}
