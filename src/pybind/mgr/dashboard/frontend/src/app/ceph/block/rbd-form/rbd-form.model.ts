import { RbdConfigurationEntry } from '../../../shared/models/configuration';

export class RbdFormModel {
  name: string;
  pool_name: string;
  data_pool: string;
  size: number;

  /* Striping */
  obj_size: number;
  stripe_unit: number;
  stripe_count: number;

  /* Configuration */
  configuration: RbdConfigurationEntry[];
}
