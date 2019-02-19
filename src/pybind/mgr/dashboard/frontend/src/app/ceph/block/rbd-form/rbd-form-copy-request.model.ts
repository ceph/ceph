import { RbdConfigurationEntry } from '../../../shared/models/configuration';

export class RbdFormCopyRequestModel {
  dest_pool_name: string;
  dest_image_name: string;
  snapshot_name: string;
  obj_size: number;
  features: Array<string> = [];
  stripe_unit: number;
  stripe_count: number;
  data_pool: string;
  configuration: RbdConfigurationEntry[];
}
