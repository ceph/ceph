export class RbdModel {
  id: string;
  unique_id: string;
  name: string;
  pool_name: string;
  namespace: string;
  image_format: RBDImageFormat;

  cdExecuting: string;
}

export enum RBDImageFormat {
  V1 = 1,
  V2 = 2
}
