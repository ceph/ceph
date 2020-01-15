export class CrushNode {
  id: number;
  name: string;
  type: string;
  type_id: number;
  // For nodes with leafs (Buckets)
  children?: number[]; // Holds node id's of children
  // For non root nodes
  pool_weights?: object;
  // For leafs (Devices)
  device_class?: string;
  crush_weight?: number;
  exists?: number;
  primary_affinity?: number;
  reweight?: number;
  status?: string;
}
