export class RbdSnapshotModel {
  id: number;
  name: string;
  size: number;
  timestamp: string;
  is_protected: boolean;

  cdExecuting: string;
}
