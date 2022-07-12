import { Task } from './task';
import { TaskException } from './task-exception';

export class FinishedTask extends Task {
  begin_time: string;
  end_time: string;
  exception: TaskException;
  latency: number;
  progress: number;
  ret_value: any;
  success: boolean;
  duration: number;

  errorMessage: string;
}
