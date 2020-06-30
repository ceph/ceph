import { Task } from './task';

export class ExecutingTask extends Task {
  begin_time: number;
  progress: number;
}
