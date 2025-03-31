import { Task } from './task';

export class TaskException {
  status: number;
  code: number;
  component: string;
  detail: string;
  title: string;
  task: Task;
}
