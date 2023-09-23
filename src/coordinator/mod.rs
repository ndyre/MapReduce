//! The MapReduce coordinator.
//!

use anyhow::Result;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Instant;
use std::collections::{HashMap, VecDeque};

use crate::{log, rpc::coordinator::*};
use crate::app::named;
use crate::*;

pub mod args;

#[derive(PartialEq)]
pub enum TaskStatus {
    Idle,
    Running,
    Finished,
}

pub struct CoordinatorState {
    next_worker_id: WorkerId,
    next_job_id: JobId,
    heartbeats: HashMap<WorkerId, Instant>,
    jobs: HashMap<JobId, Job>,
    job_queue: VecDeque<JobId>,
    workers_map_tasks: HashMap<WorkerId, Vec<TaskId>>,
    workers_reduce_tasks: HashMap<WorkerId, Vec<TaskId>>
}

pub struct Coordinator {
    inner: Arc<Mutex<CoordinatorState>>
}

pub struct Job {
    map_tasks: Vec<Task>,
    reduce_tasks: Vec<Task>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    n_map: u32,
    n_reduce_remaining: u32,
    n_map_remaining: u32,
    args: Vec<u8>,
    finished: bool,
    failed: bool,
    errors: Vec<String>,
    map_task_assignments: Vec<MapTaskAssignment>
}


pub struct Task {
    task_num: TaskNumber,
    worker: Option<WorkerId>,
    file: String,
    status: TaskStatus,
}

#[derive(Clone)]
pub struct TaskId {
    job_id: JobId,
    task_num: TaskNumber
}

impl CoordinatorState {
    pub fn new() -> Self {
        Self {
            next_worker_id: 1,
            next_job_id: 1,
            heartbeats: HashMap::new(),
            jobs: HashMap::new(),
            job_queue: VecDeque::new(),
            workers_map_tasks: HashMap::new(),
            workers_reduce_tasks: HashMap::new()
        }
    }
}

impl Coordinator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CoordinatorState::new()))
        }
    }
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        let mut coordinater_state = self.inner.lock().await;
        let job_id = coordinater_state.next_job_id;
        let job_request = req.into_inner();

        match named(&job_request.app) {
            Err(e) => {
                return Err(tonic::Status::new(tonic::Code::InvalidArgument, e.to_string()))
            }
            Ok(_) => ()
        };

        let num_files = job_request.files.len();
        let mut job = Job {
            map_tasks: Vec::new(),
            reduce_tasks: Vec::new(),
            output_dir: job_request.output_dir,
            app: job_request.app,
            n_reduce: job_request.n_reduce,
            n_map: num_files as u32,
            n_reduce_remaining: job_request.n_reduce,
            n_map_remaining: num_files as u32,
            args: job_request.args,
            finished: false,
            failed: false,
            errors: Vec::new(),
            map_task_assignments: Vec::new()
        };

        let mut map_task_num: usize = 0;
        for file in job_request.files {
            job.map_tasks.push(
                Task {
                    task_num: map_task_num,
                    worker: None,
                    file: file,
                    status: TaskStatus::Idle,
                }
            );
            map_task_num += 1;
        }

        for reduce_task_num in 0..job.n_reduce {
            job.reduce_tasks.push(
                Task {
                    task_num: reduce_task_num as TaskNumber,
                    worker: None,
                    file: String::new(),
                    status: TaskStatus::Idle
                }
            )
        }

        log::info!(
            "Coordinator succesfully recieved job: {}",
            job_id
        );

        coordinater_state.jobs.insert(job_id, job);
        coordinater_state.job_queue.push_back(job_id);
        coordinater_state.next_job_id += 1;

        Ok(Response::new(SubmitJobReply { job_id : job_id}))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        let coordinater_state = self.inner.lock().await;
        let poll_request = req.into_inner();
        match coordinater_state.jobs.get(&poll_request.job_id) {
            Some(job) => Ok(Response::new(PollJobReply { 
                done: job.finished,
                failed: job.failed,
                errors: job.errors.clone()
            })),
            None => Err(tonic::Status::new(tonic::Code::NotFound, "job id is invalid"))
        }
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let mut coordinater_state = self.inner.lock().await;
        let worker_id = req.into_inner().worker_id;
        coordinater_state.heartbeats.insert(worker_id, Instant::now());
        Ok(Response::new(HeartbeatReply {}))
    }

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let mut coordinator_state = self.inner.lock().await;
        log::info!(
            "Coordinator assigning worker id: {}",
            coordinator_state.next_worker_id
        );
        let worker_id = coordinator_state.next_worker_id;
        coordinator_state.heartbeats.insert(worker_id, Instant::now());
        coordinator_state.next_worker_id += 1;
        Ok(Response::new(RegisterReply { worker_id: worker_id }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        let mut coordinator_state = self.inner.lock().await;

        // The worker id of the worker 
        let worker_id = req.into_inner().worker_id;

        // Flags to indicate if a map or reduce task is assigned
        let mut map_task_assigned = false;
        let mut reduce_task_assigned = false;

        // If we assign a task we should add it to the list of tasks for the worker
        let mut task_id: Option<TaskId> = None;
        
        // Empty reponse if there are no tasks to assign
        let mut response = Response::new(GetTaskReply {
            job_id: 0,
            output_dir: "".to_string(),
            app: "".to_string(),
            task: 0,
            file: "".to_string(),
            n_reduce: 0,
            n_map: 0,
            reduce: false,
            wait: true,
            map_task_assignments: Vec::new(),
            args: Vec::new(),
        });

        // Check if any workers have died
        let mut dead_workers: Vec<WorkerId> = Vec::new();
        let workers_map_tasks = coordinator_state.workers_map_tasks.clone();
        let workers_reduce_tasks = coordinator_state.workers_reduce_tasks.clone();
        for (worker_id, last_heartbeat) in coordinator_state.heartbeats.clone() {
            if last_heartbeat.elapsed().as_secs() > TASK_TIMEOUT_SECS {
                log::info!("Worker {} is dead", worker_id);
                dead_workers.push(worker_id);
                
                // Worker has died check if it has any tasks that need to be reassigned
                if workers_map_tasks.contains_key(&worker_id) {

                    // All map tasks must be reassigned
                    let map_task_ids = workers_map_tasks.get(&worker_id).unwrap();
                    for id in map_task_ids {
                        let job = coordinator_state.jobs.get_mut(&id.job_id).unwrap();
                        let map_task = job.map_tasks.get_mut(id.task_num).unwrap();
                        if map_task.status == TaskStatus::Finished {
                            job.n_map_remaining += 1;
                        }
                        log::info!("Reassiging map task {}", map_task.task_num);
                        map_task.status = TaskStatus::Idle;
                        job.finished = false;
                        job.map_task_assignments.retain(|assignment| assignment.worker_id != worker_id);
                    }
                    coordinator_state.workers_map_tasks.remove(&worker_id);
                }

                if workers_reduce_tasks.contains_key(&worker_id) {

                    // Only reduce tasks that were previously running must be reassigned
                    let reduce_task_ids = workers_reduce_tasks.get(&worker_id).unwrap();
                    for id in reduce_task_ids {
                        let job = coordinator_state.jobs.get_mut(&id.job_id).unwrap();
                        let reduce_task = job.reduce_tasks.get_mut(id.task_num).unwrap();
                        if reduce_task.status == TaskStatus::Running {
                            log::info!("Reassiging reduce task {}", reduce_task.task_num);
                            reduce_task.status = TaskStatus::Idle;
                            job.finished = false;
                        }
                    }
                    coordinator_state.workers_reduce_tasks.remove(&worker_id);
                }
            }
        }

        // Do not need to keep track of dead workers' last heartbeat
        for worker in dead_workers {
            coordinator_state.heartbeats.remove(&worker);
        }

        // Check job queue
        'outer: for job_id in coordinator_state.job_queue.clone() {
            let job = coordinator_state.jobs.get_mut(&job_id).unwrap();
            if !job.finished && !job.failed {
                let mut map_tasks_finished = true;
                if job.n_map_remaining > 0 {
                    
                    // There are map tasks that are still running/idle
                    map_tasks_finished = false;
                    for mut map_task in job.map_tasks.iter_mut() {
                        if map_task.status == TaskStatus::Idle {

                            // Asign map task to worker
                            map_task_assigned = true;
                            map_task.worker = Some(worker_id);
                            map_task.status = TaskStatus::Running;

                            // Add task to list of tasks for this worker
                            task_id = Some(TaskId {job_id: job_id, task_num: map_task.task_num});

                            // Add to list of map task assigments
                            job.map_task_assignments.push(
                                MapTaskAssignment {task: map_task.task_num as u32, worker_id: worker_id }
                            );

                            log::info!(
                                "Coordinator assigning map task {} to worker {}",
                                map_task.task_num,
                                worker_id
                            );
                            response = Response::new(GetTaskReply {
                                job_id: job_id,
                                output_dir: job.output_dir.clone(),
                                app: job.app.clone(),
                                task: map_task.task_num as u32,
                                file: map_task.file.clone(),
                                n_reduce: job.n_reduce,
                                n_map: job.n_map,
                                reduce: false,
                                wait: false,
                                map_task_assignments: Vec::new(),
                                args: job.args.clone(),
                            });
                            break 'outer;
                        }
                    }
                }

                // All map_tasks done
                if job.n_reduce_remaining > 0 && map_tasks_finished {
                    for mut reduce_task in job.reduce_tasks.iter_mut() {
                        if reduce_task.status == TaskStatus::Idle {

                            // Asign reduce task to worker
                            reduce_task_assigned = true;
                            reduce_task.worker = Some(worker_id);
                            reduce_task.status = TaskStatus::Running;


                            // Add task to list of this worker's tasks
                            task_id = Some(TaskId{ job_id: job_id, task_num: reduce_task.task_num });

                            log::info!(
                                "Coordinator assigning reduce task {} to worker {}",
                                reduce_task.task_num,
                                worker_id
                            );
                            response = Response::new(GetTaskReply {
                                job_id: job_id,
                                output_dir: job.output_dir.clone(),
                                app: job.app.clone(),
                                task: reduce_task.task_num as u32,
                                file: String::new(),
                                n_reduce: job.n_reduce,
                                n_map: job.n_map,
                                reduce: true,
                                wait: false,
                                map_task_assignments: job.map_task_assignments.clone(),
                                args: job.args.clone(),
                            });
                            break 'outer;
                        }
                    }
                }
            }
        }

        if map_task_assigned {
            match coordinator_state.workers_map_tasks.entry(worker_id) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(vec![task_id.unwrap()]);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().push(task_id.unwrap());
                }
            }
        } else if reduce_task_assigned {
            match coordinator_state.workers_reduce_tasks.entry(worker_id) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(vec![task_id.unwrap()]);
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().push(task_id.unwrap());
                }
            }
        } else {
            // No tasks available
            log::info!(
                "Coordinator has no tasks available for Worker {}",
                worker_id
            );
        }
        Ok(response)
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        let mut coordinator_state = self.inner.lock().await;
        let finish_request = req.into_inner();
        let job = coordinator_state.jobs.get_mut(&finish_request.job_id).unwrap();

        // We set the task to finished and decrement the number of tasks remaining for the associated job
        if finish_request.reduce {
            let mut reduce_task = job.reduce_tasks.get_mut(finish_request.task as TaskNumber).unwrap();
            reduce_task.status = TaskStatus::Finished;
            job.n_reduce_remaining -= 1;
            log::info!("Finished reduce task {}", finish_request.task);
        } else {
            let mut map_task = job.map_tasks.get_mut(finish_request.task as TaskNumber).unwrap();
            map_task.status = TaskStatus::Finished;
            job.n_map_remaining -= 1;
            log::info!("Finished map task {}", finish_request.task);
        }

        // Job is finished
        if job.n_map_remaining == 0 && job.n_reduce_remaining == 0 {
            job.finished = true;
        }
        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        let mut coordinator_state = self.inner.lock().await;
        let fail_request = req.into_inner();
        let job = coordinator_state.jobs.get_mut(&fail_request.job_id).unwrap();

        // If we can retry we set the task to Idle so it is reassigned otherwise the job has failed
        if !fail_request.retry {
            job.errors.push(fail_request.error);
            job.failed = true;
        } else {
            let reduce_task = job.reduce_tasks.get_mut(fail_request.task as usize).unwrap();
            reduce_task.status = TaskStatus::Idle;
        }

        // Remove task from list of tasks worker is working on/finished
        if fail_request.reduce {
            let reduce_task_ids = coordinator_state.workers_reduce_tasks.get_mut(&fail_request.worker_id).unwrap();
            reduce_task_ids.retain(|id| id.job_id != fail_request.job_id || id.task_num != fail_request.task as usize);
        } else {
            let map_task_ids = coordinator_state.workers_map_tasks.get_mut(&fail_request.worker_id).unwrap();
            map_task_ids.retain(|id| id.job_id != fail_request.job_id || id.task_num != fail_request.task as usize);
        }
        log::info!("Worker {} failed task {}", fail_request.worker_id, fail_request.task);
        Ok(Response::new(FailTaskReply {}))
    }
}

pub async fn start(_args: args::Args) -> Result<()> {
    let addr = COORDINATOR_ADDR.parse().unwrap();

    let coordinator = Coordinator::new();
    let svc = coordinator_server::CoordinatorServer::new(coordinator);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
