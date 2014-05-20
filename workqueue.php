<?php
/**
 * A simple (one file) task queue for background processing.  Can be
 * used for parallel processing or as a daemonised service.
 *
 * Launches workers in subprocesses using a bootstrap specified by the job and then
 * runs task methods on the workers.
 *
 * @licence GPLv3
 * @author Tris Forster
 */

$job_stopped = serialize(new WorkQueueException("Job stopped"));

class WorkQueueDatabase extends PDO {

	function __construct($path) {
		parent::__construct('sqlite:' . $path);

		$key = ftok($path, 'a');
		$this->sem = sem_get($key);
	}

	/**
	 * Helper method - fetch a single row from the database using a prepared statement.
	 *
	 * @param string $sql
	 * @param array $params
	 * @throws OutOfBoundsException if no items returned
	 * @return array
	 */
	public function fetchOne($sql, $params=array()) {
		$query = $this->prepare($sql);
		$query->execute($params);
		$result = $query->fetch();
		$query->closeCursor();
		if(!$result) throw new OutOfBoundsException("No item returned");
		return $result;
	}

	/**
	 * Helper method - execute a single query using a prepared statement.
	 *
	 * @param string $sql
	 * @param array $params
	 */
	public function execOne($sql, $params=array()) {
		$query = $this->prepare($sql);
		$query->execute($params);
	}

	function beginTransaction() {
		$this->lock = sem_acquire($this->sem);
		return parent::beginTransaction();
	}

	function commit() {
		$success = parent::commit();
		$this->lock = !sem_release($this->sem);
		return $success;
	}

	function rollBack() {
		$success = parent::rollBack();
		$this->lock = !sem_release($this->sem);
		return $success;
	}

}

class WorkQueue {

	const STATUS_QUEUED = 0;
	const STATUS_RUNNING = 1;
	const STATUS_FAILED = 2;
	const STATUS_SUCCESS = 3;

	private $db;

	public static $instance = null;
	public static $job = null;

	private static $instances = array();

	public $retry_delay = 5; # seconds to wait between catchable exceptions

	public $max_retry = 3; # how many attempts should a task get

	public $poll_interval = 1; # seconds between manager polls

	public $spawn_limit = 3;

	public $debug = false;

	private function __construct($file) {

		$this->file = $file;
		$this->pid = getmypid();
		$this->start_time = microtime(true);

		$created = false;
		if(file_exists($file)) {
			$this->path = realpath($file);
		} else {
			$this->path = realpath(dirname($file)) . "/" . basename($file);
			$created = true;
		}

		$this->log("Database: {$this->path}");

		$this->db = new WorkQueueDatabase($this->path);
		$this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		$this->db->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

		$this->log_dir = $this->path . ".logs";

		$this->db->exec("PRAGMA busy_timeout=10000");

		if($created) {
			$cmds = array(
					#"PRAGMA journal_mode=WAL",
					"CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, bootstrap VARCHAR, pid INTEGER, workers INTEGER, started INTEGER);",
					"CREATE TABLE tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, pid INTEGER, method VARCHAR, params_data TEXT, priority INTEGER, status INTEGER, retry INTEGER, result_data TEXT);",
					"CREATE TABLE workers (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, pid INTEGER);"
			);
			foreach($cmds as $cmd) $this->db->exec($cmd);

			// create or clear the log dir
			if(is_dir($this->log_dir)) {
				exec("rm -f {$this->log_dir}/*");
			} else {
				mkdir($this->log_dir);
			}
		}
	}

	/**
	 * Use singleton pattern
	 *
	 * @param string $file filename for queue
	 * @return WorkQueue:
	 */
	public static function factory($file) {
		if(!isset(self::$instances[$file])){
			self::$instances[$file] = new WorkQueue($file);
		}
		return self::$instances[$file];
	}

	/**
	 * Remove the WorkQueue instance from the static instances.
	 */
	public static function release($file) {
		self::$instances[$file]->db = null;
		unset(self::$instances[$file]);
	}

	/**
	 * Create a new job.  Does not start the job.
	 *
	 * @param string $bootstrap the bootstrap file to use for workers
	 * @param number $workers number of workers to use
	 * @return number job id assigned
	 */
	public function add_job($bootstrap, $workers=3) {
		$this->db->beginTransaction();
		$this->db->execOne("INSERT INTO jobs (bootstrap, 'workers') VALUES (?, ?)", array($bootstrap, $workers));
		$jid = $this->db->lastInsertId();
		$this->db->commit();

		$this->log("Created job with id ${jid}");
		return (int) $jid;
	}

	/**
	 * Add a task to a job.
	 *
	 * @param number $jid job id
	 * @param string $method callable method
	 * @param array $params params to pass to the method
	 * @param number $priority priority group - higher executes later
	 */
	public function add_task($jid, $method, $params=array(), $priority=0, $retry=3) {
		// check the job exists
		$job = $this->get_job($jid);

		$this->db->beginTransaction();
		$this->db->execOne(
				"INSERT INTO tasks (job_id, method, params_data, priority, retry, status) VALUES (?, ?, ?, ?, ?, 0)",
				array($jid, $method, serialize($params), $priority, $retry)
		);
		$task_id = $this->db->lastInsertId();
		$this->db->commit();

		$this->log("Added task {$task_id}: {$method}()");
		return $task_id;
	}

	/**
	 * Retrieve job info
	 *
	 * @param number $jid
	 * @return array
	 */
	public function get_job($jid) {
		try {
			return $this->db->fetchOne("SELECT * FROM jobs WHERE id=?", array($jid));
		} catch(OutOfBoundsException $nfe) {
			throw new WorkQueueException("No such job: {$jid}");
		}
	}

	/**
	 * Get job tasks, optionally limited to one priority level.
	 * Will block until all tasks have completed.
	 *
	 * @param number $jid
	 * @param number $priority
	 * @param number $timeout;
	 */
	public function get_tasks($jid, $priority=false, $timeout=300) {
		$criteria = "job_id=?";
		$params = array($jid);
		if($priority !== false) {
			$criteria .= " AND priority=?";
			$params[] = $priority;
		}

		return $this->task_query($criteria, $params);
	}

	public function get_task_set($task_ids, $timeout=300) {

	}

	public function task_query($criteria, $params, $timeout=300) {
		$check = "SELECT COUNT(id) c FROM tasks WHERE {$criteria} AND status<2";

		# wait for all tasks to complete (up to timeout)
		while($timeout > 0) {
			$result = $this->db->fetchOne($check, $params);
			if($result['c'] == 0) break;
			$this->log("Waiting for {$result['c']} tasks to complete");
			usleep($this->poll_interval * 1000000);
			$timeout--;
		}
		if($timeout==0) throw new WorkQueueTimeoutException("Tasks failed to complete in time");

		$query = $this->db->prepare("SELECT * FROM tasks WHERE {$criteria} ORDER BY id");
		$query->execute($params);

		$result = $query->fetchAll();
		for($i=0, $c=count($result); $i<$c; $i++) {
			$result[$i]['result'] = unserialize($result[$i]['result_data']);
		}
		return $result;
	}

	/**
	 * Get a list of results for a a job, optionally limited to one priority level.
	 * Will block until all results are ready.
	 *
	 * @param number $jid job id
	 * @param number $priority limit to a specific priority queue
	 * @param number $timeout seconds to block before raising WorkQueueTimeoutException
	 * @throws WorkQueueTimeoutException
	 * @throws Exception if any of the tasks failed
	 */
	public function get_results($jid, $priority=false, $timeout=300, $throw=true) {
		$tasks = $this->get_tasks($jid, $priority, $timeout);
		$result = array();
		foreach($tasks as $task) {
			if($throw && $task['status']==self::STATUS_FAILED) throw $task['result'];
			$result[] = $task['result'];
		}
		return $result;
	}

	/**
	 * Get a single completed task.
	 * Will block until task is completed.
	 *
	 * @param number $task_id
	 * @param number $timeout
	 * @throws OutOfBoundsException
	 * @throws WorkQueueTimeoutException
	 * @return string
	 */
	public function get_task($task_id, $timeout=300) {
		$query = $this->db->prepare("SELECT * FROM tasks WHERE id=?");
		for($i=0; $i<$timeout; $i++) {
			$query->execute(array($task_id));
			$task = $query->fetch();
			$query->closeCursor();

			if(!$task) throw new OutOfBoundsException("No such task: {$task_id}");
			if($task['status'] > self::STATUS_RUNNING) {
				$task['result'] = unserialize($task['result_data']);
				return $task;
			}
			usleep($this->poll_interval * 1000000);
		}
		throw new WorkQueueTimeoutException("Task failed to complete in time");
	}

	/**
	 * Get the result for a single completed task.
	 * Will block until task is completed.
	 *
	 * @param unknown $task_id
	 * @param number $timeout
	 * @param string $throw
	 * @throws string
	 * @return Ambiguous
	 */
	public function get_result($task_id, $timeout=300, $throw=true) {
		$task = $this->get_task($task_id, $timeout);
		if($throw && $task['status']==self::STATUS_FAILED) throw $task['result'];
		return $task['result'];
	}

	/**
	 * Get the current status for a job
	 *
	 * @param number $job job id
	 * @return array containing 'workers', 'tasks' and 'stats' elements.
	 */
	public function status($job) {
		$result = array(
				'workers' => array(),
				'tasks' => array(),
				'stats' => array('tasks' => 0, 'workers' => 0)
		);

		$result['job'] = $this->get_job($job);

		$result['job']['running'] = ($result['job']['pid'] && $this->is_running($result['job']['pid']));

		// workers
		$query = $this->db->prepare("SELECT * FROM workers WHERE job_id=?");
		$query->execute(array($job));
		while($row = $query->fetch()) {
			$result['workers'][$row['pid']] = $this->is_running($row['pid']) ? "running" : "stopped";
			$result['stats']['workers']++;
		}

		// tasks
		$query = $this->db->prepare("SELECT * FROM tasks WHERE job_id=? ORDER BY priority, id");
		$query->execute(array($job));
		$stats = array(
				self::STATUS_QUEUED => 0,
				self::STATUS_RUNNING => 0,
				self::STATUS_FAILED => 0,
				self::STATUS_SUCCESS => 0,
		);
		while($row = $query->fetch()) {
			$stats[$row['status']]++;
			$result['stats']['tasks']++;
			$result['tasks'][] = $row;
		}

		$result['stats']['queued'] = $stats[self::STATUS_QUEUED];
		$result['stats']['running'] = $stats[self::STATUS_RUNNING];
		$result['stats']['failed'] = $stats[self::STATUS_FAILED];
		$result['stats']['success'] = $stats[self::STATUS_SUCCESS];

		// do some calculations
		$result['stats']['progress'] = (float) $stats[self::STATUS_SUCCESS] / $result['stats']['tasks'];
		return $result;
	}

	/**
	 * Bootstraps the current process ready for processing tasks.
	 * @param number $jid job id
	 * @throws Exception
	 */
	public function bootstrap($jid) {
		$job = $this->get_job($jid);
		$this->log("Bootstrapping with {$job['bootstrap']}");
		require_once $job['bootstrap'];
	}

	/**
	 * Spawn an external process
	 *
	 * @param string $cmd command to execute
	 * @param string $output file to send output to (including stderr)
	 * @return number process id spawned
	 */
	private function spawn($cmd, $output="/dev/null") {
		$shell = sprintf("%s > \"%s\" 2>&1 & echo $!", $cmd, $output);
		#$this->log("Full command: {$shell}");
		$pid = (int) rtrim(shell_exec($shell));
		if(!$pid) throw new Exception("Failed to spawn process");
		return $pid;
	}

	/**
	 * Spawns a WorkQueue instance
	 * @param string $action one of the command line actions
	 * @return number process id spawned
	 */
	private function spawn_workqueue($job, $action, $output="/dev/null") {
		$command = "php \"" . __FILE__ . "\" \"{$this->path}\" {$job} {$action}";
		return $this->spawn($command, $output);
	}

	/**
	 * Check if a process is running
	 * @param number $pid process id
	 * @return boolean
	 */
	private function is_running($pid) {
		$result = shell_exec(sprintf('ps %d', $pid));
		if(count(explode("\n", $result)) > 2) return true;
		return false;
	}

	private function kill($pid) {
		$result = shell_exec(sprintf('kill %d', $pid));
		var_dump($result);
		return true;
	}

	/**
	 * Run a job by spawning and monitoring workers.
	 *
	 * @param number $jid job id
	 * @param string $daemon if true then don't exit when queue is empty
	 */
	public function run_job($jid, $daemon=false) {
		$this->db->beginTransaction();

		$job = $this->get_job($jid);

		// check there isn't another manager running
		if($job['pid']) {
			if($this->is_running($job['pid'])) {
				$this->log("Process {$job['pid']} already managing this queue");
				exit(1);
			}
		}

		// claim the job
		$this->db->execOne("UPDATE jobs SET pid=? WHERE id=?", array($this->pid, $jid));

		// clear previous half run jobs
		$this->db->execOne("UPDATE tasks SET pid=NULL, status=0 WHERE job_id=? AND status=1", array($jid));

		$this->db->commit();

		$this->log("Starting job {$jid} ({$job['workers']} workers)");


		// prepare statements for efficiency
		$delete_worker = $this->db->prepare("DELETE FROM workers WHERE id=?");
		$insert_worker = $this->db->prepare("INSERT INTO workers (job_id, pid) VALUES (?, ?)");
		$clear_tasks = $this->db->prepare("UPDATE tasks SET pid=NULL, status=0 WHERE job_id=? AND pid=? AND status=1");

		$spawn_limit = $job['workers'] + $this->spawn_limit;

		$running = true;
		$spawned = 0;
		while(true) {

			// wrap entire check in transaction
			$this->db->beginTransaction();

			// get a count of remaining tasks
			$result = $this->db->fetchOne("SELECT COUNT(id) c FROM tasks WHERE job_id=? AND status<2", array($jid));
			$remaining = $result['c'];
			$this->log("{$remaining} tasks remaining");

			// clear out old worker entries
			$query = $this->db->prepare("SELECT * FROM workers WHERE job_id=?");
			$query->execute(array($jid));
			$workers = $query->fetchAll();
			$active = 0;
			foreach($workers as $worker) {
				if($this->is_running($worker['pid'])) {
					$active += 1;
				} else {
					$this->log("Worker {$worker['pid']} has exited");
					$delete_worker->execute(array($worker['id']));
					$clear_tasks->execute(array($jid, $worker['pid']));
				}
			}

			// check if we can exit
			if($remaining == 0 && !$daemon) {
				$this->db->commit();
				$this->log("Job {$jid} finished");
				break;
			}

			$required = min($job['workers'], $remaining);
			$this->log("{$required} processes required, {$active} currently active");

			// start workers
			while($active < $required) {

				if($spawned >= $spawn_limit) {
					$this->log("Limit of {$spawn_limit} processes reached - bailing!");
					$this->db->commit();
					$this->stop_job($jid);
					throw new WorkQueueException("Spawn limit of {$this->spawn_limit} reached");
				}
				$spawned++;
				$pid = $this->spawn_workqueue($jid, 'worker', "{$this->log_dir}/job-{$jid}_{$spawned}.log");
				$insert_worker->execute(array($jid, $pid));
				$this->log("Spawned worker: {$pid} [{$spawned}]");
				$active++;
			}

			$this->db->commit();

			// sleep till next check
			usleep($this->poll_interval * 1000000);
		}

		// release the job
		$this->db->beginTransaction();
		$this->db->execOne("UPDATE jobs SET pid=NULL WHERE id=?", array($jid));
		$this->db->commit();
		$this->log("Finished job {$jid}");

		// check the result
		$result = $this->db->fetchOne("SELECT COUNT(id) c FROM tasks WHERE job_id=? AND status<>?", array($jid, self::STATUS_SUCCESS));
		if($result['c'] > 0) throw new WorkQueueException("{$result['c']} tasks failed");

		return true;
	}

	/**
	 * Execute a single task for a job.
	 *
	 * Exceptions in the task are caught and the task will be retried up to MAX_ATTEMPTS(3)
	 * with a gap of RETRY_DELAY(5) seconds.  If it still does not succeed the exception is
	 * stored as the result and the status is marked as STATUS_FAILED.
	 *
	 * @param number $jid job id
	 * @throws NoMoreWork if there is no work available
	 * @throws RuntimeException
	 */
	public function run_task($jid) {
		$this->log("Fetching work for job ${jid}");
		$this->db->beginTransaction();
		$this->db->execOne(
				"UPDATE tasks set pid=?, status=1 WHERE job_id=? AND status=0 ORDER BY priority, id LIMIT 1",
				array($this->pid, $jid)
		);
		$this->db->commit();

		$query = $this->db->prepare("SELECT * FROM tasks WHERE job_id=? AND pid=? AND status=?");
		$query->execute(array($jid, $this->pid, self::STATUS_RUNNING));
		$result = $query->fetchAll();

		if(count($result) == 0) {
			$this->log("No more work");
			throw new NoMoreWork();
		}
		if(count($result) > 1) {
			throw new RuntimeException("More than one item assigned!");
		}


		// run the actual job
		$task = $result[0];
		$this->log("Got task {$task['id']}");

		for($i=$task['retry']; $i>0; $i--) {
			try {
				$this->log("Executing {$task['method']}");
				$result = call_user_func_array($task['method'], unserialize($task['params_data']));
				$this->log("Result: {$result}");
				$status = self::STATUS_SUCCESS;
				$this->log("Finished task {$task['id']}");
				break;
			} catch(Exception $e) {
				$result = $e;
				$status = self::STATUS_FAILED;
				$this->log("Exception while executing {$task['id']}");
				$this->log($e->getTraceAsString());
				if($i > 1) {
					$this->log("Trying again in {$this->retry_delay} seconds...");
					usleep($this->retry_delay * 1000000);
				} else {
					$this->log("Retry limit reached - returning failure");
				}
			}
		}
		// release the task
		$this->db->beginTransaction();
		$this->db->execOne("UPDATE tasks SET status=?, result_data=? WHERE id=?", array($status, serialize($result), $task['id']));
		$this->db->commit();
		$this->log("Released task {$task['id']}");
	}

	/**
	 * Spawns a new manager in the background.
	 *
	 * @param number $job job id
	 */
	public function run_background($job) {
		$pid = $this->spawn_workqueue($job, 'process', "{$this->log_dir}/job-{$job}_0.log");
		$this->log("Spawned background processor with pid ${pid}");
		return $pid;
	}

	public function stop_job($jid) {
		$this->log("Stopping job {$jid}");
		$job = $this->get_job($jid);

		$this->db->beginTransaction();

		// kill all the workers
		$query = $this->db->prepare("SELECT * FROM workers WHERE job_id=?");
		$query->execute(array($jid));
		$this->log("Terminating workers");
		while($row = $query->fetch()) {
			if($this->is_running($row['pid'])) {
				$this->log("Shutting down worker {$row['pid']}");
				$this->kill($row['pid']);
			}
		}
		$this->db->execOne("DELETE FROM workers WHERE job_id=?", array($jid));

		global $job_stopped;
		$this->db->execOne(
				"UPDATE tasks SET status=?, result_data=? WHERE job_id=? AND status<2",
				array(self::STATUS_FAILED, $job_stopped, $jid)
		);

		$this->db->execOne("UPDATE jobs SET pid=NULL WHERE id=?", array($jid));
		$this->db->commit();
		$this->log("Job {$jid} stopped");
	}

	/**
	 * Requeue any failed items for a job.
	 *
	 * @param number $jid job id
	 */
	public function requeue_failed($jid) {
		$this->db->beginTransaction();
		$this->db->execOne("UPDATE tasks SET pid=NULL, status=0 WHERE job_id=? AND status=?", array($jid, self::STATUS_FAILED));
		#$query = $this->db->prepare("UPDATE tasks SET pid=NULL, status=0 WHERE job_id=? AND status=?");
		#$query->execute(array($jid, self::STATUS_FAILED));
		$this->db->commit();
		$this->log("Requeued {$query->rowCount()} tasks");
	}

	/**
	 * Write a log message to stderr with process and time information.
	 *
	 * @param string $message
	 */
	public function log($message) {
		if(!$this->debug) return;
		fprintf(STDERR, "%6d %7.3f: %s\n", $this->pid, microtime(true)-$this->start_time, $message);
	}

	/**
	 * Command line interface
	 *
	 * @param number $argc
	 * @param array $argv
	 */
	public static function main($argc, $argv) {
		if($argc != 4) WorkQueue::usage(STDERR);

		$queue = WorkQueue::factory($argv[1]);
		$queue->debug = true;
		$job = $argv[2];

		switch($argv[3]) {
			case 'process':
				$queue->log("------------------");
				$queue->run_job($job);
				break;
			case 'daemon':
				$queue->log("------------------");
				$queue->run_job($job, true);
				break;
			case 'worker':
				WorkQueue::$instance = $queue;
				WorkQueue::$job = $job;

				$queue->log("------------------");
				$queue->bootstrap($job);

				try {
					while(true) {
						$queue->run_task($job);
					}
				} catch(NoMoreWork $mnw) {
					exit(0);
				}
				break;
			case 'status':
				$status = $queue->status($job);

				print "\n";
				if($status['job']['running']) {
					print "Job is running: {$status['job']['pid']}\n";
				} else {
					print "Job is not currently running\n";
				}

				print "\n";
				$status['stats']['progress'] = sprintf("%3d%%", $status['stats']['progress'] * 100);
				foreach($status['stats'] as $key => $value) printf("%10s: %s\n", $key, $value);

				print "\n";
				break;
			case 'requeue':
				$queue->requeue_failed($job);
				break;
			default:
				WorkQueue::usage(STDERR);
		}
	}

	/**
	 * Usage information
	 *
	 * @param resource $stream
	 */
	public static function usage($stream) {
		fputs($stream, "\n\n");
		fputs($stream, "Usage: php workqueue.php <queue> <job> <cmd>\n\n");
		fputs($stream, "  queue : file to store queue information in\n");
		fputs($stream, "  job   : job id (integer)\n");
		fputs($stream, "  cmd   : one of\n");
		fputs($stream, "    process : run as job manager, spawning workers as required\n");
		fputs($stream, "    daemon  : run as a daemon for a job (waits for tasks)\n");
		fputs($stream, "    stop    : terminate manager and workers for job\n");
		fputs($stream, "    worker  : run a single worker for a job\n");
		fputs($stream, "    status  : print job status\n");
		fputs($stream, "    requeue : requeue failed tasks for a job\n");
		fputs($stream, "\n");
		exit(1);
	}
}

class PDONotFoundException extends PDOException {}

class NoMoreWork extends Exception {}

class WorkQueueException extends Exception {}

class WorkQueueTimeoutException extends WorkQueueException {}

// make file executable
if ( realpath(__FILE__) == realpath($_SERVER["SCRIPT_FILENAME"]) ) {
	WorkQueue::main($argc, $argv);
}