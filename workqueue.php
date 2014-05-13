<?php
/**
 * A simple (one file) task queue for background processing.  Can be
 * used for parallel processing or as a daemonised service.
 *
 * @licence GPLv3
 * @author Tris Forster
 */
class WorkQueue {

	const MAX_ATTEMPTS = 3;
	const RETRY_DELAY = 5;

	const STATUS_QUEUED = 0;
	const STATUS_RUNNING = 1;
	const STATUS_FAILED = 2;
	const STATUS_SUCCESS = 3;

	private $db;

	private static $instances = array();

	private function __construct($file) {

		$this->pid = getmypid();
		$this->start_time = microtime(true);

		$created = false;
		if(file_exists($file)) {
			$this->path = realpath($file);
		} else {
			$this->path = realpath(dirname($file)) . "/" . basename($file);
			$created = true;
		}

		$this->db = new PDO('sqlite:' . $this->path);
		$this->db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		$this->db->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);

		if($created) {
			$this->db->exec("CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, bootstrap VARCHAR, pid INTEGER, workers INTEGER, started INTEGER)");
			$this->db->exec("CREATE TABLE tasks (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, pid INTEGER, method VARCHAR, params TEXT, priority INTEGER, status INTEGER, retry INTEGER, result TEXT)");
			$this->db->exec("CREATE TABLE workers (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, pid INTEGER)");
		}

		$this->log_dir = $this->path . ".logs";
		if(!is_dir($this->log_dir)) mkdir($this->log_dir);
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
	 * Create a new job.  Does not start the job.
	 *
	 * @param string $bootstrap the bootstrap file to use for workers
	 * @param number $workers number of workers to use
	 * @return number job id assigned
	 */
	public function add_job($bootstrap, $workers=3) {
		$this->db->beginTransaction();
		$query = $this->db->prepare("INSERT INTO jobs (bootstrap, 'workers') VALUES (?, ?)");
		$query->execute(array($bootstrap, $workers));
		$jid = $this->db->lastInsertId();
		$this->db->commit();

		$this->log("Created job with id ${jid}");
		return (int) $jid;
	}

	/**
	 * Add a task to a job.
	 *
	 * @param number $job job id
	 * @param string $method callable method
	 * @param array $params params to pass to the method
	 * @param number $priority task priority - higher executes later
	 */
	public function add_task($job, $method, $params=array(), $priority=0, $retry=3) {
		$this->db->beginTransaction();
		$query = $this->db->prepare("INSERT INTO tasks (job_id, method, params, priority, retry, status) VALUES (?, ?, ?, ?, ?, 0)");
		$query->execute(array($job, $method, serialize($params), $priority, $retry));
		$this->db->commit();

		$this->log("Added task: {$method}()");
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

		$query = $this->db->prepare("SELECT * FROM jobs WHERE id=?");
		$query->execute(array($job));
		$result['job'] = $query->fetch();
		$query->closeCursor();

		if(!$result['job']) throw new Exception("No such job: {$job}");

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
	 * @param number $job job id
	 * @throws Exception
	 */
	public function bootstrap($job) {
		$query = $this->db->prepare("SELECT bootstrap FROM jobs WHERE id=?");
		$query->execute(array($job));
		$file = $query->fetchColumn(0);
		$query->closeCursor();
		if(!$file) throw new Exception("Failed to retrieve job");
		$this->log("Bootstrapping with {$file}");
		require_once $file;
	}

	/**
	 * Spawn an external process
	 *
	 * @param string $cmd command to execute
	 * @param string $output file to send output to (including stderr)
	 * @return number process id spawned
	 */
	private function spawn($cmd, $output="/dev/null") {
		$pid = (int) rtrim(shell_exec(sprintf("%s > \"%s\" 2>&1 & echo $!", $cmd, $output)));
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

	/**
	 * Run a job by spawning and monitoring workers.
	 *
	 * @param number $jid job id
	 * @param string $daemon if true then don't exit when queue is empty
	 */
	public function run_job($jid, $daemon=false) {
		$this->db->beginTransaction();

		$query = $this->db->prepare("SELECT * FROM jobs WHERE id=?");
		$query->execute(array($jid));
		$job= $query->fetch();
		$query->closeCursor();

		if(!$job) {
			$this->log("No such job: ${jid}");
			exit(1);
		}

		// check there isn't another manager running
		if($job['pid']) {
			if($this->is_running($job['pid'])) {
				$this->log("Process {$job['pid']} already managing this queue");
				exit(1);
			}
		}

		// claim the job
		$query = $this->db->prepare("UPDATE jobs SET pid=? WHERE id=?");
		$query->execute(array($this->pid, $jid));

		// clear previous half run jobs
		$query = $this->db->prepare("UPDATE tasks SET pid=NULL, status=0 WHERE job_id=? AND status=1");
		$query->execute(array($jid));

		$this->db->commit();

		$this->log("Starting job {$jid} ({$job['workers']} workers)");

		// todo: add pid to job table and bail if already managed

		$delete_worker = $this->db->prepare("DELETE FROM workers WHERE id=?");
		$insert_worker = $this->db->prepare("INSERT INTO workers (job_id, pid) VALUES (?, ?)");
		$clear_tasks = $this->db->prepare("UPDATE tasks SET pid=NULL, status=0 WHERE job_id=? AND pid=? AND status=1");

		$spawn_limit = $job['workers'] * 3;

		$running = true;
		$spawned = 0;
		while(true) {

			// wrap entire check in transaction
			$this->db->beginTransaction();

			// get a count of remaining tasks
			$query = $this->db->prepare("SELECT COUNT(id) FROM tasks WHERE job_id=? AND status<2");
			$query->execute(array($jid));
			$remaining = $query->fetchColumn(0);
			$query->closeCursor();
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
					break 2;
				}
				$spawned++;
				$pid = $this->spawn_workqueue($jid, 'worker', "{$this->log_dir}/job-{$jid}_{$spawned}.log");
				$insert_worker->execute(array($jid, $pid));
				$this->log("Spawned worker: {$pid} [{$spawned}]");
				$active++;
			}

			$this->db->commit();

			// sleep till next check
			sleep(5);
		}

		// release the job
		$this->db->beginTransaction();
		$query = $this->db->prepare("UPDATE jobs SET pid=NULL WHERE id=?");
		$query->execute(array($jid));
		$this->db->commit();
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
		$query = $this->db->prepare("UPDATE tasks set pid=?, status=1 WHERE job_id=? AND status=0 ORDER BY priority, id LIMIT 1");
		$this->db->beginTransaction();
		$result = $query->execute(array($this->pid, $jid));
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
				$result = call_user_func_array($task['method'], unserialize($task['params']));
				$status = self::STATUS_SUCCESS;
				$this->log("Finished task {$task['id']}");
				break;
			} catch(Exception $e) {
				$result = $e;
				$status = self::STATUS_FAILED;
				$this->log("Exception while executing {$task['id']}");
				$this->log($e->getTraceAsString());
				if($i > 1) {
					$this->log("Trying again in " . self::RETRY_DELAY . " seconds...");
					sleep(self::RETRY_DELAY);
				} else {
					$this->log("Retry limit reached - returning failure");
				}
			}
		}
		// release the task
		$query = $this->db->prepare("UPDATE tasks SET status=?, result=? WHERE id=?");
		$this->db->beginTransaction();
		$query->execute(array($status, serialize($result), $task['id']));
		$this->db->commit();
		$this->log("Released task {$task['id']}");
	}

	/**
	 * Spawns a new manager in the background.
	 *
	 * @param number $job job id
	 */
	public function run_background($job) {
		$pid = $this->spawn_workqueue($job, 'process', "{$this->log_dir}/job-{$job}.log");
		$this->log("Spawned background processor with pid ${pid}");
		return $pid;
	}

	/**
	 * Requeue any failed items for a job.
	 *
	 * @param number $jid job id
	 */
	public function requeue_failed($jid) {
		$this->db->beginTransaction();
		$query = $this->db->prepare("UPDATE tasks SET pid=NULL, status=0 WHERE job_id=? AND status=?");
		$query->execute(array($jid, self::STATUS_FAILED));
		$this->log("Requeued {$query->rowCount()} tasks");
		$this->db->commit();
	}

	/**
	 * Write a log message to stderr with process and time information.
	 *
	 * @param string $message
	 */
	private function log($message) {
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
		fputs($stream, "    worker  : run a single worker for a job\n");
		fputs($stream, "    status  : print job status\n");
		fputs($stream, "    requeue : requeue failed tasks for a job\n");
		fputs($stream, "\n");
		exit(1);
	}
}

class NoMoreWork extends Exception {}

// make file executable
if ( basename(__FILE__) == basename($_SERVER["SCRIPT_FILENAME"]) ) {
	WorkQueue::main($argc, $argv);
}