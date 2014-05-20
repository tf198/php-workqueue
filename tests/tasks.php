<?php
WorkQueue::$instance->retry_delay = 0.1;
WorkQueue::$instance->poll_interval = 0.1;

class Test {

	static $exception_count = 0;

	static function hello($name) {
		return "Hello {$name}";
	}

	static function add_two($i) {
		sleep($i/10);
		return $i+2;
	}

	static function add_results($i) {
		$result = WorkQueue::$instance->get_results(WorkQueue::$job, $i);
		return array_sum($result);
	}

	static function dodgy_task($fatal=false) {
		self::$exception_count++;
		if(self::$exception_count > 1) return "Succeeded on go " . self::$exception_count;

		if($fatal) {
			Workqueue::$instance->log("Fatal error");
			exit(1);
		} else {
			WorkQueue::$instance->log("Catchable error");
			throw new RuntimeException("Catchable error");
		}

	}

	static function long_process($t=10) {
		sleep($t);
		return $t;
	}

	static function factors($target, $start, $end) {
		$result = array();
		for($i=$start; $i<$end; $i++) {
			if($target % $i == 0) $result[] = $i;
		}
		return $result;
	}

}