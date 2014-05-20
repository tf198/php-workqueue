<?php
include "workqueue.php";

class WorkQueueTest extends PHPUnit_Framework_TestCase {

	const QUEUE_NAME = 'test.queue';

	function setUp() {
		@unlink(self::QUEUE_NAME);
		$this->queue = WorkQueue::factory(self::QUEUE_NAME);
		$this->queue->poll_interval = 0.25;
	}

	function tearDown() {
		WorkQueue::release(self::QUEUE_NAME);
		unlink(self::QUEUE_NAME);
	}

	/**
	 * @expectedException WorkQueueException
	 */
	function testBadJobStatus() {
		$this->queue->status(23);
	}

	/**
	 * @expectedException WorkQueueException
	 */
	function testBadJobAddTask() {
		$this->queue->add_task(23, 'Test::hello', array('Bob'));
	}

	/**
	 * Basic queue with a single processor
	 */
	function testSingleRunner() {
		$job = $this->queue->add_job('tests/tasks.php', 1);
		$this->assertEquals($job, 1);

		$task_bob = $this->queue->add_task($job, 'Test::hello', array('Bob'));
		$task_fred = $this->queue->add_task($job, 'Test::hello', array('Fred'));

		$status = $this->queue->status($job);
		$this->assertFalse($status['job']['running']);

		$this->queue->run_job($job);

		$status = $this->queue->status($job);
		$this->assertFalse($status['job']['running']);

		$result = $this->queue->get_tasks($job);
		$this->assertEquals(count($result), 2);
		$this->assertEquals($result[0]['result'], 'Hello Bob');
		$this->assertEquals($result[1]['result'], 'Hello Fred');

		$this->assertEquals($this->queue->get_result($task_fred), 'Hello Fred');
	}

	/**
	 * Force a spawn limit exception by using a bad bootstrap
	 *
	 * @expectedException WorkQueueException
	 * @expectedExceptionMessage Spawn limit of 3 reached
	 */
	function testSpawnLimit() {
		$job = $this->queue->add_job('tests/foo.php', 1);
		$this->queue->add_task($job, 'Test::foo');
		$this->queue->run_job($job);
	}

	/**
	 * Run a multi stage calculation and check the results.
	 * Should block until background processing finishes.
	 */
	function testTaskResults() {
		$job = $this->queue->add_job('tests/tasks.php', 2);
		$this->queue->add_task($job, 'Test::add_two', array(3), 1);
		$this->queue->add_task($job, 'Test::add_two', array(6), 1);
		$task = $this->queue->add_task($job, 'Test::add_results', array(1), 2);

		$this->queue->run_background($job);

		$this->assertEquals($this->queue->get_result($task), 13);
	}

	function testTaskSetResults() {
		$job = $this->queue->add_job('tests/tasks.php', 2);
		$andy = $this->queue->add_task($job, 'Test::Hello', array('Andy'));
		$bob = $this->queue->add_task($job, 'Test::Hello', array('Bob'));
		$charlie = $this->queue->add_task($job, 'Test::Hello', array('Charlie'));

		$this->queue->run_job($job);

		$this->assertEquals($this->queue->get_result_set(array($andy, $charlie)), array('Hello Andy', 'Hello Charlie'));
	}

	/**
	 * Run a task that sometimes throws a catchable error
	 */
	function testDodgyTask() {
		$job = $this->queue->add_job('tests/tasks.php');
		$this->queue->add_task($job, 'Test::hello', array('Bob'));
		$dodgy = $this->queue->add_task($job, 'Test::dodgy_task');

		$this->queue->run_job($job);

		$this->assertEquals($this->queue->get_result($dodgy), 'Succeeded on go 2');
	}

	/**
	 */
	function testDodgyFatalTask() {
		$job = $this->queue->add_job('tests/tasks.php');
		$this->queue->add_task($job, 'Test::hello', array('Bob'));
		$dodgy = $this->queue->add_task($job, 'Test::dodgy_task', array(true));

		try {
			$this->queue->run_job($job);
			$this->fail("Expected WorkQueueException not thrown");
		} catch(WorkQueueException $wqe) {
		}

		try {
			$this->queue->get_result($dodgy);
			$this->fail("Expected WorkQueueException not thrown");
		} catch(WorkQueueException $wqe) {
		}
	}

	/**
	 * A rather inefficient parallel factorization routine.
	 * Matched workers to cores (on my test system) and a decent step size.
	 * Should run in 15-20 secs.
	 */
	function testFactors() {
		$job = $this->queue->add_job('tests/tasks.php', 4);

		$target = 638524527;
		$step = 1000000;

		for($i=1; $i<$target/2; $i+=$step) {
				$this->queue->add_task($job, 'Test::factors', array($target, $i, $i+$step));
		}

		#$this->queue->debug = true;
		$this->queue->run_job($job);

		$result = call_user_func_array('array_merge', $this->queue->get_results($job));
		#print "Factors: " . implode(', ', $result);
		$this->assertEquals($result, array(1, 3, 8623, 24683, 25869, 74049, 212841509));
	}

	/**
	 * Run factorisation but with non-optimal parameters to load test
	 * the database.
	 */
	function testHeavyLoad() {
		$job = $this->queue->add_job('tests/tasks.php', 10);

		$target = 638529;
		$step = 1000;

		for($i=1; $i<$target/2; $i+=$step) {
			$this->queue->add_task($job, 'Test::factors', array($target, $i, $i+$step));
		}

		#$this->queue->debug = true;
		$this->queue->run_job($job);

		$result = call_user_func_array('array_merge', $this->queue->get_results($job));
		#print "Factors: " . implode(', ', $result);
		$this->assertEquals($result, array(1, 3, 212843));
	}

}