PHP Workqueue
-------------

Simple SQLite based workqueue for running concurrent PHP tasks in the
background.  Can be used as a simple processor (sending emails, processing
uploaded images etc) or to run tasks in parallel.

**Still alpha code - do not use in production**

Features
========

* API for monitoring and manipulating jobs.
* Command line interface for monitoring and manipulating jobs.
* Job continuation in the event of power loss etc.
* Custom parameters per job (number of workers, bootstrap file).
* Full logging from workers.
* Priority based task groups.

Example
=======

``tasks.php``:

````php
<?php
class Background {
  static function say_hi($name) {
    return "Hello {$name}";
  }
}
?>
````

``runner.php``:

```php
<?php
import "workqueue.php";

// multiple jobs can share the same queue
$queue = WorkQueue::factory('test.queue');

/*
Each job has a bootstrap file which will be loaded by the workers.
It can either contain the tasks or be a generic AutoLoader.
The job also has a maximum number of workers - in this case 2.
*/
$job = $queue->add_job('tasks.php', 2);

// Create some tasks
$queue->add_task($job, 'Background::say_hi', array('Bob'));
$queue->add_task($job, 'Background::say_hi', array('Fred'));

/*
Here we run the job in the current process - note this just starts the workers
and makes sure they dont die.
You can also run asynchronously with run_background($job)
*/
$queue->run_job($job);

// Fetching results waits for all to complete.
var_dump($queue->get_results($job));
// output: array('Hello Bob', 'Hello Fred');
?>
```
