PHP Workqueue
-------------

Simple SQLite based workqueue for running concurrent PHP tasks in the
background.  Can be used as a simple processor (sending emails, processing
uploaded images etc) or to run tasks in parallel.

**Still alpha code - do not use in production**

Features
========

* Command line interface for monitoring and manipulating task queues.
* Job continuation in the event of power loss etc.
* Custom parameters per job (number of workers, bootstrap file etc)

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

$queue = WorkQueue::factory('test.queue');
$job = $queue->add_job('tasks.php', 2); # 2 workers for the job

$queue->add_task($job, 'Background::say_hi', array('Bob'));
$queue->add_task($job, 'Background::say_hi', array('Fred'));

// Run the job in the current process, can also spawn a separate process if required
$queue->run_job($job);

var_dump($queue->get_results($job));
// array('Hello Bob', 'Hello Fred');
?>
```
