<?php
require_once dirname(__FILE__) . '/Resque/Event.php';
require_once dirname(__FILE__) . '/Resque/Exception.php';

/**
 * Base Resque class.
 *
 * @package		Resque
 * @author		Chris Boulton <chris@bigcommerce.com>
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Resque
{
	const VERSION = '1.2.5';

	/**
	 * @var Resque_Redis Instance of Resque_Redis that talks to redis.
	 */
	public static $redis = null;

	/**
	 * @var mixed Host/port conbination separated by a colon, or a nested
	 * array of server swith host/port pairs
	 */
	protected static $redisServer = null;

	/**
	 * @var int ID of Redis database to select.
	 */
	protected static $redisDatabase = 0;

	/**
	 * @var string namespace of the redis keys
	 */
	protected static $namespace = '';

	/**
	 * @var string password for the redis server
	 */
	protected static $password = null;

	/**
	 * @var int PID of current process. Used to detect changes when forking
	 *  and implement "thread" safety to avoid race conditions.
	 */
	protected static $pid = null;

    protected static $queueTypes = ['Resque_Queue_Redis'];
    protected static $queues = [];

	/**
	 * Given a host/port combination separated by a colon, set it as
	 * the redis server that Resque will talk to.
	 *
	 * @param mixed $server Host/port combination separated by a colon, or
	 *                      a nested array of servers with host/port pairs.
	 * @param int $database
	 */
	public static function setBackend($server, $database = 0, $namespace = 'resque', $password = null)
	{
		self::$redisServer   = $server;
		self::$redisDatabase = $database;
		self::$redis         = null;
		self::$namespace 	 = $namespace;
		self::$password 	 = $password;
	}

    public static function addQueueType($type) {
        if (class_exists($type) && in_array('Resque_Queue_Interface', class_implements($type, true))) {
            array_push(self::$queueTypes, $type);
        } else {
            throw new \Exception('QueueType does not exist or does not implement Resque_Queue_Interface');
        }
    }

    public static function isRedisQueue($queue)
    {
        return (self::queueClass($queue) === 'Resque_Queue_Redis');
    }

    public static function queueClass($queue)
    {
        if (!isset(self::$queues[$queue])) {
            self::queues();
        }
        return self::$queues[$queue];
    }

	/**
	 * Return an instance of the Resque_Redis class instantiated for Resque.
	 *
	 * @return Resque_Redis Instance of Resque_Redis.
	 */
	public static function redis()
	{
		// Detect when the PID of the current process has changed (from a fork, etc)
		// and force a reconnect to redis.
		$pid = getmypid();
		if (self::$pid !== $pid) {
			self::$redis = null;
			self::$pid   = $pid;
		}

		if(!is_null(self::$redis)) {
			return self::$redis;
		}

		$server = self::$redisServer;
		if (empty($server)) {
			$server = 'localhost:6379';
		}

		if(is_array($server)) {
			require_once dirname(__FILE__) . '/Resque/RedisCluster.php';
			self::$redis = new Resque_RedisCluster($server);
		}
		else {
			if (strpos($server, 'unix:') === false) {
				list($host, $port) = explode(':', $server);
			}
			else {
				$host = $server;
				$port = null;
			}
			require_once dirname(__FILE__) . '/Resque/Redis.php';
			$redisInstance = new Resque_Redis($host, $port, self::$password);
			$redisInstance->prefix(self::$namespace);
			self::$redis = $redisInstance;
		}

		if(self::$redisDatabase !== 0) {
			self::$redis->select(self::$redisDatabase);
		}

		return self::$redis;
	}

	/**
	 * Push a job to the end of a specific queue. If the queue does not
	 * exist, then create it as well.
	 *
	 * @param string $queue The name of the queue to add the job to.
	 * @param array $item Job description as an array to be JSON encoded.
	 */
	public static function push($queue, $item, $method='rpush')
	{
        if (is_array($queue)) {
            if (!isset($queue['strategy'])) {
                $queueStrategy = 'shortest';
            } else {
                $queueStrategy = $queue['strategy'];
            }
            switch ($queueStrategy) {
                case 'shortest':

                case 'random':
                default:
                   $destinationQueue = array_rand($queue['candidates']);
            }
        } else {
            $destinationQueue = $queue;
        }
        self::redis()->sadd('queues', $destinationQueue);
        call_user_func([self::queueClass($queue),'push'], $destinationQueue, $item, $method);
	}

	/**
	 * Push a job to the beginning of a specific queue. If the queue does not
	 * exist, then create it as well.
	 *
	 * @param string $queue The name of the queue to add the job to.
	 * @param array $item Job description as an array to be JSON encoded.
	 */
	public static function unshift($queue, $item)
	{
        call_user_func([self::queueClass($queue), 'unshift'], $queue, $item);
	}

	/**
	 * Pop an item off the end of the specified queue, decode it and
	 * return it.
	 *
	 * @param string $queue The name of the queue to fetch an item from.
	 * @return array Decoded item from the queue.
	 */
	public static function pop($queue)
	{
        $item = call_user_func([self::queueClass($queue),'pop'], $queue);
		if(empty($item)) {
			return;
		}

		return $item;
	}

	/**
	 * Return the size (number of pending jobs) of the specified queue.
	 *
	 * @param $queue name of the queue to be checked for pending jobs
	 *
	 * @return int The size of the queue.
	 */
	public static function size($queue)
	{
		return call_user_func([self::queueClass($queue), 'size'], $queue);
	}

	/**
	 * Create a new job and save it to the specified queue.
	 *
	 * @param string $queue The name of the queue to place the job in.
	 * @param string $class The name of the class that contains the code to execute the job.
	 * @param array $args Any optional arguments that should be passed when the job is executed.
	 * @param boolean $trackStatus Set to true to be able to monitor the status of a job.
	 *
	 * @return string
	 */
	public static function enqueue($queue, $class, $args = null, $trackStatus = false, $runNext = false)
	{
		require_once dirname(__FILE__) . '/Resque/Job.php';
		$result = Resque_Job::create($queue, $class, $args, $trackStatus, $runNext);
		if ($result) {
			Resque_Event::trigger('afterEnqueue', array(
				'class' => $class,
				'args'  => $args,
				'queue' => $queue,
			));
		}

		return $result;
	}

	/**
	 * Reserve and return the next available job in the specified queue.
	 *
	 * @param string $queue Queue to fetch next available job from.
	 * @return Resque_Job Instance of Resque_Job to be processed, false if none or error.
	 */
	public static function reserve($queue)
	{
		require_once dirname(__FILE__) . '/Resque/Job.php';
		return Resque_Job::reserve($queue);
	}

	/**
	 * Get an array of all known queues.
	 *
	 * @return array Array of queues.
	 */
	public static function queues()
	{
        $allQueues = [];
        foreach (self::$queueTypes as $queueType) {
            $queues = $queueType::queues();
            if (is_array($queues)) {
                $allQueues = array_merge($allQueues, array_combine($queues, array_fill(0, count($queues), $queueType)));
            }
        }
        self::$queues = $allQueues;
		return array_keys($allQueues);
	}
}
