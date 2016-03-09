<?php

class Resque_Queue_Redis implements Resque_Queue_Interface
{
	/**
	 * Push a job to the end of a specific queue. If the queue does not
	 * exist, then create it as well.
	 *
	 * @param string $queue The name of the queue to add the job to.
	 * @param array $item Job description as an array to be JSON encoded.
	 */
	public static function push($queue, $item, $method='last')
	{
        if ($method == 'last') {
            $redisMethod = 'rpush';
        } elseif ($method == 'first') {
            $redisMethod = 'lpush';
        }
        Resque::redis()->$method('queue:' . $queue, json_encode($item));
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
        Resque::push($queue, $item, 'first');
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
		$item = Resque::redis()->lpop('queue:' . $queue);
		if(!$item) {
			return;
		}

		return json_decode($item, true);
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
		return Resque::redis()->llen('queue:' . $queue);
	}
}