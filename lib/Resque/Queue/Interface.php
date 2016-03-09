<?php

interface Resque_Queue_Interface {
	public static function push($queue, $item, $method='last');
	public static function unshift($queue, $item);
	public static function pop($queue);
	public static function size($queue);
}
