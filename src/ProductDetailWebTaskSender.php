<?php

/**
 * @Copyright: Copyright :copyright: 2019 by IBPort. All rights reserved.
 * @Author: Neal Wong
 * @Email: ibprnd@gmail.com
 */

namespace AmazonProductLoader;

use Predis\Client;


class ProductDetailWebTaskSender {
	private $redisConfig;
	private $redisClient;

    public function __construct($host, $port = 6379, $password = NULL, $database = 0) {
    	$this->redisConfig = [
    		'host' => $host,
    		'port' => $port,
    		'password' => $password,
    		'database' => $database
    	];

    	$this->redisClient = new Client($this->redisConfig);
    }

    public function sendTask($asins, $country = 'us', $taskKey = 'detail_page:start_urls') {
		$marketplace = strtoupper($country);

		foreach ($asins as $asin) {
			$taskId = $marketplace . ':' . $asin;
			$this->redisClient->lpush($taskKey, $taskId);
		}
	}
}
