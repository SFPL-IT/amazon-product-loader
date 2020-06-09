<?php

/**
 * @Copyright: Copyright :copyright: 2019 by IBPort. All rights reserved.
 * @Author: Neal Wong
 * @Email: ibprnd@gmail.com
 */

namespace AmazonProductLoader;

class ProductDetailTaskSender {
	private $celery;
	private $queuePattern = 'SFPL_MatchingProduct_%s';
	private $batchSize = 10;
	public $task = 'mws_collector.tasks.task_update_matching_product.update_matching_product';

    public function __construct(
    	$host, $username, $password, $vhost,
    	$exchange = 'celery', $routingKey = 'celery') {
		$this->celery = new \Celery\Celery(
			$host, $username, $password, $vhost, $exchange, $routingKey);
    }

    public function sendTask($asins, $country = 'us', $async = TRUE) {
		$marketplace = strtoupper($country);
		$routingKey = sprintf($this->queuePattern, $marketplace);
        $asinChunks = array_chunk($asins, $this->batchSize);

        $result = [];
        foreach ($asinChunks as $asinsBatch) {
			$args = array($marketplace, $asinsBatch);
			$result[] = $this->celery->PostTask($this->task, $args, $async, $routingKey);
        }

        return $result;
	}
}
