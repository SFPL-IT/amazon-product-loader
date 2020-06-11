<?php

/**
 * @Copyright: Copyright :copyright: 2019 by IBPort. All rights reserved.
 * @Author: Neal Wong
 * @Email: ibprnd@gmail.com
 */

namespace AmazonProductLoader;

class ProductCategoriesTaskSender {
	private $celery;
	private $queuePattern = 'SFPL_ProductCategories_%s';
	public $task = 'mws_collector.tasks.task_update_product_categories.update_product_categories';

    public function __construct(
    	$host, $username, $password, $vhost,
    	$exchange = 'celery', $routingKey = 'celery') {
		$this->celery = new \Celery\Celery(
			$host, $username, $password, $vhost, $exchange, $routingKey);
    }

    public function sendTask($asin, $country = 'us', $async = TRUE) {
		$marketplace = strtoupper($country);
		$routingKey = sprintf($this->queuePattern, $marketplace);
		$args = array($marketplace, $asin);

		return $this->celery->PostTask($this->task, $args, $async, $routingKey);
	}
}
