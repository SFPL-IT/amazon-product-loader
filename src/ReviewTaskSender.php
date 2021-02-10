<?php

/**
 * @Copyright: Copyright :copyright: 2019 by IBPort. All rights reserved.
 * @Author: Neal Wong
 * @Email: ibprnd@gmail.com
 */

namespace AmazonProductLoader;

use AmazonProductLoader\RedisTaskSender;


class ReviewTaskSender extends RedisTaskSender {
	private $redisConfig;
	private $redisClient;

    public function sendTask($asins, $country = 'us', $taskKey = 'review:start_urls') {
        parent::sendTask($asins, $country, $taskKey);
	}
}
