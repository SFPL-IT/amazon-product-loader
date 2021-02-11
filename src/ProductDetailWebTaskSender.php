<?php

/**
 * @Copyright: Copyright :copyright: 2019 by IBPort. All rights reserved.
 * @Author: Neal Wong
 * @Email: ibprnd@gmail.com
 */

namespace AmazonProductLoader;

use AmazonProductLoader\RedisTaskSender;


class ProductDetailWebTaskSender extends RedisTaskSender {
    public function sendTask($asins, $country = 'us', $taskKey = 'detail_page:start_urls') {
        parent::sendTask($asins, $country, $taskKey);
	}
}
