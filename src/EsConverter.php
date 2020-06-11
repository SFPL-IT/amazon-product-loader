<?php

/**
 * @Copyright: Copyright :copyright: 2019 by IBPort. All rights reserved.
 * @Author: Neal Wong
 * @Email: ibprnd@gmail.com
 */

namespace AmazonProductLoader;

class EsConverter {
	public function convert($data) {
		$result = [];
		if (!$data) {
			return $result;
		}
	
		foreach ($data['hits']['hits'] as $item) {
			$result[$item['_id']] = $item['_source'];
		}

		return $result;
	}
}
