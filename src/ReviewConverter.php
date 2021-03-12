<?php

/**
 * @Copyright: Copyright :copyright: 2019 by IBPort. All rights reserved.
 * @Author: Neal Wong
 * @Email: ibprnd@gmail.com
 */

namespace AmazonProductLoader;

class ReviewConverter extends EsConverter {
	public function convert($data) {
		if (empty($data)) {
			return $data;
		}

		if (array_key_exists('hits', $data)) {
			return parent::convert($data);
		}

		if (array_key_exists('_source', $data)) {
			return $data['_source'];
		}

		return $data;
	}
}
