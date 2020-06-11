<?php

/**
 * @Copyright: Copyright :copyright: 2019 by IBPort. All rights reserved.
 * @Author: Neal Wong
 * @Email: ibprnd@gmail.com
 */

namespace AmazonProductLoader;

use Elasticsearch\ClientBuilder;

class ProductLoader {
	private $es;
	private $productDetailConverter;
    private $indiceNamePattern;

    public function __construct($host, $port, $user, $password, $maxRetries = 12) {
    	$builder = ClientBuilder::create();
    	$hostStr = $host . ':' . $port;
    	if ($user && $password) {
    		$hostStr = $user . ':' . $password . '@' . $hostStr;
    		// $builder->setBasicAuthentication($user, $password);
    	}
    	$builder->setHosts([$hostStr]);
    	$builder->setRetries($maxRetries);
    	$this->es = $builder->build();

    	$this->productDetailConverter = new ProductDetailConverter();
        $this->productCategoriesConverter = new ProductCategoriesConverter();
    	$this->indiceNamePattern = 'matching_products_%s';
        $this->categoryIndiceNamePattern = 'products_categories_%s';
    }

    public function getProducts($asins, $country = 'us') {
    	$products = array();

    	$indiceName = sprintf($this->indiceNamePattern, strtolower($country));
    	$params = array(
    		'index' => $indiceName,
    		'from' => 0,
    		'size' => count($asins),
    		'body' => array(
    			'query' => array('terms' => ['_id' => $asins])
    		)
    	);
    	$resp = $this->es->search($params);
    	$result = $this->productDetailConverter->convert($resp);
    	foreach ($asins as $asin) {
    		$products[$asin] = $result[$asin] ?? NULL;
    	}

    	return $products;
    }

    public function getProductCategories($asins, $country = 'us') {
        $productCategories = array();

        $indiceName = sprintf($this->categoryIndiceNamePattern, strtolower($country));
        $params = array(
            'index' => $indiceName,
            'from' => 0,
            'size' => count($asins),
            'body' => array(
                'query' => array('terms' => ['_id' => $asins])
            )
        );
        $resp = $this->es->search($params);
        $result = $this->productCategoriesConverter->convert($resp);
        foreach ($asins as $asin) {
            $productCategories[$asin] = $result[$asin] ?? NULL;
        }

        return $productCategories;
    }

    public function scan(
    	string $index,
    	array $query = [],
    	string $scroll="5m",
    	bool $raiseOnError = true,
    	bool $preserveOrder = false,
    	int $size = 1000,
    	bool $clearScroll = true,
    	array $scrollParams = []) {
    	if ($preserveOrder) {
    		$query['sort'] = '_doc';
    	}

    	$params = array(
    		'index' => $index,
    		'body' => $query,
    		'scroll' => $scroll,
    		'size' => $size
    	);
    	$resp = $this->es->search($params);

    	$scrollId = $resp['_scroll_id'];
    	try {
    		while ($scrollId && $resp['hits']['hits']) {
    			foreach ($resp['hits']['hits'] as $hit) {
    				yield $hit;
    			}

    			if ($resp['_shards']['successful'] + $resp['_shards']['skipped'] < $resp['_shards']['total']) {
    				if ($raiseOnError) {
    					throw new Exception('Scroll request has only succeeded on ' . $resp['_shards']['successful'] . '(+' . $resp['_shards']['skipped'] . ' skiped) shards out of ' . $resp['_shards']['total'] . '.');
    				}
    			}

    			$resp = $this->es->scroll(array(
    				'scroll_id' => $scrollId,
    				'scroll' => $scroll
    			));
            	$scrollId = $resp["_scroll_id"];
    		}
    	} finally {
    		if ($scrollId && $clearScroll) {
    			$this->es->clearScroll(array(
    				'body' => array(
    					'scroll_id' => [$scrollId]
    				)
    			));
    		}
    	}
    }
}