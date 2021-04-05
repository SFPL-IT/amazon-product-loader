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
        $this->productDetailWebConverter = new ProductDetailWebConverter();
        $this->reviewConverter = new ReviewConverter();
    	$this->indiceNamePattern = 'matching_products_%s';
        $this->categoryIndiceNamePattern = 'products_categories_%s';
        $this->productDetailsWebIndice = 'product_details_web';
        $this->reviewIndice = 'reviews';
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

    public function getProductOverview($asins, $country = 'us') {
        $productOverviews = array();

        $productIds = [];
        $countryUpper = strtoupper($country);
        foreach ($asins as $asin) {
            $productIds[] = $countryUpper . ':' . $asin;
        }

        $params = array(
            'index' => $this->productDetailsWebIndice,
            'from' => 0,
            'size' => count($productIds),
            'body' => array(
                'query' => array('terms' => ['_id' => $productIds])
            )
        );

        $resp = $this->es->search($params);
        $result = $this->productDetailWebConverter->convert($resp);
        foreach ($asins as $asin) {
            $productId = $countryUpper . ':' . $asin;
            $productDetailWeb = $result[$productId] ?? NULL;
            if ($productDetailWeb == NULL) {
                $productOverviews[$asin] = NULL;
            } else {
                if (isset($productDetailWeb['available'])) {
                    $available = $productDetailWeb['available'];
                } else {
                    $available = FALSE;
                }
                if (isset($productDetailWeb['not_found'])) {
                    $notFound = $productDetailWeb['not_found'];
                } else {
                    $notFound = TRUE;
                }

                $details = $productDetailWeb['details'] ?? [];
                $title = $productDetailWeb['title'] ?? '';
                $featureBullets = $productDetailWeb['feature_bullets'] ?? [];
                $description = $productDetailWeb['product_description'] ?? '';

                $reviewStar = isset($productDetailWeb['star']) ? $productDetailWeb['star'] : NULL;
                $reviewCnt = isset($productDetailWeb['reviews']) ? $productDetailWeb['reviews'] : NULL;
                if (isset($productDetailWeb['review_overview'])) {
                    $reviewOverview = $productDetailWeb['review_overview'];
                } else {
                    $reviewOverview = [];
                }

                $productOverview = [
                    'not_found' => $notFound,
                    'available' => $available,
                    'marketplace' => strtoupper($productDetailWeb['marketplace']),
                    'asin' => $productDetailWeb['asin'],
                    'title' => $title,
                    'feature_bullets' => $featureBullets,
                    'description' => $description,
                    'stars' => $reviewStar,
                    'reviews' => $reviewCnt,
                    'review_overview' => $reviewOverview,
                    'details' => $details,
                    'time' => $productDetailWeb['time'],
                ];

                $productOverview['review'] =  array_merge(
                    ['review_count' => $reviewCnt, 'review_star' => $reviewStar],
                    $reviewOverview
                );

                $productOverviews[$asin] = $productOverview;
            }
        }

        return $productOverviews;
    }

    public function getProductReviews($asins) {
        $result = [];
        foreach ($asins as $asin) {
            $result[$asin] = $this->getSingleProductReviews($asin);
        }

        return $result;
    }

    public function getSingleProductReviews($asin) {
        $result = [];
        $query = ['query' => ['match' => ['asin' => $asin]]];
        foreach ($this->scan($this->reviewIndice, $query) as $hit) {
            $review = $this->reviewConverter->convert($hit);
            $result[$review['review_id']] = $review;
        }

        return $result;
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