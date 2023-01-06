```
sudo apt-install libssl-dev
```

### TODO: Crawler
* task state
  * track frontier
  * track last-visit map
  * download task state from object store
  * send task state update to object store
* fetch object
  * ~~support basic fetch~~
  * ~~extract urls~~
  * ~~support robots.txt~~
  * sitemaps support
  * enforce fetch rate
  * support parallel single host fetch
* schema
  * proto definitions
  * link
  * capture
  * host
  * frontier
  * last-visit
* domain state
  * track fetch telemetry
  * track fetch rate
  * send domain state to object store
* capture state
  * send page batch to object store
  * send page batch notification to object store
  * send page to kafka
* task
  * get task from planner
  * support sub-tasks O(1M)
  * implement recipe components
    * crawl sitemap
    * crawl from seed list
  * implement deep crawl task
  * implement refresh crawl task
  * implement wide-crawl task
    * download domain seeds
    * start sub-tasks for each domain    
* planner
  * scheduled tasks
  * task SLO
* smarts
  * predict most useful seed pages per domain
  * predict utility of url
  * predict utility of page
  * detect spider trap

### TODO: Importer
* Datasets
  * Common Crawl
  * Wikipedia
  * Wikidata
  * Worldbank Open Data

