```
sudo apt-install libssl-dev libclang-dev
```

### TODO: Crawler
* state
  * downloading existing state at start of task
  * only store checkpoints when they are big enough
  * ~~store periodic checkpoints in object store~~
  * ~~test using rocksdb vs yaque~~
  * ~~priority queue for rocksdb~~
  * ~~proto serialization for priority queue~~
  * ~~priority queue in column family~~
  * ~~port frontier to rocksdb queue~~
  * ~~tiered frontiers~~
  * ~~implement disk back frontier~~
  * ~~benchmark frontier~~
  * ~~port last visit to rocksdb~~
  * ~~last visit in column family~~
  * ~~implement disk backed last visit kv store~~
  * ~~benchmark last visit map~~
* fetch object
  * sitemaps support
  * enforce fetch rate
  * normalize URLs properly
  * partition inlinks and outlinks
  * ~~support basic fetch~~
  * ~~extract urls~~
  * ~~support robots.txt~~
* schema
  * ~~support proto definitions~~
  * link
  * capture to proto
  * host to proto?
* capture
  * accumulate pages for domain
  * accumulate outlinks for domain
  * send page batch to object store
  * send page batch notification to kafka
  * send page to kafka
  * send outlinks batch to object store
  * send outlinks notification to kafka
  * send outlinks to kafka
* deep crawl task
  * support parallel single host fetch
  * track fetch rate
  * track fetch telemetry - status distribution
  * ~~build domain frontier~~
  * ~~build domain last visit map~~
  * ~~track frontier in crawl task~~
  * ~~iterative crawl URLs from frontier~~
  * ~~add outlinks to frontier~~
  * ~~dedupe outlinks on page~~
  * ~~track last-visit map in crawl task~~
  * ~~dedupe outlinks against last-visit map~~
  * ~~track fetch telemetry - latency~~
  * ~~track fetch telemetry - size~~
  * ~~track fetch telemetry - outlinks~~
* telemetry
  * ~~exponential decay~~
  * ~~summation~~
  * ~~max~~
  * ~~min~~
  * distribution
* refresh crawl task
  * time sensitive sitemap crawl
* wide crawl task
  * download domain seeds
  * start sub-tasks for each domain    
* controller
  * single-task
  * local controller
  * support sub-tasks O(1M)
* planner
  * scheduled tasks
  * task SLO
* smarts
  * implement normalization of URLs
  * predict most useful seed pages per domain
  * predict utility of url
  * predict utility of page
  * detect spider trap
### TODO: Task Server
* Active Task
  * Get task API
  * List task API
  

### TODO: Importer
* Datasets
  * Common Crawl
  * Wikipedia
  * Wikidata
  * Worldbank Open Data

