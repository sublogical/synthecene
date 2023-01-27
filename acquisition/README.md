```
sudo apt-install libssl-dev libclang-dev
```

### TODO: Crawler
* capture
  * accumulate outlinks for domain
  * send page batch notification to kafka
  * send page to kafka
  * send outlinks batch to object store
  * send outlinks notification to kafka
  * send outlinks to kafka
  * ~~accumulate pages for domain~~
  * ~~send page batch to object store~~
* fetch object
  * sitemaps support
  * ~~normalize URLs properly~~
  * ~~partition inlinks and outlinks~~
  * ~~enforce fetch rate~~
  * ~~support basic fetch~~
  * ~~extract urls~~
  * ~~support robots.txt~~
* schema
  * ~~support proto definitions~~
  * link
  * capture to proto
  * host to proto?
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
* refresh crawl task
  * time sensitive sitemap crawl
  * if-modified-since in fetch
* wide crawl task
  * download domain seeds
  * start sub-tasks for each domain    
* controller
  * ~~single-task~~
  * ~~local controller~~
  * controller REST API
  * retrieve task
  * keep-alive task
  * reap zombie task assignments
  * complete task assignment
  * support sub-tasks O(1M)
* planner
  * scheduled tasks
  * task SLO
* smarts
  * predict most useful seed pages per domain
  * predict utility of url
  * predict utility of page
  * detect spider trap
  * ~~implement normalization of URLs~~
* telemetry
  * distribution
  * move telemetry to shared
  * ~~exponential decay~~
  * ~~summation~~
  * ~~max~~
  * ~~min~~
* state
  * ~~only store checkpoints when they are big enough (maybe_checkpoint)~~
  * ~~downloading existing state at start of task~~
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

