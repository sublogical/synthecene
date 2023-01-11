```
sudo apt-install libssl-dev libclang-dev
```

### TODO: Crawler
* frontier
  * ~~test using rocksdb vs yaque~~
  * priority queue for rocksdb
  * proto serialization for priority queue
  * priority queue in column family
  * port frontier to rocksdb queue
  * store periodic frontier checkpoints in object store
  * downloading existing frontier at start of task
  * tiered frontiers
  * ~~implement disk back frontier~~
  * ~~benchmark frontier~~
* last visit map
  * port last visit to rocksdb
  * last visit in column family
  * download existing last visit map at start of task
  * store periodic last visit map checkpoints in object store
  * ~~implement disk backed last visit kv store~~
  * ~~benchmark last visit map~~
* fetch object
  * sitemaps support
  * enforce fetch rate
  * support parallel single host fetch
  * ~~support basic fetch~~
  * ~~extract urls~~
  * ~~support robots.txt~~
* schema
  * ~~support proto definitions~~
  * link
  * capture to proto
  * host to proto?
* domain state
  * track fetch rate
  * send domain state to object store
* capture state
  * send page batch to object store
  * send page batch notification to object store
  * send page to kafka
* deep crawl task
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

