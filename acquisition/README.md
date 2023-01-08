```
sudo apt-install libssl-dev
```

### TODO: Crawler
* frontier
  * ~~implement disk back frontier~~
  * ~~benchmark frontier~~
  * support tiered frontiers
  * support downloading existing frontier at start of task
  * support periodic frontier checkpoints in object store
* last visit map
  * ~~implement disk backed last visit kv store~~
  * ~~benchmark last visit map~~
  * support downloading existing last visit map at start of task
  * support periodic last visit map checkpoints in object store
* fetch object
  * ~~support basic fetch~~
  * ~~extract urls~~
  * ~~support robots.txt~~
  * sitemaps support
  * enforce fetch rate
  * support parallel single host fetch
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
  * track fetch telemetry - status distribution
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

