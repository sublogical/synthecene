# CalicoLobster


## Mission

Build an AI-first
* SaaS targetting sticky MRR
* Data arbitrage

### Consumer

* Paid Search / General information assistant (ChatGPT + Google)
* Conversational shopping assistant
* Highlight stuff on the web, search over it, ask questions over it

### Content Creator

* Add generative imagery to blog posts, presentations, websites
* Improve quality / change tone of text
* Find reference material for text
* Fill in narrative based on outline


### SMB - Shopify

* Add conversational shopping to shopify stores
* Add support chatbot for shopify stores
* 



## ML Infra Wins

* Labeling
  * synthetic data generation
  * 
* Training
  * fine-tune LLM on a dataset
  * PPO optimize a LLM on a reward model
* Analytics
  * continuous monitoring & alerting
  * observability
  * automated anomoly detection
  * production performance monitoring
* Serving
  * context aggregation (Cortex)
  * knowledge retrieval (search, kg)
  * multi-pass inference





# Tech Notes

## Decision: What infra to use to train/fine-tune?

## Decision: What infra to use to annotate?


## Decision: Build out Delta-Lake Alternative?
***ANSWER:*** NOT NOW. Just use vanilla delta lake, create as many tables as we need and deal with the join overhead.

#### FOR:
* it's fun
* git-like branching structure applied to big data is interesting
* independent column mutability (with efficient join)
* add sub-partition bucketing
* efficient pushdown predicates
* tile-level stats rollup

#### AGAINST:
* complex
* don't need for much of what we're doing

## Install

```
cargo install cargo-generate
```

## PoCs
* Chrome Plugin
  * collects page views
  * collects search terms
  * collects highlights
  * adds menu items - https://developer.chrome.com/docs/extensions/mv3/user_interface/
  * adds popup UI - 
  * uses vue UI - https://medium.com/@johannes.lauter/building-a-vue-browser-extension-including-tailwind-848e0e451f50
  * loads WASM
* Google Docs
  * read docs via API
  * plug-in https://developers.google.com/apps-script/guides/dialogs#custom_sidebars


## High-Level Priorities

* Be able to show things working (get input, perform inference, show results)
* Be able to collect data (crawling, direct input)
* Be able to collect feedback (usage, HITL, curation)
* Be able to orchestrate complex work tasks 
* Be able to orchestrate complex serving



## High-Level TODOs

* Statefulness
  * Store stuff in mongodb, be able to retrieve it
  * Retrieve stuff, pass as context for inference
  * async

* Initial Deep Crawl Support
  * Get crawler to support single domain deep crawl via CLI
  * Get crawler to upload corpus to data lake
* Implement base task manager service
  * Create crawl tasks via CLI (including bullk)
  * Implement base graphql rpc service (or decide to use grpc to vue)
  * Pilot crawl CX (list, manage, create, delete, search)
* Implement dexter - runtime serving orchestrator ala 'unruly goats'
  * async dependency graph using actors
  * stateful
  * storage (mongo, ddb)
  * streaming
  * supports cyclic dependencies (e.g. memories)
  * supports external sources (e.g. SCANN, FAISS, ElasticSearch, KG, etc)
  * supports inference
  * built-in caching
  * addressable (multiple clients can connect to same serving graph)
* Implement sinister - offline task orchestrator (maybe airflow?)
  * serving using dexter
  * annotation
  * streaming
  * spark/flink for processing with SQL
  * serverless processors in python, rust, whatever
  * storage in deltalake, mongo, ddb
* Implement base signal service
  * Get data lake working with Spark
  * Create basic crawler signals (spam detection, abuse detection, clean text) with heuristics
* Implement base annotator & model builder
  * Create clean text annotator CX
  * Create basic annotation task
  * Create basic training task
  * Pilot annotation CX (list, manage, create, delete, search)
  * Pilot training CX (list, manage, create, delete, search)
  * Model repository support
  * Model serving support
  * Update clean text signal to use model inference
  * Abuse detection
  * Spam detection
* Data Sources
  * CommonCrawl
  * Wikipedia
  * Enterprise
    * Google Drive
    * Microsoft SharePoint
    * Microsoft Dynamics
    * Microsoft OneDrive
    * Confluence
  * Social
    * Twitter
  * Chat
    * Slack
    * Google 
    * Discord

* Get AWS environment working



* ~~Get lab conda environment working correctly~~
* ~~Get Docker environment working~~
