
# Vera - source of truth service

# TODO

* create table
  * mark table as created in global registry
  * move both CQL queries into a single batch
  * ~~add proto~~
  * ~~implement service~~
  * ~~implement client CLI~~
  * ~~implement CQL~~
* delete table
  * mark table as deleted in global registry
  * move both CQL queries into a single batch
  * ~~add proto~~
  * ~~implement service~~
  * ~~implement client CLI~~
  * ~~implement CQL~~
  * ~~add not-empty check~~
* get table
  * add proto
  * implement service
  * implement client CLI
  * implement CQL
  * list of columns
* update table

* add column
  * implement service
  * implement client CLI
  * implement CQL
  * ~~add proto~~
* delete column
  * implement service
  * implement client CLI
  * implement CQL
  * ~~add proto~~
* put
  * add column if it doesn't exist
  * add support for JSON parameters in client
  * add escaping / CQL injection protection
  * ~~implement CQL~~
  * ~~implement client CLI~~
  * ~~add proto~~
  * ~~basic type support~~
* get
  * ~~add proto~~
  

* service
  * add schema check / version compat to deep healthcheck
  * add middleware to pool scylladb connections

* computed attributes
  * add support for uploading/storing WASM modules in Vera
  * add support for registering WASM modules for attributes readers
    * set dependent attributes
    * whitelist external services that can be used
    * route get requests to WASM module
    * provide API for WASM module to call back for dynamic attribute requests
  * 

# HOWTO

### Dependencies

* datastax-cpp - https://docs.datastax.com/en/developer/cpp-driver/2.16/topics/installation/index.html
