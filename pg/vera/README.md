
# Vera - source of truth service

# TODO

* create table
  * ~~add proto~~
  * ~~implement service~~
  * ~~implement client CLI~~
  * ~~implement CQL~~
* delete table
  * add proto
  * implement service
  * implement client CLI
  * implement CQL
* add column
  * ~~add proto~~
  * implement service
  * implement client CLI
  * implement CQL
* delete column
  * ~~add proto~~
  * implement service
  * implement client CLI
  * implement CQL
* put
  * ~~add proto~~
  * add column if it doesn't exist
  * implement CQL
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
