
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
  * add type coercion
  * add escaping / CQL injection protection
  * ~~add support for JSON parameters in client~~
  * ~~implement CQL~~
  * ~~implement client CLI~~
  * ~~add proto~~
  * ~~basic type support~~
* get
  * ~~add proto~~
  * implement service
  * implement client CLI
  * implement CQL

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

# How it works

* Universe is a collection of tables identified with URI
* Table is 
* COLUMN
  * belongs to a TABLE
  * has a single TYPE
* TYPE - defines format of 
  * defines a CQL type mapping



## PUT Commit Algorithm

1. Lookup column for all columns in PUT request
2. If any columns are missing, infer schema from values and create the column
  a - compute namespace with default inferred namespace property on PUT request, then TABLE, then UNIVERSE
  b - select a std: type based on the data-type used 
2. Lookup type_uri for all columns