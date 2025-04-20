
# Vera - source of truth service

# TODO

* vera-config
  * features
    * textproto support for prost OR ??
    * namespace support
      * in config
      * in parameters with merge
    * template support
  * configs
    * universe config
    * type config
    * table config
    * column config
  * manifest.yml
    * list all universes / tables / columns
    * templated configs
* vera-batch
  * launch spark job
  * input types
    * CSV
    * parquet
    * tfrecord
    * WARC / common crawl


* service
  * add schema check / version compat to deep healthcheck
  * add gRPC authentication
  * ~~add middleware to pool scylladb connections~~

* features 
  * computed attributes
    * add support for uploading/storing WASM modules in Vera
    * add support for registering WASM modules for attributes readers
      * set dependent attributes
      * whitelist external services that can be used
      * route get requests to WASM module
      * provide API for WASM module to call back for dynamic attribute requests
  * properties
    * put standard properties into namespaces
    * apply namespaces in client / config jobs
    * table
      * declared_license
      * authors
      * citations
* backends
  * define backend trait
  * migrate existing backend code into cql backend
  * create mongodb backend
* operations  
  * TABLE:
    * Create
      * mark table as created in global registry
      * move both CQL queries into a single batch
      * ~~add proto~~
      * ~~implement service~~
      * ~~implement client CLI~~
      * ~~implement CQL~~
    * Retrieve
      * add proto
      * implement service
      * implement client CLI
      * implement CQL
      * list of columns
    * Update
    * Delete
      * mark table as deleted in global registry
      * move both CQL queries into a single batch
      * ~~add proto~~
      * ~~implement service~~
      * ~~implement client CLI~~
      * ~~implement CQL~~
      * ~~add not-empty check~~
  
  * COLUMN
    * Create
      * implement service
      * implement client CLI
      * implement CQL
      * ~~add proto~~
    * Retrieve
    * Update
    * Delete
      * implement service
      * implement client CLI
      * implement CQL
      * ~~add proto~~
  * TYPE
    * Create
    * Retrieve
    * Update
    * Delete
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

# HOWTO

### Dependencies

* datastax-cpp - https://docs.datastax.com/en/developer/cpp-driver/2.16/topics/installation/index.html

# How it works

* UNIVERSE is a collection of tables identified with URI
  * has many tables
  * properties
    * default_inferred_namespace - to use for creating adhoc columns
* TABLE
  * belongs to a UNIVERSE
  * has many columns
  * properties
    * default_inferred_namespace - to use for creating adhoc columns
* COLUMN
  * belongs to a TABLE
  * has a single TYPE
* TYPE - defines format of 
  * defines a CQL type mapping
  * properties
    * cql_data_type - expected type to use for CQL columns
    * json_schema_constraint - schema for validating JSON data
    * protobuf_message
    * string_regex_constraint





## PUT Commit Algorithm

1. Lookup column for all columns in PUT request
2. If any columns are missing, infer schema from values and create the column
  a - compute namespace with default inferred namespace property on PUT request, then TABLE, then UNIVERSE
  b - select a std: type based on the data-type used 
2. Lookup type_uri for all columns