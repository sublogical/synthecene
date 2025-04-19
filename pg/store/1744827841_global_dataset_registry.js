/**
 * Global Dataset Registry
 * 
 * This table is used to store the metadata for all datasets in the system.
 * 
 * The dataset_uri is the unique identifier for the dataset.  
 * The keyspace_name is the name of the keyspace that the dataset belongs to.
 * The table_alias is the short name for the table, and is used to identify the table in the dataset.
 * The name is the name of the dataset.
 * The description is a description of the dataset.
 * The created_at is the timestamp of when the dataset was created, and is used to identify the dataset in the system.
 */
var migration1744827841 = {
  up : function (db, handler) {
    var query = `
    CREATE TABLE IF NOT EXISTS global_dataset_registry (
      dataset_uri TEXT,
      keyspace_name TEXT,
      table_alias TEXT,
      name TEXT,
      description TEXT,
      created_at TIMESTAMP
    )
    PRIMARY KEY (dataset_uri)
    WITH caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'};
    `;
    var params = [];
    db.execute(query, params, { prepare: true }, function (err) {
      if (err) {
        handler(err, false);
      } else {
        handler(false, true);
      }
    });
  },
  down : function (db, handler) {
    var query = '';
    var params = [];
    db.execute(query, params, { prepare: true }, function (err) {
      if (err) {
        handler(err, false);
      } else {
        handler(false, true);
      }
    });
  }
};
module.exports = migration1744827841;