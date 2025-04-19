
/**
 * Global Column Registry
 * 
 * This table is used to store the metadata for all columns in the system.
 * 
 * The dataset_uri is the unique identifier for the dataset that the column 
   belongs to, and is a foreign key into the global_dataset_registry table.
 * The column_uri is the unique identifier for the column
 * The type_uri is the unique identifier for the type of the column, and 
   is a foreign key into the global_type_registry table.
 * The column_alias is the short name for the column, and is used to identify
   the column in the dataset.
 * The description is a description of the column, and is used to identify the
   column in the dataset.
 * The created_at is the timestamp of when the column was created, and is
   used to identify the column in the dataset.
 */
var migration1744827744 = {
  up : function (db, handler) {
    var query = `
    CREATE TABLE IF NOT EXISTS global_column_registry (
      dataset_uri TEXT,          # unique identifier for the dataset
      column_uri TEXT,           # unique identifier for the column
      type_uri TEXT,             # unique identifier for the type
      column_alias TEXT,         # short name for the column
      description TEXT,          # description of the column
      created_at TIMESTAMP       # timestamp of when the column was created
                                 # use toTimestamp(now()) for writes
    )
    PRIMARY KEY (dataset_uri, column_uri)
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
    var query = `
    DROP TABLE IF EXISTS global_column_registry;
    `;
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
module.exports = migration1744827744;