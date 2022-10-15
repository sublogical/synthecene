use arrow::record_batch::RecordBatchReader;

fn get_tile_view(tile: Tile, timestamp: Option<u64>) -> RecordBatchReader {
}

fn get_record_reader_for_transaction(update_transaction: T) -> RecordBatchReader {
    // todo: support pushdown predicates for filtering objects
}

// Materializes an in-memory view of the current state of a tile, given a 
// set of recordreaders representing transactions on that file. Typically, 
// this will include the latest checkpoint, plus all updates since that point
fn materialize_view(readers:Vec<RecordBatchReader>) -> RecordBatchReader {
    // todo: open iterators for each of the recordbatchreaders
    // todo: open iterators on each of the records coming from the recordbatchreaders
    // todo: implement a record batch reader which returns appropriately sized record batches by merging on demand from the transaction batches


}