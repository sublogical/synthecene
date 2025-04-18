use itertools::Itertools;
use scylla::{Session };

use vera::vera_api::{
    DocumentUpdate,
    CellValue,
    Dataspace,
    cell_value::Data,
};

pub fn derive_keyspace(dataspace: &Dataspace) -> String {
    format!("{}_universe", escape_uri(&dataspace.universe))
}

pub fn derive_table_name(dataspace: &Dataspace) -> String {
    format!("{}_version_{}", escape_uri(&dataspace.table), escape_uri(&dataspace.version))
}

pub fn derive_update_schema(column_uris: &[String]) -> String {
    let mut schema = String::new();

    // Create a comma-separated list of escaped column URIs
    let columns: Vec<String> = column_uris
        .iter()
        .map(|uri| escape_uri(uri))
        .collect();
    
    // Join them with commas and wrap in a single set of parentheses
    schema.push('(');
    schema.push_str("doc_id");
    if !columns.is_empty() {
        schema.push_str(", ");
        schema.push_str(&columns.join(", "));
    }
    schema.push(')');

    schema
}

/**
 * Generate a schema for a duplicate update in a 
 * "INSERT ... ON DUPLICATE KEY UPDATE" statement
 *
 * E.g. id=VALUES(id), a=VALUES(a), b=VALUES(b), c=VALUES(c)
 */
pub fn derive_duplicate_update_schema(column_uris: &[String]) -> String {
    let mut schema = String::new();

    // Create a comma-separated list of escaped column URIs
    let columns: Vec<String> = column_uris
        .iter()
        .map(|uri| format!("{c} = VALUES({c})", c = escape_uri(uri)))
        .collect();

    schema.push_str("doc_id = VALUES(doc_id), ");
    schema.push_str(&columns.join(", "));

    schema
}

fn to_cql_value(value: &CellValue) -> String {
    // todo: add column_spec and validate type vs value
    // todo: add support for set, list and map types
    // todo: add support for append, prepend, add and remove operations on complex types
    match &value.data {
        Some(Data::StringValue(s)) => format!("'{}'", s),
        Some(Data::Int64Value(i)) => i.to_string(),
        Some(Data::DoubleValue(f)) => f.to_string(),
        _ => todo!("handle other data types"),
    }
}

/**
 * Generate a comma-separated list of values for a "INSERT ... ON DUPLICATE KEY UPDATE" statement
 *
 * E.g. (1, 'a1', 'b1', 'c1'), (2, 'a2', 'b2', 'c2'), (3, 'a3', 'b3', 'c3')
 */
pub fn derive_values(document_updates: &[DocumentUpdate]) -> String {

    let values = document_updates
        .iter()
        .map(|update| {
            let mut values = String::new();
            let cell_values: Vec<String> = update.cell_values
                .iter()
                .map(|value| to_cql_value(value))
                .collect();

            values.push_str(&format!("({}, ", update.document_id));
            values.push_str(&cell_values.join(", "));
            values.push_str(")");
            values
        })
        .join(", ");

    values
}

fn escape_uri(input: &str) -> String {
    let escape_count = input.chars().filter(|c| !c.is_alphanumeric() && *c != '_').count();

    let mut result = String::with_capacity(input.len()+escape_count);
    for c in input.chars() {
        if c.is_alphanumeric() || c == '_' {
            result.push(c);
        } else {
            result.push_str("__");
        }
    }
    result
}



#[cfg(test) ]
mod tests { 
    use super::*;
    fn generate_dataspace(universe: &str, table: &str, version: &str) -> Dataspace {
        Dataspace {
            universe: universe.to_string(),
            table: table.to_string(),
            version: version.to_string(),
        }
    }

    #[test]
    fn test_escape_uri() {
        assert_eq!(escape_uri("hello-world"), "hello__world");
        assert_eq!(escape_uri("hello_world"), "hello_world");
        assert_eq!(escape_uri("/hello/world#foo"), "__hello__world__foo");
    }

    #[test]
    fn test_derive_keyspace() {
        assert_eq!(derive_keyspace(&generate_dataspace("test", "t1", "1")), "test_universe");
        assert_eq!(derive_keyspace(&generate_dataspace("foo/bar#howdy", "t2", "1")), "foo__bar__howdy_universe");
    }

    #[test]
    fn test_derive_table_name() {
        assert_eq!(derive_table_name(&generate_dataspace("test", "test", "1.0.1")), "test_version_1__0__1");
        assert_eq!(derive_table_name(&generate_dataspace("test", "foo/bar#howdy", "v2")), "foo__bar__howdy_version_v2");
    }

    #[test]
    fn test_derive_update_schema() {
        let column_uris = vec![
            "__std__text_prompt".to_string(),
            "__std__my_foo".to_string(),
            "__std__my_bar".to_string(),
        ];

        assert_eq!(
            derive_update_schema(&column_uris),
            "(doc_id, __std__text_prompt, __std__my_foo, __std__my_bar)"
        );
    }

    fn make_document_update(document_id: &str, cell_values: Vec<CellValue>) -> DocumentUpdate {
        DocumentUpdate {
            document_id: document_id.to_string(),
            cell_values: cell_values,
        }
    }
    fn make_string_cell_value(value: &str) -> CellValue {
        CellValue {
            data: Some(Data::StringValue(value.to_string())),
        }
    }

    fn make_int_cell_value(value: i64) -> CellValue {
        CellValue {
            data: Some(Data::Int64Value(value)),
        }
    }

    fn make_double_cell_value(value: f64) -> CellValue {
        CellValue {
            data: Some(Data::DoubleValue(value)),
        }
    }

    #[test]
    fn test_derive_values() {
        let document_updates = vec![
            make_document_update("1", vec![
                make_string_cell_value("alpha"),
                make_string_cell_value("beta"),
                make_int_cell_value(1),
                make_double_cell_value(2.0),
            ]),
            make_document_update("2", vec![
                make_string_cell_value("delta"),
                make_string_cell_value("epsilon"),
                make_int_cell_value(3),
                make_double_cell_value(4.15),
            ]),
        ];

        assert_eq!(derive_values(&document_updates), "(1, 'alpha', 'beta', 1, 2), (2, 'delta', 'epsilon', 3, 4.15)");
    }
}     

