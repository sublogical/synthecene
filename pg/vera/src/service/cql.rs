use itertools::Itertools;
use scylla::{Session };

use vera::vera_api::{
    CellValue,
    ColumnSpec,
    DocumentUpdate,
    cell_value::Data,
};

#[derive(Debug, thiserror::Error)]
pub enum CqlError {
    #[error("unsupported type: {0}")]
    UnsupportedType(String),
}

pub fn derive_keyspace(universe_uri: &str) -> String {
    format!("universe_{}", escape_uri(universe_uri))
}

pub fn derive_table_name(table_uri: &str) -> String {
    escape_uri(table_uri)
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
    schema.push_str("document_uri");
    if !columns.is_empty() {
        schema.push_str(", ");
        schema.push_str(&columns.join(", "));
    }
    schema.push(')');

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

            values.push_str(&format!("('{}', ", update.document_id));
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

fn derive_column_type(type_uri: &str) -> Result<String, CqlError> {
    // todo: add support for set, list and map types
    // todo: add support for user defined types
    match type_uri {
        "/std/text" => Ok("TEXT".to_string()),
        "/std/int64" => Ok("BIGINT".to_string()),
        "/std/int32" => Ok("INT".to_string()),
        "/std/int" => Ok("INT".to_string()),
        "/std/double" => Ok("DOUBLE".to_string()),
        _ => Err(CqlError::UnsupportedType(type_uri.to_string())),
    }
}

pub fn derive_table_create_schema(column_specs: &[ColumnSpec]) -> Result<String, CqlError> {
    let mut schema = String::new();

    schema.push_str("document_uri TEXT PRIMARY KEY, ");
    let column_types = column_specs
        .iter()
        .map(|spec| 
            derive_column_type(&spec.type_uri)
                .map(|column_type| format!("{} {}", escape_uri(&spec.column_uri), column_type))
        )
        .collect::<Result<Vec<String>, CqlError>>()?;

    schema.push_str(&column_types.join(", "));

    Ok(schema)
}



/**
CREATE TABLE IF NOT EXISTS mykeyspace.mytable (
    document_id string,
    col1 text,
    col2 text,
    PRIMARY KEY (document_id)
);
*/


#[cfg(test) ]
mod tests { 
    use super::*;


    #[test]
    fn test_escape_uri() {
        assert_eq!(escape_uri("hello-world"), "hello__world");
        assert_eq!(escape_uri("hello_world"), "hello_world");
        assert_eq!(escape_uri("/hello/world#foo"), "__hello__world__foo");
    }

    #[test]
    fn test_derive_keyspace() {
        assert_eq!(derive_keyspace("test"), "universe_test");
        assert_eq!(derive_keyspace("foo/bar#howdy"), "universe_foo__bar__howdy");
    }

    #[test]
    fn test_derive_table_name() {
        assert_eq!(derive_table_name("test"), "test");
        assert_eq!(derive_table_name("foo/bar#howdy"), "foo__bar__howdy");
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
            "(document_uri, __std__text_prompt, __std__my_foo, __std__my_bar)"
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

    #[test]
    fn test_derive_table_create_schema() {
        let column_specs = vec![
            ColumnSpec {
                column_uri: "/my/foo".to_string(),
                type_uri: "/std/text".to_string(),   
            },
            ColumnSpec {
                column_uri: "/my/bar".to_string(),
                type_uri: "/std/int64".to_string(),
            },
        ];

        assert_eq!(derive_table_create_schema(&column_specs).unwrap(), "document_id TEXT PRIMARY KEY, __my__foo TEXT, __my__bar BIGINT");

        let column_specs = vec![
            ColumnSpec {
                column_uri: "/my/foo".to_string(),
                type_uri: "unknown".to_string(),   
            },
        ];

        assert!(derive_table_create_schema(&column_specs).is_err());
    }
}     

