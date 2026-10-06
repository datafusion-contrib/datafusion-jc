mod serialization;
mod schema_provider;

mod catalog_provider;

pub use crate::catalog_provider::DatafusionJsonCatalog;


#[cfg(test)]
mod tests {
    // #[tokio::test]
    // async fn write_to_json_catalog() {
    //     let tmp_file = tempfile::NamedTempFile::new().unwrap();
    //     let catalog = DatafusionJsonSchema::new();
    //
    //     assert!(catalog.table_names().is_empty());
    //
    //     let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    //     let schema_ref = SchemaRef::new(schema);
    //     let memtable = MemTable::try_new(
    //         schema_ref.clone(),
    //         vec![vec![RecordBatch::new_empty(schema_ref)]],
    //     )
    //         .unwrap();
    //     catalog
    //         .register_table("tadashi".to_owned(), Arc::new(memtable))
    //         .unwrap();
    //
    //     let schema_2 = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    //     let schema_ref_2 = SchemaRef::new(schema_2);
    //     let memtable_2 = MemTable::try_new(
    //         schema_ref_2.clone(),
    //         vec![vec![RecordBatch::new_empty(schema_ref_2)]],
    //     )
    //         .unwrap();
    //     catalog
    //         .register_table("mizu".to_owned(), Arc::new(memtable_2))
    //         .unwrap();
    //
    //     let tables = catalog.table_names();
    //     assert_eq!(tables.len(), 2);
    //     assert!(tables.contains(&"tadashi".to_owned()));
    //     assert!(tables.contains(&"mizu".to_owned()));
    //
    //     let table = catalog.table("tadashi").await.unwrap();
    //     assert!(table.is_some());
    //     let table = table.unwrap();
    //     let table_ref = datafusion::common::TableReference::from("tadashi");
    // }
}
