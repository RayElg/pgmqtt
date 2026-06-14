//! Safe wrappers around PostgreSQL FFI operations.
//!
//! This module concentrates all unsafe FFI operations in well-documented,
//! single-responsibility functions. Each function has clear safety requirements.
//!
//! Instead of scattering `unsafe { ... }` throughout callback code, we call
//! these helpers, making the unsafe boundary explicit and auditable.

use pgrx::pg_sys;
use std::ffi::CStr;

/// Convert a PostgreSQL OID to a schema name.
///
/// # Safety
/// - `schema_oid` must be a valid PostgreSQL OID.
/// - The pointer returned by `get_namespace_name` must remain valid for the
///   duration of conversion (it's managed by PostgreSQL).
pub unsafe fn oid_to_schema_name(schema_oid: pg_sys::Oid) -> String {
    let ns_name = pg_sys::get_namespace_name(schema_oid);
    if ns_name.is_null() {
        "unknown".to_string()
    } else {
        CStr::from_ptr(ns_name)
            .to_string_lossy()
            .into_owned()
    }
}

/// Extract a table name from a PostgreSQL Relation pointer.
///
/// # Safety
/// - `relation` must be a valid, non-null Relation pointer.
/// - PostgreSQL guarantees Relation pointers are valid for the duration of
///   the callback in which they're passed.
pub unsafe fn relation_name(relation: pg_sys::Relation) -> String {
    let rel_data = *relation;
    CStr::from_ptr((*rel_data.rd_rel).relname.data.as_ptr())
        .to_string_lossy()
        .into_owned()
}

/// Extract schema OID from a Relation pointer.
///
/// # Safety
/// - `relation` must be a valid, non-null Relation pointer.
pub unsafe fn relation_schema_oid(relation: pg_sys::Relation) -> pg_sys::Oid {
    (*(*relation).rd_rel).relnamespace
}

/// Decode the operation type from a ReorderBufferChange.
///
/// # Safety
/// - `change` must be a valid, non-null ReorderBufferChange pointer.
pub unsafe fn change_operation(change: *mut pg_sys::ReorderBufferChange) -> Option<&'static str> {
    match (*change).action {
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_INSERT => Some("INSERT"),
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_UPDATE => Some("UPDATE"),
        pg_sys::ReorderBufferChangeType::REORDER_BUFFER_CHANGE_DELETE => Some("DELETE"),
        _ => None,
    }
}
