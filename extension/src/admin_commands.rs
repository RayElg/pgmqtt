//! Admin command queue drained by the BGW main loop.
//!
//! SQL functions (`pgmqtt_disconnect_client`, `pgmqtt_disconnect_role`,
//! `pgmqtt_reload_acls`) insert rows into `pgmqtt_admin_commands`. Each tick
//! the BGW calls [`drain`] to pull pending rows, delete them in the same
//! transaction, and return them as a typed `Vec<Command>` for dispatch.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::spi::Spi;

#[derive(Debug, Clone)]
pub enum Command {
    /// Disconnect a single client by `client_id`.
    DisconnectClient { client_id: String, reason: u8 },
    /// Disconnect every client whose authenticated role matches `role_name`.
    DisconnectRole { role_name: String, reason: u8 },
    /// Re-read pgmqtt_acls for the named client (or all clients when "*").
    ReloadAcls { target: String },
}

/// Drain up to `limit` pending admin commands.
///
/// Each call wraps the SELECT + DELETE in a single BGW transaction so that a
/// crash mid-tick doesn't replay commands.
pub fn drain(limit: usize) -> Vec<Command> {
    let sql = format!(
        "WITH popped AS ( \
             DELETE FROM pgmqtt_admin_commands \
             WHERE id IN ( \
                 SELECT id FROM pgmqtt_admin_commands ORDER BY id LIMIT {} \
             ) \
             RETURNING id, kind, target, reason_code \
         ) \
         SELECT id, kind, target, reason_code FROM popped ORDER BY id",
        limit
    );
    BackgroundWorker::transaction(|| {
        let result: Result<Vec<Command>, pgrx::spi::Error> = Spi::connect_mut(|client| {
            let table = client.update(&sql, None, &[])?;
            let mut rows = Vec::new();
            for row in table {
                let kind: String = row.get::<String>(2)?.unwrap_or_default();
                let target: String = row.get::<String>(3)?.unwrap_or_default();
                let reason_raw: Option<i16> = row.get::<i16>(4)?;
                let reason = reason_raw.map(|r| r as u8).unwrap_or(crate::mqtt::reason::NOT_AUTHORIZED);
                match kind.as_str() {
                    "disconnect_client" => rows.push(Command::DisconnectClient {
                        client_id: target,
                        reason,
                    }),
                    "disconnect_role" => rows.push(Command::DisconnectRole {
                        role_name: target,
                        reason,
                    }),
                    "reload_acls" => rows.push(Command::ReloadAcls { target }),
                    other => {
                        pgrx::log!(
                            "pgmqtt admin: unknown command kind '{}', ignoring",
                            other
                        );
                    }
                }
            }
            Ok(rows)
        });
        match result {
            Ok(rows) => rows,
            Err(e) => {
                pgrx::log!("pgmqtt admin: failed to drain command queue: {}", e);
                Vec::new()
            }
        }
    })
}
