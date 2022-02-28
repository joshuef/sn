// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::read_network_conn_info;
use crate::client::{Client, ClientConfig};
use eyre::Result;
use sn_interface::types::Keypair;
use std::{sync::Once, time::Duration};
use tempfile::tempdir;
use tracing_core::{Event, Subscriber};
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::{
    fmt, format::Writer, FmtContext, FormatEvent, FormatFields, FormattedFields,
};
use tracing_subscriber::{registry::LookupSpan, EnvFilter};

static INIT: Once = Once::new();

#[derive(Default)]
struct MyFormatter;

impl<S, N> FormatEvent<S, N> for MyFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        // Write level and target
        let level = *event.metadata().level();
        let target = event.metadata().file().expect("will never be `None`");
        let span_separation_string = "\t ➤ ";
        let time = SystemTime::default();
        write!(writer, " {} ", level)?;

        time.format_time(&mut writer)?;

        writeln!(
            writer,
            " [{}:L{}]:",
            target,
            event.metadata().line().expect("will never be `None`"),
        )?;

        write!(writer, "{}", span_separation_string)?;

        // let mut span_count = 0;
        // Write spans and fields of each span
        ctx.visit_spans(|span| {
            write!(writer, "{} ", span.name())?;

            let ext = span.extensions();

            // `FormattedFields` is a a formatted representation of the span's
            // fields, which is stored in its extensions by the `fmt` layer's
            // `new_span` method. The fields will have been formatted
            // by the same field formatter that's provided to the event
            // formatter in the `FmtContext`.
            let fields = &ext
                .get::<FormattedFields<N>>()
                .expect("will never be `None`");

            if !fields.is_empty() {
                write!(writer, "{{{}}}", fields)?;
            }

            write!(writer, "\n{}", span_separation_string)?;

            Ok(())
        })?;

        // Write fields on the event
        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}

/// Initialise logger for tests, this is run only once, even if called multiple times.
pub fn init_test_logger() {
    INIT.call_once(|| {
        fmt()
            // NOTE: uncomment this line for pretty printed log output.
            //.pretty()
            .with_thread_names(true)
            .with_ansi(false)
            .with_env_filter(EnvFilter::from_default_env())
            .with_target(false)
            .event_format(MyFormatter::default())
            .init()
    });
}

/// Create a test client without providing any specific keypair, bootstrap_config, or timeout.
pub async fn create_test_client() -> Result<Client> {
    create_test_client_with(None, None, false).await
}

/// Create a test client optionally providing keypair and/or bootstrap_config
/// If no keypair is provided, a check is run that a balance has been generated for the client
pub async fn create_test_client_with(
    optional_keypair: Option<Keypair>,
    timeout: Option<u64>,
    read_prefix_map: bool,
) -> Result<Client> {
    let root_dir = tempdir().map_err(|e| eyre::eyre!(e.to_string()))?;
    let timeout = timeout.map(Duration::from_secs);
    let (genesis_key, bootstrap_nodes) = read_network_conn_info()?;

    // use standard wait
    let cmd_ack_wait = None;

    let config = ClientConfig::new(
        Some(root_dir.path()),
        None,
        genesis_key,
        None,
        timeout,
        timeout,
        cmd_ack_wait,
    )
    .await;
    let client = Client::create_with(
        config,
        bootstrap_nodes,
        optional_keypair.clone(),
        read_prefix_map,
    )
    .await?;

    Ok(client)
}
