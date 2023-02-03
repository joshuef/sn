mod appender;

use sn_interface::LogFormatter;
use sn_node::node::{Config, Result};

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    filter::{EnvFilter, Targets},
    fmt as tracing_fmt,
    layer::Filter,
    prelude::*,
    Layer, Registry,
};

#[derive(Default)]
pub struct TracingLayers {
    layers: Vec<Box<dyn Layer<Registry> + Send + Sync>>,
    guard: Option<WorkerGuard>,
}

impl TracingLayers {
    fn fmt_layer(&mut self, config: &Config) {
        // Filter by log level either from `RUST_LOG` or default to crate only.
        let target_filter: Box<dyn Filter<Registry> + Send + Sync> =
            if let Ok(f) = EnvFilter::try_from_default_env() {
                Box::new(f)
            } else {
                Box::new(Targets::new().with_target(current_crate_str(), config.verbose()))
            };
        let fmt_layer = tracing_fmt::layer().with_ansi(false);

        if let Some(log_dir) = config.log_dir() {
            println!("Starting logging to directory: {log_dir:?}");

            let (non_blocking, worker_guard) = appender::file_rotater(
                log_dir,
                config.logs_max_bytes,
                config.logs_max_lines,
                config.logs_retained,
                config.logs_uncompressed,
            );
            self.guard = Some(worker_guard);

            let fmt_layer = fmt_layer.with_writer(non_blocking);

            if config.json_logs {
                let layer = fmt_layer.json().with_filter(target_filter).boxed();
                self.layers.push(layer);
            } else {
                let layer = fmt_layer
                    .event_format(LogFormatter::default())
                    .with_filter(target_filter)
                    .boxed();
                self.layers.push(layer);
            }
        } else {
            println!("Starting logging to stdout");

            let layer = fmt_layer
                .with_target(false)
                .event_format(LogFormatter::default())
                .with_filter(target_filter)
                .boxed();
            self.layers.push(layer);
        };
    }

    #[cfg(feature = "otlp")]
    fn otlp_layer(&mut self) -> Result<()> {
        use opentelemetry::{
            sdk::{trace, Resource},
            KeyValue,
        };
        use opentelemetry_otlp::WithExportConfig;
        use opentelemetry_semantic_conventions::resource::{SERVICE_INSTANCE_ID, SERVICE_NAME};
        use rand::{distributions::Alphanumeric, thread_rng, Rng};
        use tracing_subscriber::filter::LevelFilter;

        // Set the service_name through env variable. Defaults to a randomly generated name
        let service_name = std::env::var("OTLP_SERVICE_NAME").unwrap_or_else(|_| {
            let random_node_name: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect();
            format!("{}_{}", current_crate_str(), random_node_name)
        });
        println!("The opentelemetry traces are logged under the name: {service_name}");

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    // Derive endpoints etc. from environment variables like `OTEL_EXPORTER_OTLP_ENDPOINT`
                    .with_env(),
            )
            .with_trace_config(trace::config().with_resource(Resource::new(vec![
                KeyValue::new(SERVICE_NAME, service_name),
                KeyValue::new(SERVICE_INSTANCE_ID, std::process::id().to_string()),
            ])))
            .install_batch(opentelemetry::runtime::Tokio)?;

        // Set filter level through env variable. Defaults to Info log level
        let env_filter = EnvFilter::builder()
            .with_default_directive(LevelFilter::INFO.into())
            .with_env_var("RUST_LOG_OTLP")
            .from_env_lossy();

        let otlp_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(env_filter)
            .boxed();
        self.layers.push(otlp_layer);
        Ok(())
    }
}

/// Inits node logging, returning the global node guard if required.
/// This guard should be held for the life of the program.
///
/// Logging should be instantiated only once.
pub fn init_node_logging(config: &Config) -> Result<Option<WorkerGuard>> {
    let mut layers = TracingLayers::default();
    layers.fmt_layer(config);

    #[cfg(feature = "otlp")]
    {
        use tracing::info;
        match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
            Ok(_) => layers.otlp_layer()?,
            Err(_) => info!(
                "The OTLP feature is enabled but the OTEL_EXPORTER_OTLP_ENDPOINT variable is not \
                set, so traces will not be submitted."
            ),
        }
    }

    #[cfg(feature = "tokio-console")]
    layers.layers.push(console_subscriber::spawn().boxed());

    tracing_subscriber::registry().with(layers.layers).init();

    Ok(layers.guard)
}

/// Get current root module name (e.g. "sn_node")
fn current_crate_str() -> &'static str {
    // Grab root from module path ("sn_node::log::etc" -> "sn_node")
    let m = module_path!();
    &m[..m.find(':').unwrap_or(m.len())]
}
