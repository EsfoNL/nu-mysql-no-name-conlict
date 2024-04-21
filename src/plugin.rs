use std::sync::Arc;

use nu_plugin::{Plugin, PluginCommand};
use nu_protocol::{LabeledError, ListStream, PipelineData, Signature, Span, SyntaxShape, Value};
use sqlx::Connection;
use tokio::sync::Mutex;

use crate::{
    conversions::{sqlstream_to_record_stream, ShellGet, ShellLock},
    custom_values::{InternalDBCon, MySqlCon},
};

pub(crate) struct MyPlugin {
    databases: Mutex<Vec<Arc<tokio::sync::Mutex<InternalDBCon>>>>,
    tokio_context: tokio::runtime::Runtime,
}

impl MyPlugin {
    pub(crate) fn new() -> Self {
        Self {
            databases: Mutex::new(vec![]),
            tokio_context: tokio::runtime::Runtime::new().unwrap(),
        }
    }
}

impl Plugin for MyPlugin {
    fn commands(&self) -> Vec<Box<dyn nu_plugin::PluginCommand<Plugin = Self>>> {
        vec![Box::new(Connect), Box::new(Query)]
    }
}

struct Query;

impl PluginCommand for Query {
    type Plugin = MyPlugin;

    fn name(&self) -> &str {
        "mysql query"
    }

    fn signature(&self) -> Signature {
        Signature::new(self.name())
            .input_output_type(
                nu_protocol::Type::Custom(String::from("mysql connection")),
                nu_protocol::Type::Any,
            )
            .required("query", SyntaxShape::String, "the query to execute")
            .optional("bindings", SyntaxShape::Any, "values to bind in query")
    }

    fn usage(&self) -> &str {
        "query a db"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        _engine: &nu_plugin::EngineInterface,
        call: &nu_plugin::EvaluatedCall,
        input: nu_protocol::PipelineData,
    ) -> Result<nu_protocol::PipelineData, LabeledError> {
        let Value::Custom {
            val: var,
            internal_span: _span,
        } = input.into_value(Span::unknown())
        else {
            return Err(LabeledError::new("Invalid type"));
        };
        let Some(v) = var.as_any().downcast_ref::<MySqlCon>() else {
            return Err(LabeledError::new("Invalid type"));
        };
        let query = call.positional.shell_get(0)?.as_str()?.to_string();
        let binds = call.positional.get(1).cloned();
        let lock = plugin.databases.shell_lock()?;
        let db_arc = lock.shell_get(v.id)?.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop1 = stop.clone();
        plugin.tokio_context.spawn(async move {
            use futures::StreamExt;
            use sqlx::Executor;
            let mut db_lock = db_arc.lock().await;
            let db = match db_lock.get().await {
                Ok(v) => v,
                Err(v) => {
                    let _ = tx.send(Value::error(v.into(), Span::unknown())).await;
                    return;
                }
            };
            if binds.is_some() {
                todo!("implement query binds")
            }
            let res = db.fetch(sqlx::query(&query));
            let mut res = sqlstream_to_record_stream(res).await;
            while let (Some(v), false) = (
                res.next().await,
                stop.load(std::sync::atomic::Ordering::Relaxed),
            ) {
                if tx.send(v).await.is_err() {
                    break;
                };
            }
        });
        Ok(PipelineData::ListStream(
            ListStream::from_stream(std::iter::from_fn(move || rx.blocking_recv()), Some(stop1)),
            None,
        ))
    }
}

struct Connect;

impl PluginCommand for Connect {
    type Plugin = MyPlugin;

    fn name(&self) -> &str {
        "mysql open"
    }

    fn signature(&self) -> nu_protocol::Signature {
        Signature::new(self.name()).required("url", SyntaxShape::String, "url to connect to")
    }

    fn usage(&self) -> &str {
        "help"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &nu_plugin::EngineInterface,
        call: &nu_plugin::EvaluatedCall,
        _input: nu_protocol::PipelineData,
    ) -> Result<nu_protocol::PipelineData, nu_protocol::LabeledError> {
        let url = call
            .positional
            .first()
            .ok_or(LabeledError::new("help"))?
            .as_str()?;
        engine.set_gc_disabled(true).unwrap();
        let conn = sqlx::MySqlConnection::connect(url);
        let mut lock = plugin.databases.shell_lock()?;
        lock.push(Arc::new(Mutex::new(InternalDBCon::Pending(conn))));
        let id = lock.len() - 1;
        Ok(nu_protocol::PipelineData::Value(
            Value::custom(Box::new(MySqlCon { id }), Span::unknown()),
            None,
        ))
    }
}
