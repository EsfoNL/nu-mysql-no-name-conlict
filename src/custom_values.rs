use nu_protocol::{CustomValue, LabeledError, Record, ShellError, Span, Value};
use serde::{Deserialize, Serialize};

pub enum InternalDBCon {
    Loaded(sqlx::MySqlConnection),
    Pending(
        std::pin::Pin<
            Box<dyn Send + std::future::Future<Output = sqlx::Result<sqlx::MySqlConnection>>>,
        >,
    ),
    Error(sqlx::Error),
}
impl InternalDBCon {
    pub(crate) async fn get(&mut self) -> Result<&mut sqlx::MySqlConnection, LabeledError> {
        match self {
            InternalDBCon::Loaded(ref mut v) => Ok(v),
            InternalDBCon::Pending(ref mut v) => {
                let val = match v.await {
                    Ok(v) => v,
                    Err(e) => {
                        *self = InternalDBCon::Error(e);
                        let InternalDBCon::Error(ref r) = self else {
                            unreachable!()
                        };
                        return Err(LabeledError::new(r.to_string()));
                    }
                };
                *self = InternalDBCon::Loaded(val);
                let r = match self {
                    InternalDBCon::Loaded(ref mut v) => v,
                    _ => unreachable!(),
                };
                Ok(r)
            }
            InternalDBCon::Error(ref e) => Err(LabeledError::new(e.to_string())),
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct MySqlCon {
    pub(crate) id: usize,
}

#[typetag::serde]
impl CustomValue for MySqlCon {
    fn clone_value(&self, span: Span) -> Value {
        Value::custom(Box::new(self.clone()), span)
    }
    fn type_name(&self) -> String {
        String::from("mysql connection")
    }
    fn to_base_value(&self, span: Span) -> Result<Value, ShellError> {
        let mut rec = Record::new();
        rec.insert("id", Value::int(self.id as i64, span));
        Ok(Value::Record {
            val: Box::new(rec),
            internal_span: span,
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn follow_path_string(
        &self,
        self_span: Span,
        column_name: String,
        path_span: Span,
    ) -> Result<Value, ShellError> {
        todo!("get table")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct MysqlTable {
    id: usize,
    name: String,
}

#[typetag::serde]
impl CustomValue for MysqlTable {
    fn clone_value(&self, span: Span) -> Value {
        todo!()
    }

    fn type_name(&self) -> String {
        todo!()
    }

    fn to_base_value(&self, span: Span) -> Result<Value, ShellError> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        todo!()
    }
}
