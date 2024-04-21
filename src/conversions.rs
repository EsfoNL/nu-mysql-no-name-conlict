use futures::Stream;
use nu_protocol::{LabeledError, Record, ShellError, Span, Value};
use sqlx::mysql::{MySqlRow, MySqlValueRef};
use std::collections::HashMap;
use std::hash::Hash;
use tokio::sync::{Mutex, MutexGuard};

pub(crate) trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for MySqlValueRef<'_> {
    fn into_value(self) -> Value {
        use sqlx::TypeInfo as _;
        use sqlx::Value as _;
        use sqlx::ValueRef;
        let type_info = self.type_info();
        if self.is_null() {
            return Value::nothing(Span::unknown());
        }

        match type_info.name() {
            "INT" | "BIGINT" => Value::Int {
                val: ValueRef::to_owned(&self).decode(),
                internal_span: Span::unknown(),
            },
            "INT UNSIGNED" => Value::Int {
                val: ValueRef::to_owned(&self).decode::<u64>() as i64,
                internal_span: Span::unknown(),
            },
            "VARCHAR" | "TEXT" => Value::String {
                val: ValueRef::to_owned(&self).decode(),
                internal_span: Span::unknown(),
            },
            e => todo!("type conversion for {e} not implemented"),
        }
    }
}

pub(crate) async fn sqlstream_to_record_stream<'a>(
    mut stream: impl Stream<Item = Result<MySqlRow, sqlx::Error>>
        + std::marker::Unpin
        + std::marker::Send
        + 'a,
) -> impl Stream<Item = Value> + 'a {
    use futures::StreamExt;
    use sqlx::{Column, Row};
    let Some(val) = stream.next().await else {
        return futures::stream::empty().boxed();
    };
    let val = match val {
        Err(v) => {
            return futures::stream::once(async move {
                Value::Error {
                    error: Box::new(ShellError::LabeledError(Box::new(LabeledError::new(
                        v.to_string(),
                    )))),
                    internal_span: Span::unknown(),
                }
            })
            .boxed();
        }
        Ok(v) => v,
    };
    let columns = val.columns().to_vec();
    let map_closure = move |e: Result<MySqlRow, sqlx::Error>| {
        let e = match e {
            Ok(e) => e,
            Err(e) => {
                return Value::Error {
                    error: Box::new(ShellError::LabeledError(Box::new(LabeledError::new(
                        e.to_string(),
                    )))),
                    internal_span: Span::unknown(),
                }
            }
        };
        let mut rec = Record::new();
        for i in columns.iter() {
            let val = e.try_get_raw(i.ordinal()).unwrap();
            rec.insert(i.name(), val.into_value());
        }
        Value::Record {
            val: Box::new(rec),
            internal_span: Span::unknown(),
        }
    };
    let mapped_val = map_closure(Ok(val));
    futures::stream::once(async move { mapped_val })
        .chain(stream.map(map_closure))
        .boxed()
}

pub(crate) trait ShellLock {
    type Output;
    fn shell_lock(self) -> Result<Self::Output, LabeledError>;
}

impl<'a, T: 'a> ShellLock for &'a Mutex<T> {
    type Output = MutexGuard<'a, T>;

    fn shell_lock(self) -> Result<Self::Output, LabeledError> {
        Ok(self.blocking_lock())
    }
}

pub(crate) trait ShellGet: Sized {
    type Output;
    type Input;
    fn get(&self, input: Self::Input) -> Option<&Self::Output>;
    fn get_mut(&mut self, input: Self::Input) -> Option<&mut Self::Output>;

    fn shell_get(&self, input: Self::Input) -> Result<&Self::Output, LabeledError> {
        self.get(input)
            .ok_or(LabeledError::new("invalid reference"))
    }
    fn shell_get_mut(&mut self, input: Self::Input) -> Result<&mut Self::Output, LabeledError> {
        self.get_mut(input)
            .ok_or(LabeledError::new("invalid reference"))
    }
}

impl<'a, T, V> ShellGet for &'a mut HashMap<T, V>
where
    T: 'static + PartialEq + Hash + Eq,
{
    type Output = V;
    type Input = &'static T;

    fn get(&self, input: &'static T) -> std::option::Option<&V> {
        HashMap::get(self, input)
    }

    fn get_mut(&mut self, input: Self::Input) -> Option<&mut Self::Output> {
        HashMap::get_mut(self, input)
    }

    fn shell_get(&self, input: Self::Input) -> Result<&Self::Output, LabeledError> {
        self.get(input)
            .ok_or(LabeledError::new("invalid reference"))
    }

    fn shell_get_mut(&mut self, input: Self::Input) -> Result<&mut Self::Output, LabeledError> {
        self.get_mut(input)
            .ok_or(LabeledError::new("invalid reference"))
    }
}

impl<T> ShellGet for Vec<T> {
    type Output = T;
    type Input = usize;

    fn get(&self, input: Self::Input) -> std::option::Option<&T> {
        <[T]>::get(self, input)
    }

    fn get_mut(&mut self, input: Self::Input) -> Option<&mut Self::Output> {
        <[T]>::get_mut(self, input)
    }
}
