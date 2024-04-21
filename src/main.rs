use nu_plugin::serve_plugin;
use nu_plugin::JsonSerializer as Serializer;

mod conversions;
mod custom_values;
mod plugin;

use plugin::MyPlugin;

fn main() {
    let plugin = MyPlugin::new();
    serve_plugin(&plugin, Serializer)
}
