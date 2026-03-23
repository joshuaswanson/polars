use std::sync::Arc;

use polars_error::{PolarsError, PolarsResult, polars_err};
use polars_io::pl_async;
use polars_plan::dsl::sink::{SinkedPathInfo, SinkedPathsCallback, SinkedPathsCallbackArgs};

pub async fn call_sinked_paths_callback(
    sinked_paths_callback: SinkedPathsCallback,
    sinked_path_info_list: SinkedPathInfoList,
) -> PolarsResult<()> {
    pl_async::get_runtime()
        .spawn_blocking(move || {
            let SinkedPathInfoList { path_info_list } = sinked_path_info_list;

            let args = SinkedPathsCallbackArgs {
                path_info_list: std::mem::take(&mut path_info_list.lock()),
            };

            sinked_paths_callback.call_(args)
        })
        .await
        .unwrap()
}

#[derive(Default, Debug, Clone)]
pub struct SinkedPathInfoList {
    pub path_info_list: Arc<parking_lot::Mutex<Vec<SinkedPathInfo>>>,
}

impl SinkedPathInfoList {
    pub fn non_path_error(&self) -> PolarsError {
        polars_err!(
            ComputeError:
            "paths callback was set but encountered non-path sink target"
        )
    }
}
