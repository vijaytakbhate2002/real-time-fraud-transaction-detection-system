from pathlib import Path

project_root = next(
    (candidate for candidate in (Path.cwd(), *Path.cwd().parents) if (candidate / "config.py").is_file()),
    Path.cwd(),
).parents[1]

processed_data_path = project_root / "data" / "processed" / "transactions_parquet"
checkpoint_path = project_root / "data" / "checkpoints" / "transactions_parquet"
