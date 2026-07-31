from pathlib import Path


def test_required_files_exist():
    root = Path(__file__).resolve().parents[1]
    required = [
        "main.tf",
        "glue.tf",
        "iam.tf",
        "lambda.tf",
        "step_functions.tf",
        "glue_scripts/lakehouse_delta_etl.py",
        "glue_scripts/quality_checks.py",
        "lambda/archive_files.py",
    ]
    for file in required:
        assert (root / file).exists(), file


def test_glue_script_mentions_delta():
    root = Path(__file__).resolve().parents[1]
    script = (root / "glue_scripts/lakehouse_delta_etl.py").read_text()
    assert "DeltaTable" in script
    assert "write_delta_upsert" in script


def test_quality_checks_validates_catalog_and_delta_log():
    root = Path(__file__).resolve().parents[1]
    script = (root / "glue_scripts/quality_checks.py").read_text()
    assert "get_table" in script
    assert "_delta_log" in script
