import pytest
import pathlib
import json
from src.sp_data_v16.transformation.schema_manager import SchemaManager

@pytest.fixture
def schema_manager_env(tmp_path: pathlib.Path):
    """
    Sets up a temporary SchemaManager with a dummy schemas.json file.
    """
    schemas_data = {
        "schema_utf8_test": {"keywords": ["UTF8Keyword", "測試UTF8"]},
        "schema_big5_test": {"keywords": ["BIG5Keyword", "測試BIG5"]},
        "schema_ascii_only_test": {"keywords": ["ASCIIOnly"]}
    }
    schema_file = tmp_path / "temporary_schemas.json"
    with open(schema_file, 'w', encoding='utf-8') as f:
        json.dump(schemas_data, f)
    manager = SchemaManager(str(schema_file))
    return manager

def test_identifies_utf8_schema(schema_manager_env: SchemaManager):
    """Tests identification of a schema using UTF-8 encoded content."""
    manager = schema_manager_env
    raw_content = "This is a file containing UTF8Keyword.".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content) == "schema_utf8_test"

    raw_content_alt = "另一個包含 測試UTF8 的檔案".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content_alt) == "schema_utf8_test"

def test_identifies_big5_schema(schema_manager_env: SchemaManager):
    """Tests identification of a schema using BIG5 encoded content."""
    manager = schema_manager_env
    # BIG5Keyword needs to be representable in BIG5. Assuming it's ASCII.
    # If "BIG5Keyword" itself contains non-ASCII chars not in BIG5, this part of test would be problematic.
    # For simplicity, using an ASCII keyword here for the first assertion.
    raw_content_ascii_in_big5 = "File with ASCIIOnly keyword".encode('big5') # Assuming ASCIIOnly is in schema_big5_test or a new one
    # Let's adjust the fixture to have a schema that makes more sense for this
    # For the provided fixture, let's test "BIG5Keyword" as ASCII
    raw_content_big5_kwd = "A file that has BIG5Keyword here.".encode('big5')
    assert manager.identify_schema_from_content(raw_content_big5_kwd) == "schema_big5_test"

    raw_content_alt = "另一個包含 測試BIG5 的檔案".encode('big5')
    assert manager.identify_schema_from_content(raw_content_alt) == "schema_big5_test"

def test_returns_none_for_no_match(schema_manager_env: SchemaManager):
    """Tests that None is returned when no keywords match."""
    manager = schema_manager_env
    raw_content = "This file has no relevant keywords.".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content) is None

def test_handles_decode_error_gracefully(schema_manager_env: SchemaManager):
    """Tests that None is returned if content cannot be decoded by UTF-8 or BIG5."""
    manager = schema_manager_env
    raw_content = b'\xff\xfe\x41\x00'  # UTF-16LE BOM + 'A', will fail UTF-8 and BIG5 decoding
    assert manager.identify_schema_from_content(raw_content) is None

def test_empty_content_returns_none(schema_manager_env: SchemaManager):
    """Tests that empty byte content returns None."""
    manager = schema_manager_env
    raw_content = b''
    assert manager.identify_schema_from_content(raw_content) is None

def test_schema_file_not_found(tmp_path: pathlib.Path):
    """Tests behavior when the schema file is not found."""
    manager = SchemaManager(str(tmp_path / "non_existent_schemas.json"))
    assert manager.schemas == {}  # Expect schemas to be empty

    raw_content = "Any content".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content) is None # No schemas to match against

def test_schema_with_no_keywords(tmp_path: pathlib.Path):
    """Tests that a schema definition with no keywords is skipped."""
    schemas_data = {
        "schema_with_keywords": {"keywords": ["RelevantKeyword"]},
        "schema_no_keywords": {"keywords": []},
        "schema_missing_keywords_key": {}
    }
    schema_file = tmp_path / "test_no_keywords_schema.json"
    with open(schema_file, 'w', encoding='utf-8') as f:
        json.dump(schemas_data, f)

    manager = SchemaManager(str(schema_file))

    content_for_no_keywords = "This content should not match schema_no_keywords".encode('utf-8')
    assert manager.identify_schema_from_content(content_for_no_keywords) is None

    content_for_missing_keywords = "This content should not match schema_missing_keywords_key".encode('utf-8')
    assert manager.identify_schema_from_content(content_for_missing_keywords) is None

    content_for_with_keywords = "RelevantKeyword is here".encode('utf-8')
    assert manager.identify_schema_from_content(content_for_with_keywords) == "schema_with_keywords"

def test_json_decode_error(tmp_path: pathlib.Path):
    """Tests behavior when the schema file contains invalid JSON."""
    schema_file = tmp_path / "invalid_schemas.json"
    with open(schema_file, 'w', encoding='utf-8') as f:
        f.write("{'keywords': ['bad json, this is not good']}") # Invalid JSON (single quotes)

    manager = SchemaManager(str(schema_file))
    assert manager.schemas == {} # Expect schemas to be empty due to decode error

    raw_content = "Any content".encode('utf-8')
    assert manager.identify_schema_from_content(raw_content) is None

# Note: The original test_identifies_big5_schema had a slight ambiguity
# with "BIG5Keyword" if it wasn't ASCII. The fixture was updated to
# schema_ascii_only_test to make one part of such a test clearer,
# but then the test was re-focused on testing BIG5 encoded keywords directly.
# The current `schema_manager_env` fixture is:
# {
#     "schema_utf8_test": {"keywords": ["UTF8Keyword", "測試UTF8"]},
#     "schema_big5_test": {"keywords": ["BIG5Keyword", "測試BIG5"]},
#     "schema_ascii_only_test": {"keywords": ["ASCIIOnly"]} # Added this for clarity
# }
# The tests above should work with this updated fixture.
# The `test_identifies_big5_schema` uses "BIG5Keyword" which is ASCII and thus BIG5 compatible.
# The Chinese characters "測試BIG5" are valid BIG5 characters.
