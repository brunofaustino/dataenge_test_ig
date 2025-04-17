import pytest
from pathlib import Path
from src.data_processor import CommonCrawlProcessor

@pytest.fixture
def processor():
    return CommonCrawlProcessor(
        db_url="postgresql://dataengineer:dataengineer@postgres/crawldata",
        data_dir=Path("test_data")
    )

def test_is_homepage(processor):
    assert processor._is_homepage("https://example.com")
    assert processor._is_homepage("https://example.com/")
    assert processor._is_homepage("https://example.com/index.html")
    assert not processor._is_homepage("https://example.com/page.html")

def test_get_subsection(processor):
    assert processor._get_subsection("https://example.com/blog/post") == "blog"
    assert processor._get_subsection("https://example.com") is None
    assert processor._get_subsection("https://example.com/") is None

def test_extract_links(processor, tmp_path):
    # Create a mock WARC file
    warc_file = tmp_path / "test.warc"
    with open(warc_file, "w") as f:
        f.write("WARC/1.0\n")
        f.write("WARC-Type: response\n")
        f.write("WARC-Target-URI: https://example.com\n")
        f.write("Content-Type: text/html\n\n")
        f.write("<html><body><a href='https://test.com'>Link</a></body></html>")
    
    df = processor.extract_links(warc_file)
    assert len(df) == 1
    assert df.iloc[0]["source_domain"] == "example"
    assert df.iloc[0]["target_domain"] == "test"
    assert df.iloc[0]["is_homepage"] 