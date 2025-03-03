import gzip
import os
import sqlite3
import xml.etree.ElementTree as ET
from tqdm import tqdm
from argparse import ArgumentParser
import threading
import concurrent.futures

parser = ArgumentParser(description="Process PubMed Baseline to SQLite DB")
parser.add_argument("directory", help="Directory containing .xml.gz")
parser.add_argument("--output_db", default="pubmed_articles.db", help="Path of resulting DB.")
parser.add_argument("--max_threads", type=int, default=4, help="Maximum number of threads to use.")
args = parser.parse_args()

DB_FILE = args.output_db
DB_LOCK = threading.Lock()


def init_db():
    """Initialize SQLite database with a table for articles."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")  # Enables WAL mode
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            pmcid TEXT PRIMARY KEY,
            pmid TEXT,
            doi TEXT,
            title TEXT,
            journal TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mesh_terms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pmcid TEXT,
            descriptor TEXT,
            ui TEXT,
            major INTEGER,
            qualifier TEXT,
            qual_ui TEXT,
            qual_major INTEGER,
            FOREIGN KEY (pmcid) REFERENCES articles (pmcid)
        )
        """
    )
    conn.commit()
    conn.close()


def parse_pubmed_xml(file_path: str):
    """Parses a PubMed XML.gz file and extracts article data."""
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        tree = ET.parse(f)
        root = tree.getroot()

        articles = []

        for article in tqdm(
            root.findall(".//PubmedArticle"),
            desc=f"Processing {os.path.basename(file_path)}",
        ):
            pmid = article.find(".//PMID").text
            mesh_terms = []
            pmcid = None
            doi = None

            for article_id in article.findall(".//ArticleId"):
                if article_id.get("IdType") == "doi":
                    doi = article_id.text
                elif article_id.get("IdType") == "pmc":
                    pmcid = article_id.text

            if pmcid is not None:
                title = None
                journal = None

                article_title = article.find(".//ArticleTitle")
                if article_title is not None:
                    title = article_title.text

                journal_title = article.find(".//Journal/Title")
                if journal_title is not None:
                    journal = journal_title.text

                for mesh in article.findall(".//MeshHeading"):
                    descriptor = mesh.find("DescriptorName")
                    if descriptor is not None:
                        desc_text = descriptor.text
                        desc_ui = descriptor.get("UI")
                        desc_major = descriptor.get("MajorTopicYN") == "Y"

                        for qualifier in mesh.findall("QualifierName"):
                            qual_text = qualifier.text
                            qual_ui = qualifier.get("UI")
                            qual_major = qualifier.get("MajorTopicYN") == "Y"
                            mesh_terms.append(
                                {
                                    "pmcid": pmcid,
                                    "descriptor": desc_text,
                                    "ui": desc_ui,
                                    "major": int(desc_major),
                                    "qualifier": qual_text,
                                    "qual_ui": qual_ui,
                                    "qual_major": int(qual_major),
                                }
                            )

                articles.append(
                    {
                        "pmcid": pmcid,
                        "pmid": pmid,
                        "doi": doi,
                        "title": title,
                        "journal": journal,
                        "mesh_terms": mesh_terms,
                    }
                )

        return articles


def insert_articles(articles):
    """Inserts articles and mesh terms into SQLite database with a lock."""
    with DB_LOCK:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        for article in articles:
            try:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO articles (pmcid, pmid, doi, title, journal)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        article["pmcid"],
                        article["pmid"],
                        article["doi"],
                        article["title"],
                        article["journal"],
                    ),
                )

                for mesh in article["mesh_terms"]:
                    cursor.execute(
                        """
                        INSERT INTO mesh_terms (pmcid, descriptor, ui, major, qualifier, qual_ui, qual_major)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            mesh["pmcid"],
                            mesh["descriptor"],
                            mesh["ui"],
                            mesh["major"],
                            mesh["qualifier"],
                            mesh["qual_ui"],
                            mesh["qual_major"],
                        ),
                    )
            except sqlite3.IntegrityError:
                print(f"Skipping duplicate entry for PMCID: {article['pmcid']}")

        conn.commit()
        conn.close()


def process_file(file_path: str):
    """Processes a single file: parse the XML and insert its articles into the DB."""
    try:
        print(f"Processing file: {file_path}")
        articles = parse_pubmed_xml(file_path)
        insert_articles(articles)
    except Exception as e:
        print(f"Error processing file: {file_path}")
        print(f"Exception: {e}")


def process_directory(directory: str, max_threads: int):
    """Processes all .xml.gz files in a directory using a thread pool."""
    init_db()

    files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.endswith(".xml.gz")
    ]

    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        executor.map(process_file, files)


if __name__ == "__main__":
    directory = args.directory
    max_threads = args.max_threads
    process_directory(directory, max_threads)
