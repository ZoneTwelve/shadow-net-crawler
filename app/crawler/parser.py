# ./app/crawler/parser.py
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Set


def strip_noise(html: str) -> str:
    """Remove <script>, <style>, <noscript> – keep the rest."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return str(soup)


def extract_links(html: str, base_url: str) -> Set[str]:
    """Return a set of absolute URLs (fragments stripped)."""
    soup = BeautifulSoup(html, "html.parser")
    links: Set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        absolute = urljoin(base_url, href)
        links.add(absolute.split("#")[0])   # drop fragment
    return links