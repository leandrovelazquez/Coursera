import re
import requests
from bs4 import BeautifulSoup


def print_secret_message(doc_url: str) -> None:

    response = requests.get(doc_url, timeout=20)
    response.raise_for_status()
    html = response.text

    entries = _parse_entries_from_html(html)

    if not entries:
        raise ValueError("No valid coordinate/character entries were found in the document.")

    max_x = max(x for x, _, _ in entries)
    max_y = max(y for _, _, y in entries)

    grid = [[" " for _ in range(max_x + 1)] for _ in range(max_y + 1)]

    for x, char, y in entries:
        grid[y][x] = char

    for row in grid:
        print("".join(row))


def _parse_entries_from_html(html: str):
    
    soup = BeautifulSoup(html, "html.parser")
    entries = []

    tables = soup.find_all("table")
    for table in tables:
        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) >= 3:
                maybe_x, maybe_char, maybe_y = cells[0], cells[1], cells[2]
                if maybe_x.isdigit() and maybe_y.isdigit() and len(maybe_char) >= 1:
                    entries.append((int(maybe_x), maybe_char[0], int(maybe_y)))

    if entries:
        return entries

    text = soup.get_text("\n")
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    filtered = []
    for line in lines:
        lower = line.lower()
        if lower in {"x-coordinate", "x", "character", "char", "y-coordinate", "y"}:
            continue
        filtered.append(line)

    lines = filtered

    i = 0
    while i + 2 < len(lines):
        if lines[i].isdigit() and lines[i + 2].isdigit() and len(lines[i + 1]) >= 1:
            entries.append((int(lines[i]), lines[i + 1][0], int(lines[i + 2])))
            i += 3
        else:
            i += 1

    if entries:
        return entries

    pattern = re.compile(r"(\d+)\s+(\S)\s+(\d+)")
    for match in pattern.finditer(text):
        x = int(match.group(1))
        char = match.group(2)
        y = int(match.group(3))
        entries.append((x, char, y))

    return entries

print_secret_message("https://docs.google.com/document/d/e/2PACX-1vSvM5gDlNvt7npYHhp_XfsJvuntUhq184By5xO_pA4b_gCWeXb6dM6ZxwN8rE6S4ghUsCj2VKR21oEP/pub")