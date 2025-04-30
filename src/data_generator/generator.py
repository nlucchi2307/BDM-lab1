import random
import string
import uuid
from datetime import date, timedelta

from faker import Faker

COUNTS = dict(
    authors=1000,
    papers=500,
    conferences=50,
    workshops=50,
    journals=50,
    keywords=500,
)

YEARS = list(range(2015, 2025))
fake = Faker()
random.seed(42)
Faker.seed(42)

def rand_doi():
    prefix = random.randint(1000, 9999)
    suffix1 = "".join(random.choices(string.ascii_lowercase, k=5))
    suffix2 = random.randint(10000, 99999)
    return f"10.{prefix}/{suffix1}.{suffix2}"


class DataGenerator:
    def __init__(self, counts=COUNTS):
        self.c = counts

        self.authors, self.keywords = [], []
        self.conferences, self.workshops = [], []
        self.editions = []
        self.journals, self.volumes = [], []
        self.papers = []

        self.authorship, self.paper_in_edition, self.paper_in_volume = [], [], []
        self.paper_keywords, self.citations = [], []

        self.authorship: list[tuple[str, str, str]] = []

        self.reviews: list[dict] = []
        self.review_votes: list[tuple[str,str,str]] = [] 
        self.paper_in_review: list[tuple[str, str]] = []

    def run(self):
        pipeline = [
            self._authors,
            self._keywords,
            self._conferences_editions,
            self._workshops_editions,
            self._journals_volumes,
            self._papers,
            self._reviews,
            self._citations,
        ]
        for step in pipeline:
            step()


    def _authors(self):
        for _ in range(self.c["authors"]):
            self.authors.append(
                {"id": str(uuid.uuid4()), "name": fake.name(), "email": fake.email()}
            )

    def _keywords(self):
        for _ in range(self.c["keywords"]):
            self.keywords.append(
                {"id": str(uuid.uuid4()), "name": fake.word()}
            )

    def _conferences_editions(self):
        for idx in range(self.c["conferences"]):
            cid = str(uuid.uuid4())
            self.conferences.append(
                {"id": cid, "name": f"Conf_{fake.company()}_{idx}", "type": "conference"}
            )
            self._make_editions(parent_id=cid)

    def _workshops_editions(self):
        for idx in range(self.c["workshops"]):
            wid = str(uuid.uuid4())
            self.workshops.append(
                {"id": wid, "name": f"WS_{fake.bs().title()}_{idx}", "type": "workshop"}
            )
            self._make_editions(parent_id=wid, max_span=3)

    def _make_editions(self, parent_id, max_span: int = 6):
        first_year = random.choice(YEARS[:-max_span])
        num_editions = random.randint(1, max_span)
        for n in range(num_editions):
            y = first_year + n
            start = date(y, random.randint(1, 12), random.randint(1, 28))
            end = start + timedelta(days=random.randint(2, 4))
            self.editions.append(
                {
                    "id": str(uuid.uuid4()),
                    "year": y,
                    "number": n + 1,
                    "event_id": parent_id,
                    "city": fake.city(),
                    "country": fake.country(),
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                }
            )

    def _journals_volumes(self):
        for idx in range(self.c["journals"]):
            jid = str(uuid.uuid4())
            self.journals.append(
                {
                    "id": jid,
                    "name": f"Journal {idx}",
                    "issn": f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
                }
            )
            for y in (2023, 2024):
                self.volumes.append(
                    {"id": str(uuid.uuid4()), "number": 1, "year": y, "journal_id": jid}
                )

    def _reviews(self):
        ed2event = {ed["id"]: ed["event_id"] for ed in self.editions}

        conf_ids = {c["id"] for c in self.conferences}
        ws_ids   = {w["id"] for w in self.workshops}

        p_in_ed  = {pid: edid for pid, edid in self.paper_in_edition}
        p_in_vol = {pid: vid  for pid, vid  in self.paper_in_volume}

        for p in self.papers:
            pid = p["id"]

            if pid in p_in_ed:
                event_id = ed2event[p_in_ed[pid]]
                if event_id in ws_ids:
                    continue

            elif pid in p_in_vol:
                pass
            else:
                continue

            authors_on_paper = {a for a, p_, _ in self.authorship if p_ == pid}
            candidates = [a for a in self.authors if a["id"] not in authors_on_paper]
            if len(candidates) < 3:
                continue

            reviewers = random.sample(candidates, 3)
            votes = ['accept' if random.random() < 0.5 else 'reject' for _ in range(3)]

            for rev_auth, vote in zip(reviewers, votes):
                rid = str(uuid.uuid4())
                self.reviews.append({'id': rid, 'decision': vote})
                self.paper_in_review.append((pid, rid))
                self.review_votes.append((rev_auth["id"], rid, vote))

    def _papers(self):
        for ev in (self.conferences + self.workshops):
            core_authors = random.sample(self.authors, 5)
            ev_editions = [e for e in self.editions if e["event_id"] == ev["id"]]
            for ed in ev_editions:
                for _ in range(3):
                    pid = str(uuid.uuid4())
                    self.papers.append(
                        {
                            "id": pid,
                            "title": fake.sentence(nb_words=6).rstrip("."),
                            "year": ed["year"],
                            "doi": rand_doi(),
                            "abstract": fake.paragraph(),
                            "pages": "1-10",
                        }
                    )
                    self.paper_in_edition.append((pid, ed["id"])) 
                    picked = random.sample(core_authors, 3)
                    self.authorship.append((picked[0]["id"], pid, "author"))
                    for a in picked[1:]:
                        self.authorship.append((a["id"], pid, "coauthor"))

        for vol in self.volumes:
            for _ in range(2):
                pid = str(uuid.uuid4())
                self.papers.append(
                    {
                        "id": pid,
                        "title": fake.sentence(nb_words=8).rstrip("."),
                        "year": vol["year"],
                        "doi": rand_doi(),
                        "abstract": fake.paragraph(),
                        "pages": "11-20",
                    }
                )
                self.paper_in_volume.append((pid, vol["id"]))

                main = random.choice(self.authors)
                self.authorship.append((main["id"], pid, "author"))

        for p in self.papers:
            for kw in random.sample(self.keywords, random.randint(1, 4)):
                self.paper_keywords.append((p["id"], kw["name"]))

    def _citations(self):
        ids = [p["id"] for p in self.papers]
        for p in ids:
            for target in random.sample(ids, 5):
                if target != p:
                    self.citations.append((p, target))
