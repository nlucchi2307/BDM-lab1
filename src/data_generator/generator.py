import os
import random
import string
import uuid
from datetime import date, timedelta

from faker import Faker

COUNTS = dict(authors=1000, papers=500, conferences=50, journals=50, keywords=500)

YEARS = list(range(2015, 2025))
fake = Faker()
random.seed(42); Faker.seed(42)

def rand_doi():
    """
    Generates a random DOI in the format 10.XXXX/xxxxx.xxxxx
    where XXXX is a random number between 1000 and 9999
    """
    return f"10.{random.randint(1000,9999)}/ {"".join(random.choices(string.ascii_lowercase,k=5))}.{random.randint(10000,99999)}"

class DataGenerator:
    def __init__(self, counts=COUNTS):
        """
        initialize the DataGenerator with the given counts.

        :param counts: Dictionary with the number of authors, papers, conferences, journals and keywords
        """
        self.c = counts

        self.authors, self.keywords = [], []
        self.events, self.editions = [], []
        self.journals, self.volumes = [], []
        self.papers = []

        self.authorship, self.paper_in_edition, self.paper_in_volume = [], [], []
        self.paper_keywords, self.citations = [], []

    def run(self):
        pipeline = [self._authors, self._keywords, self._events_editions, self._journals_volumes, self._papers, self._citations]
        for method in pipeline:
            method()

    def _authors(self):
        for _ in range(self.c["authors"]):
            self.authors.append(
                {"id": str(uuid.uuid4()), "name": fake.name(), "email": fake.email()}
            )

    def _keywords(self):
        pool = [fake.word() for _ in range(self.c["keywords"])]
        for kw in pool:
            self.keywords.append({"id": str(uuid.uuid4()), "name": kw})

    def _events_editions(self):
        for idx in range(self.c["conferences"]):
            eid = str(uuid.uuid4())
            self.events.append({"id": eid, "name": f"Conf_{fake.company()}_{idx}", "type": "conference"})
            first = random.choice(YEARS[:-4])
            for n in range(random.randint(1,6)):
                y = first + n
                start = date(y, random.randint(1,12), random.randint(1,28))
                end   = start + timedelta(days=3)
                self.editions.append({
                    "id": str(uuid.uuid4()), "year": y, "number": n+1,
                    "event_id": eid, "city": fake.city(), "country": fake.country(),
                    "start": start.isoformat(), "end": end.isoformat()
                })

    def _journals_volumes(self):
        for idx in range(self.c["journals"]):
            jid = str(uuid.uuid4())
            self.journals.append({"id": jid, "name": f"Journal {idx}",
                                  "issn": f"{random.randint(1000,9999)}-{random.randint(1000,9999)}"})
            
            for y in (2022, 2023):      
                self.volumes.append({"id": str(uuid.uuid4()),
                                     "number": 1, "year": y, "journal_id": jid})

    def _papers(self):
        for conf in self.events:
            core = random.sample(self.authors, 5)
            eds  = [e for e in self.editions if e["event_id"] == conf["id"]]
            for ed in eds:
                for _ in range(3):
                    pid = str(uuid.uuid4())
                    self.papers.append({
                        "id": pid, "title": fake.sentence(nb_words=6).rstrip("."),
                        "year": ed["year"], "doi": rand_doi(),
                        "abstract": fake.paragraph(), "pages": "1-10"
                    })
                    self.paper_in_edition.append((pid, ed["id"]))
                    authors = random.sample(core, 3)
                    for a in authors:
                        self.authorship.append((a["id"], pid))

        for vol in self.volumes:
            for _ in range(2):
                pid = str(uuid.uuid4())
                self.papers.append({
                    "id": pid, "title": fake.sentence(nb_words=8).rstrip("."),
                    "year": vol["year"], "doi": rand_doi(),
                    "abstract": fake.paragraph(), "pages": "11-20"
                })
                self.paper_in_volume.append((pid, vol["id"]))
                a = random.choice(self.authors)
                self.authorship.append((a["id"], pid))

        for p in self.papers:
            for kw in random.sample(self.keywords, random.randint(1,4)):
                self.paper_keywords.append((p["id"], kw["name"]))

    def _citations(self):
        ids = [p["id"] for p in self.papers]
        for p in self.papers:
            targets = random.sample(ids, 5)
            for t in targets:
                if t != p["id"]:
                    self.citations.append((p["id"], t))
