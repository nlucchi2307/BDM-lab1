from neo4j import GraphDatabase
from data_generator.generator import DataGenerator


class Loader:
    def __init__(self, uri: str, user: str, pwd: str, wipe: bool = False):
        self.driver = GraphDatabase.driver(uri, auth=(user, pwd))
        self.wipe = wipe

    @staticmethod
    def _schema():
        return [
            "CREATE CONSTRAINT au_id IF NOT EXISTS FOR (a:Author)    REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT pa_id IF NOT EXISTS FOR (p:Paper)     REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT co_id IF NOT EXISTS FOR (c:Conference) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT ws_id IF NOT EXISTS FOR (w:Workshop)  REQUIRE w.id IS UNIQUE",
            "CREATE CONSTRAINT ed_id IF NOT EXISTS FOR (d:Edition)   REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT jo_id IF NOT EXISTS FOR (j:Journal)   REQUIRE j.id IS UNIQUE",
            "CREATE CONSTRAINT vo_id IF NOT EXISTS FOR (v:Volume)    REQUIRE v.id IS UNIQUE",
            "CREATE CONSTRAINT kw_id IF NOT EXISTS FOR (k:Keyword)   REQUIRE k.id IS UNIQUE",
            "CREATE INDEX kw_name IF NOT EXISTS FOR (k:Keyword) ON (k.name)",
        ]

    @staticmethod
    def _unwind(label: str, props: list[str]) -> str:
        set_clause = ", ".join(f"n.{p}=row.{p}" for p in props)
        return f"UNWIND $rows AS row MERGE (n:{label}{{id:row.id}}) SET {set_clause}"

    def _batch(self, query: str, rows: list, chunk: int = 500):
        with self.driver.session() as sess:
            for i in range(0, len(rows), chunk):
                sess.run(query, rows=rows[i : i + chunk])

    def load(self, g: DataGenerator):
        with self.driver.session() as sess:
            if self.wipe:
                sess.run("MATCH (n) DETACH DELETE n")
            for q in self._schema():
                sess.run(q)

        self._batch(self._unwind("Author",     ["name", "email"]),          g.authors)
        self._batch(self._unwind("Keyword",    ["name"]),                   g.keywords)

        self._batch(self._unwind("Conference", ["name", "type"]),           g.conferences)
        self._batch(self._unwind("Workshop",   ["name", "type"]),           g.workshops)

        self._batch(self._unwind("Edition",    ["year", "number", "event_id",
                                                "city", "country", "start", "end"]),
                    g.editions)

        self._batch(self._unwind("Journal",    ["name", "issn"]),           g.journals)
        self._batch(self._unwind("Volume",     ["number", "year", "journal_id"]),
                    g.volumes)
        self._batch(self._unwind("Paper",      ["title", "year", "doi",
                                                "abstract", "pages"]),
                    g.papers)

        self._batch(
            """
            UNWIND $rows AS row
            MATCH (a:Author{id:row[0]}), (p:Paper{id:row[1]})
            MERGE (a)-[:AUTHORED]->(p)
            """,
            g.authorship,
        )

        self._batch(
            """
            UNWIND $rows AS row
            MATCH (p:Paper{id:row[0]}), (ed:Edition{id:row[1]})
            MERGE (p)-[:APPEARED_IN]->(ed)
            """,
            g.paper_in_edition,
        )

        conf_ids = {c["id"] for c in g.conferences}
        conf_links = [(ed["id"], ed["event_id"]) for ed in g.editions
                      if ed["event_id"] in conf_ids]

        self._batch(
            """
            UNWIND $rows AS row
            MATCH (ed:Edition{id:row[0]}), (c:Conference{id:row[1]})
            MERGE (ed)-[:OF_EVENT]->(c)
            """,
            conf_links,
        )

        ws_ids = {w["id"] for w in g.workshops}
        ws_links = [(ed["id"], ed["event_id"]) for ed in g.editions
                    if ed["event_id"] in ws_ids]

        self._batch(
            """
            UNWIND $rows AS row
            MATCH (ed:Edition{id:row[0]}), (w:Workshop{id:row[1]})
            MERGE (ed)-[:OF_EVENT]->(w)
            """,
            ws_links,
        )

        self._batch(
            """
            UNWIND $rows AS row
            MATCH (p:Paper{id:row[0]}), (v:Volume{id:row[1]})
            MERGE (p)-[:APPEARED_IN]->(v)
            WITH v
            MATCH (j:Journal{id:v.journal_id})
            MERGE (v)-[:OF_JOURNAL]->(j)
            """,
            g.paper_in_volume,
        )

        self._batch(
            """
            UNWIND $rows AS row
            MATCH (p:Paper{id:row[0]}), (k:Keyword{name:row[1]})
            MERGE (p)-[:HAS_KEYWORD]->(k)
            """,
            g.paper_keywords,
        )

        self._batch(
            """
            UNWIND $rows AS row
            MATCH (citing:Paper{id:row[0]}), (cited:Paper{id:row[1]})
            MERGE (citing)-[:CITES]->(cited)
            """,
            g.citations,
        )

        print("Graph successfully populated.")
