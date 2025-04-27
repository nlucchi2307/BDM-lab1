from neo4j import GraphDatabase

from data_generator.generator import DataGenerator

class Loader:
    def __init__(self, uri: str, user: str, pwd: str, wipe: bool = False):
        """
        :param uri: URI of the Neo4j database
        :param user: Username for authentication
        :param pwd: Password for authentication
        :param wipe: Whether to wipe the database before loading
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, pwd))
        self.wipe = wipe

    @staticmethod
    def _schema():
        return [
            "CREATE CONSTRAINT au_id  IF NOT EXISTS FOR (a:Author)  REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT pa_id  IF NOT EXISTS FOR (p:Paper)   REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT ev_id  IF NOT EXISTS FOR (e:Event)   REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT ed_id  IF NOT EXISTS FOR (d:Edition) REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT jo_id  IF NOT EXISTS FOR (j:Journal) REQUIRE j.id IS UNIQUE",
            "CREATE CONSTRAINT vo_id  IF NOT EXISTS FOR (v:Volume)  REQUIRE v.id IS UNIQUE",
            "CREATE CONSTRAINT kw_id  IF NOT EXISTS FOR (k:Keyword) REQUIRE k.id IS UNIQUE",
            "CREATE INDEX kw_name IF NOT EXISTS FOR (k:Keyword) ON (k.name)"
        ]

    @staticmethod
    def _unwind(lbl: str, props: list[str]):
        """
        Generates a Cypher query to unwind a list of rows and merge them into the graph.

        :param lbl: Label for the nodes
        :param props: List of properties to set on the nodes
        """
        setcl = ", ".join([f"n.{p}=row.{p}" for p in props])
        return f"UNWIND $rows AS row MERGE (n:{lbl}{{id:row.id}}) SET {setcl}"

    def _batch(self, q: str, rows: int, n: int = 500):
        """
        Batches the rows into chunks of size n and runs the Cypher query.

        :param q: Cypher query to run
        :param rows: List of rows to pass to the query
        :param n: Size of each batch
        """
        with self.driver.session() as s:
            for i in range(0, len(rows), n):
                s.run(q, rows=rows[i:i+n])

    def load(self, g: DataGenerator):
        with self.driver.session() as s:
            if self.wipe:  
                s.run("MATCH (n) DETACH DELETE n")
            for q in self._schema():  
                s.run(q)

        self._batch(self._unwind("Author", ["name","email"]), g.authors)
        self._batch(self._unwind("Keyword", ["name"]), g.keywords)
        self._batch(self._unwind("Event", ["name","type"]), g.events)
        self._batch(self._unwind("Edition",["year","number","event_id","city","country","start","end"]), g.editions)
        self._batch(self._unwind("Journal",["name","issn"]), g.journals)
        self._batch(self._unwind("Volume", ["number","year","journal_id"]), g.volumes)
        self._batch(self._unwind("Paper", ["title","year","doi","abstract","pages"]), g.papers)

        self._batch("""
            UNWIND $rows AS row
            MATCH (a:Author{id:row[0]}),(p:Paper{id:row[1]})
            MERGE (a)-[:AUTHORED]->(p)
        """, g.authorship)

        self._batch("""
        UNWIND $rows AS row
        MATCH (p:Paper{id:row[0]}),(ed:Edition{id:row[1]})
        MERGE (p)-[:APPEARED_IN]->(ed)
        WITH ed MATCH (ev:Event{id:ed.event_id}) MERGE (ed)-[:OF_EVENT]->(ev)""", g.paper_in_edition)

        self._batch("""
        UNWIND $rows AS row
        MATCH (p:Paper{id:row[0]}),(v:Volume{id:row[1]})
        MERGE (p)-[:APPEARED_IN]->(v)
        WITH v MATCH (j:Journal{id:v.journal_id}) MERGE (v)-[:OF_JOURNAL]->(j)""", g.paper_in_volume)

        self._batch("""
        UNWIND $rows AS row
        MATCH (p:Paper{id:row[0]}),(k:Keyword{name:row[1]})
        MERGE (p)-[:HAS_KEYWORD]->(k)""", g.paper_keywords)

        self._batch("""
        UNWIND $rows AS row
        MATCH (citing:Paper{id:row[0]}),(cited:Paper{id:row[1]})
        MERGE (citing)-[:CITES]->(cited)""", g.citations)

        print("Generated successfully")