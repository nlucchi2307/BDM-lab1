// top 3 most cited papers 
// для каждой Conference-серии найдём три наиболее цитируемые статьи
MATCH (conf:Conference)

CALL { 
  WITH conf
  MATCH (conf)<-[:OF_EVENT]-(:Edition)<-[:APPEARED_IN]-(p:Paper)
  OPTIONAL MATCH (p)<-[c:CITES]-()
  WITH p, count(c) AS cites
  ORDER BY cites DESC
  RETURN collect({title: p.title, citations: cites})[0..3] AS top3
}

RETURN conf.name AS conference, top3;


MATCH (conf:Conference)

CALL {
  WITH conf
  MATCH (conf)<-[:OF_EVENT]-(ed:Edition)
        <-[:APPEARED_IN]-(p:Paper)
        <-[:AUTHORED]-(a:Author)
  WITH a, count(DISTINCT ed) AS editions 
  WHERE editions >= 4
  RETURN collect({id: a.id, name: a.name, editions: editions}) AS community
}

RETURN conf.name AS conference, community;


// impact factor
WITH 2024 AS yr 
MATCH (j:Journal)<-[:OF_JOURNAL]-(:Volume)<-[:APPEARED_IN]-(p:Paper)
WHERE p.year IN [yr-1, yr-2]

OPTIONAL MATCH (p)<-[:CITES]-(citing:Paper)
WHERE citing.year = yr

WITH j, yr, count(DISTINCT citing) AS cites, count(DISTINCT p) AS pubs

RETURN j.name AS journal, round(toFloat(cites)/pubs, 3) AS impactFactor, pubs, cites
ORDER BY impactFactor DESC;


// h - index
MATCH (a:Author)-[:AUTHORED]->(p:Paper)
OPTIONAL MATCH (p)<-[:CITES]-(:Paper)
WITH a, p, count(*) AS c
ORDER BY a.id, c DESC
WITH a, collect(c) AS cs
WITH a, reduce(h=0, i IN range(0,size(cs)-1) | CASE WHEN cs[i] >= i+1 THEN i+1 ELSE h END) AS h_index
RETURN a.name AS author, h_index
ORDER BY h_index DESC
LIMIT 50;