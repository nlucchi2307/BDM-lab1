MATCH (p:Paper)-[:HAS_REVIEW]->(rev:Review)
WITH p,
     collect(rev) AS reviews,
     sum(CASE rev.decision WHEN 'accept' THEN 1 ELSE 0 END) AS acc,
     sum(CASE rev.decision WHEN 'reject' THEN 1 ELSE 0 END) AS rej

// majority rule
SET p.accepted =
  CASE
    WHEN acc > rej THEN true
    WHEN rej > acc THEN false
    ELSE null 
  END,
    p.reviewCount = size(reviews), 
    p.acceptVotes = acc,
    p.rejectVotes = rej

RETURN p.title AS Paper, p.reviewCount AS Reviews, p.acceptVotes AS Accept, p.rejectVotes AS Reject, p.accepted AS Accepted;

// organization affiliations
UNWIND range(1,40) AS n
CREATE (:Org {id: randomUUID(),
name: 'Org '+n,
type: CASE WHEN rand()<0.5 THEN 'university' ELSE 'company' END,
country: apoc.coll.randomItem(['ES','DE','FR','US','UK'])});

MATCH (a:Author), (o:Org) WHERE rand()<0.3
WITH a,o,2025-toInteger(rand()*15) AS since
MERGE (a)-[:AFFILIATED_TO {since: since}]->(o);
