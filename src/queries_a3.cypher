MATCH (p:Paper)
WHERE NOT (p)-[:HAS_REVIEW]->()
WITH p LIMIT 1000

MATCH (cand:Author)
WHERE NOT (cand)-[:AUTHORED]->(p)
WITH p, collect(cand) AS cands
WHERE size(cands) >= 3 
WITH p, apoc.coll.randomItems(cands, 3, false) AS reviewers
WHERE size(reviewers) = 3

CALL {
  WITH p, reviewers
  CREATE (rev:Review {id: randomUUID()})
  CREATE (p)-[:HAS_REVIEW]->(rev)

  WITH rev, reviewers
  UNWIND reviewers AS rv
  WITH rev, rv,
       CASE WHEN rand() < 0.5 THEN 'accept' ELSE 'reject' END AS vote
  CREATE (rv)-[:VOTE {decision: vote}]->(rev)

  WITH rev, collect(vote) AS votes
  WITH rev,
       size([v IN votes  WHERE v = 'accept']) AS acc,
       size([v IN votes  WHERE v = 'reject']) AS rej
  SET rev.decision =
        CASE
          WHEN acc > rej THEN 'accept'
          WHEN rej > acc THEN 'reject'
          ELSE 'tie' // just in case there gonna be more than 3 reviewers
        END
  RETURN rev
}

RETURN count(*) AS reviewsCreated;
 count(*) AS papersProcessed;

// organization affiliations
UNWIND range(1,40) AS n
CREATE (:Org {id: randomUUID(),
name: 'Org '+n,
type: CASE WHEN rand()<0.5 THEN 'university' ELSE 'company' END,
country: apoc.coll.randomItem(['ES','DE','FR','US','UK'])});

MATCH (a:Author), (o:Org) WHERE rand()<0.3
WITH a,o,2025-toInteger(rand()*15) AS since
MERGE (a)-[:AFFILIATED_TO {since: since}]->(o);
