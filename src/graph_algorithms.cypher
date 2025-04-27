// create a projection of the graph in memory - this projection includes all 'Paper' nodes 
// and all the 'Cites' relationships. 

CALL gds.pageRank.write('publications-pagerank', {
    maxIterations: 20,
    dampingFactor: 0.85,
    writeProperty: 'pageRankScore'  
})
YIELD nodePropertiesWritten;


// implement the PageRank algorithm on the projected graph

CALL gds.pageRank.stream('publications-pagerank', {
    maxIterations: 20,    // number of iterations to do before stopping 
    dampingFactor: 0.85   // this is a standard value for the dumping factor. This means that 85% 
                          // of a paper's importance comes from citations, while 15% is evenly distributed
})
YIELD nodeId, score
MATCH (paper:Paper) WHERE id(paper) = nodeId
RETURN paper.title AS PaperTitle, score AS InfluenceScore
ORDER BY InfluenceScore DESC
LIMIT 100;


// simple comparison between PageRank score and raw number of citations

MATCH (p:Paper)<-[c:CITES]-()
WITH p, count(c) AS citationCount
WHERE p.pageRankScore IS NOT NULL
RETURN p.title AS PaperTitle, 
       p.pageRankScore AS PageRankScore, 
       citationCount AS CitationCount,
       p.pageRankScore / citationCount AS Ratio
ORDER BY PageRankScore DESC
LIMIT 100;


// remove the projected graph that was created before 

CALL gds.graph.drop('publications-pagerank', false);