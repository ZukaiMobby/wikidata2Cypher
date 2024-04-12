## What is it
This is a simple wikidata json to neo4j Cypher converter.
Born for school work of "YNU 智能数据工程 2023 春"
It is multiprocessed, and can resume the convertion where it is breaked.
It is not out of box implementation, since only part of the original field will be import into neo4j, change if necessary

## How to use
- Change the config.py, it is very straightforward. 
- Run the main.py
- After the process finished, only nodes have imported
- run the follow in neo4j browser to add edges
```
MATCH (source:Entity)-[]->(related)
WHERE (related:Claim) and  related.datavalue_type = "wikibase-entityid"
match (dst: Entity {id:related.datavalue_value})
merge (related) -[:CONN]->(dst)
```
