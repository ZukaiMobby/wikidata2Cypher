URI = "neo4j://localhost:7687"
FILENAME = "E:\\aaa\\wikidata-20170220-all.json"
ERROR_FILE = "Err.cypher"
STATUS_FILE = "stat.txt"
MAXLINE = None
AUTH = ("neo4j", "12345678")
CLEAR = True  # if resume true and clear should be false
RESUME = False
ERR_TOL = 100

##### muti process control #####
NUM_WORKERS = 6 # processes to exec query
QUEUE_LEN = 600
QUEUE_LEN_MIN = 60