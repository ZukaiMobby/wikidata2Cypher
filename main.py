import json
import multiprocessing
import time
from config import *
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, Neo4jError


def init():
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        try:
            driver.verify_connectivity()
            print("DB conn suc..")
        except ServiceUnavailable:
            print("DB conn failed")
            exit(-1)

        if CLEAR:
            _ = input("Clear DataBase? Enter to Continue")
            driver.execute_query("MATCH (n) DETACH DELETE(n)")

def Controler_process(queue,err_msg_queue,error_count: multiprocessing.Value):
    resource = open(FILENAME, 'r')
    next(resource)
    currentLine = 2
    if (RESUME):
        with open(STATUS_FILE,'r') as f:
            target_line = int(f.read())
            print(f"Seeking json file,target: {target_line}")
            for _ in range(target_line-currentLine):
                currentLine += 1
                next(resource)
    stat = open(STATUS_FILE,'w')

    try:
        while True:
            while(queue.qsize()<QUEUE_LEN):
                if(MAXLINE and currentLine > MAXLINE): raise StopIteration
                line = next(resource)
                currentLine += 1
                queue.put(line.strip())
            with error_count.get_lock():
                print(f"Current Line {currentLine-1}/{str(str(currentLine*100/16408790))[:6]+'%'}, Errors {error_count.value}", end='\r')
            stat.seek(0)
            stat.truncate()
            stat.write(str(currentLine-QUEUE_LEN))
            stat.flush()
            with open(ERROR_FILE,'a') as ef:
                while(err_msg_queue.qsize()!=0):
                    ef.write(err_msg_queue.get())

            while(queue.qsize()>QUEUE_LEN_MIN):
                time.sleep(1)

    except StopIteration:
        print(f"Current Line {currentLine-1}/{str(str(currentLine*100/16408790))[:6]+'%'}, Errors {error_count.value}", end='\r')
        for _ in range(NUM_WORKERS):
            queue.put(None)
        resource.close()
    
def Exec_process(queue,err_msg_queue,error_count):

    def create_Q_C(line:str):
        line = line.rstrip(',')
        item = json.loads(line)
        node_dict = {}
        if item['type'] != "item":  
            return ""

        node_dict['id'] = item['id']

        if 'labels' in item:

            if 'en' in item['labels']: node_dict['label'] = item['labels']['en']['value']
            if 'zh-cn' in item['labels']: node_dict['label_zh'] = item['labels']['zh-cn']['value']

        if 'description' in item:

            if 'en' in item['description']: node_dict['description'] = item['descriptions']['en']['value']
            if 'zh-cn' in item['descriptions']: node_dict['description_zh'] = item['descriptions']['zh-cn']['value']


        properties_string = ""
        for key, value in node_dict.items():
            value = value.replace('"', '\\"')
            properties_string += f'{key}: "{value}"' + ','
        properties_string = properties_string[:-1]
        properties_string = "{" + properties_string + "}"
        query_string = f"""
        CREATE (node:Entity {properties_string})
        """

        if 'claims' not in item:
            return query_string

        claims_dict = item['claims']
        claims_list = []

        for _, value in claims_dict.items():
            for claim in value: #value:list  item:dict
                claim_dict = {}

                if "type" not in claim: continue
                if claim["type"] != "statement": continue
                if "mainsnak" not in claim: continue
                claim = claim["mainsnak"]
                if "property" not in claim: continue
                if "datatype" not in claim: continue
                if claim["snaktype"] != "value": continue

                claim_dict["property"] = claim["property"]
                claim_dict["datatype"] = claim["datatype"]
                
                if claim_dict["datatype"] == "wikibase-item" and claim["datavalue"]["value"]["entity-type"]=="item" :
                    nid = claim["datavalue"]["value"]["numeric-id"]
                    claim_dict["datavalue_value"] = 'Q'+str(nid)
                    claim_dict["datavalue_type"] = claim["datavalue"]["type"]

                else:
                    claim_dict["datavalue_value"] = claim["datavalue"]["value"]
                    claim_dict["datavalue_type"] = claim["datavalue"]["type"]
                claims_list.append(claim_dict)

        for item in claims_list:
            properties_string = ""
            for key, value in item.items():
                value = str(value).replace('"', '\\"')
                properties_string += f'{key}: "{value}"' + ','
            properties_string = properties_string[:-1]
            properties_string = "{" + properties_string + "}"
            claim_query_string = f"""
            CREATE (node)-[:{item['property']}]->(:Claim {properties_string})
            """
            query_string += claim_query_string

        return query_string

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:
            while True:
                line = queue.get()
                if line == None: break
                query = create_Q_C(line)
                if not query: continue
                try:
                    session.run(query)
                except Neo4jError as e:
                    print(e)
                    with error_count.get_lock():
                        error_count.value+=1
                    err_msg_queue.put(query)


if __name__ == '__main__':

    init()

    error_count = multiprocessing.Value('i', 0)

    print("Stage 1/2: Create Q and it's Claims")
    queue = multiprocessing.Queue()
    err_msg_queue = multiprocessing.Queue()
    control_p = multiprocessing.Process(target=Controler_process, args=(queue,err_msg_queue,error_count))
    control_p.start()

    exec_processes = []
    for i in range(NUM_WORKERS):
        exec_p = multiprocessing.Process(target=Exec_process, args=(queue,err_msg_queue,error_count))
        exec_processes.append(exec_p)
        exec_p.start()

    control_p.join()
    for exec_p in exec_processes:
        exec_p.join()
    

    print("\n\n")
    print("Stage 1 Finshed")




