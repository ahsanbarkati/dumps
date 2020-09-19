import datetime
import json

import pydgraph

# Create a client stub.
def create_client_stub():
    return pydgraph.DgraphClientStub('localhost:9080')


# Create a client.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


# Drop All - discard all data and start from a clean slate.
def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))

def set_schema(client):
    schema = """
    <EMPLOYED_BY>: [uid] @reverse .
	<Name>: string @index(exact) .
	<dgraph.graphql.schema>: string .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def create_data(client, data):
    # Create a new transaction.
    txn = client.txn()
    try:
        response = txn.mutate(set_nquads=data)
        # Commit transaction.
        txn.commit()

    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()

def get_uids(client):
	q = """{
	q1(func: eq(Name, "alice")){
	uid
	}
	}
  	"""
	res = client.txn(read_only=True).query(q)
	alice = json.loads(res.json)['q1'][0]['uid']

	q = """{
	q1(func: eq(Name, "bob")){
	uid
	}
	}
  	"""
	res = client.txn(read_only=True).query(q)
	bob = json.loads(res.json)['q1'][0]['uid']
	return alice, bob

def query(client, alice, bob):
    # Run query.
    q = """ {
        q0 as shortest(from:""" +  alice + ", to: " + bob + """, numpaths:2, depth:4, minweight:1){
            EMPLOYED_BY ~EMPLOYED_BY
        }
        path(func: uid(q0)){
            uid
            dgraph.type
            name
        } 
    }"""

    res = client.txn(read_only=True).query(q)
    ppl = json.loads(res.json)
    return ppl

data =  """
     _:alice <Name> "alice" .
    _:bob <Name> "bob" .
    _:charles <Name> "charles" .
    _:dan <Name> "dan" .
    _:ed <Name> "ed" .
    _:fran <Name> "fran" .
    _:greg <Name> "greg" .
    _:helen <Name> "helen" .
    _:irene <Name> "irene" .
    _:julia <Name> "julia" .
    _:karen <Name> "karen" .
    _:louis <Name> "louis" .

    _:orgA <Name> "orgA" .
    _:orgB <Name> "orgB" .
    _:orgC <Name> "orgC" .
    _:orgD <Name> "orgD" .

    _:alice <EMPLOYED_BY> _:orgA .
    _:bob <EMPLOYED_BY> _:orgA .
    _:fran <EMPLOYED_BY> _:orgA .
    _:greg <EMPLOYED_BY> _:orgA .


    _:charles <EMPLOYED_BY> _:orgB .
    _:dan <EMPLOYED_BY> _:orgB .
    _:ed <EMPLOYED_BY> _:orgB .
    _:fran <EMPLOYED_BY> _:orgB .
    _:greg <EMPLOYED_BY> _:orgB .
    _:helen <EMPLOYED_BY> _:orgB .
    _:irene <EMPLOYED_BY> _:orgB  .
    _:karen <EMPLOYED_BY> _:orgB .
    _:louis <EMPLOYED_BY> _:orgB .

    _:charles <EMPLOYED_BY> _:orgC .
    _:dan <EMPLOYED_BY> _:orgC .
    _:ed <EMPLOYED_BY> _:orgC .
    _:helen <EMPLOYED_BY> _:orgC .
    _:irene <EMPLOYED_BY> _:orgC  .

    _:bob <EMPLOYED_BY> _:orgD .
    _:louis <EMPLOYED_BY> _:orgD .
    _:helen <EMPLOYED_BY> _:orgD .
    _:irene <EMPLOYED_BY> _:orgD  .
    _:karen <EMPLOYED_BY> _:orgD .
    
"""


def main():
    client_stub = create_client_stub()
    client = create_client(client_stub)
    drop_all(client)
    set_schema(client)
    create_data(client, data)
    alice, bob = get_uids(client)

    last = query(client, alice, bob)
    i = 0
    q = """{
        q(func: has(Name)){
            uid
            Name
        }
        }
    """
    while(1):
        resp = query(client, alice, bob)
        i += 1
        f = open('/home/ash/log.txt', 'a+')
        f.write("######################## iteration: " + str(i) + "\n\n")
        f.close()
        if(i %1000 == 0):
        	print("Queries done:", i)
        if( resp['_path_'][0] != last['_path_'][0] ):
            # res = client.txn(read_only=True).query(q)
            # print(json.loads(res.json))
            # print(json.dumps(resp, indent=2) , "\n\n", json.dumps(last, indent=2), "\n")
            print("This appeared after %d correct responses\n"%(i))
            print("Shortest path queried: From: %s To: %s\n\n"%(alice, bob))
            print("The shortest path is:\n", json.dumps(resp["path"], indent=2), "\n")
            
            break
        last = resp

        
    # Close the client stub.
    client_stub.close()


if __name__ == '__main__':
    main()
