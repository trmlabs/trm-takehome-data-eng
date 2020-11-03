import psycopg2

server0 = {'server': 'db0',
           'password': 'password',
           'user': 'user',
           'db': 'TRMLabs',
           'port': '5440'}

server1 = {'server': 'db1',
           'password': 'password',
           'user': 'user',
           'db': 'TRMLabs',
           'port': '5441'}

server2 = {'server': 'db2',
           'password': 'password',
           'user': 'user',
           'db': 'TRMLabs',
           'port': '5442'}

main_server = server0
helper_servers = [server1, server2]


def setup_local_server(server, remote_servers):
    conn = psycopg2.connect(
        host="localhost",
        database=server["db"],
        user=server["user"],
        password=server["password"],
        port=server["port"])
    cur = conn.cursor()

    query = "CREATE EXTENSION postgres_fdw;"
    cur.execute(query)
    conn.commit()

    for rs in remote_servers:
        query = """CREATE SERVER {server} FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host '{server}', dbname '{dbname}');""".format(server=rs["server"], dbname=rs["db"])
        cur.execute(query)
        conn.commit()

        query = """CREATE USER MAPPING FOR {user} SERVER {server}
            OPTIONS (user '{user}', password '{password}' );""".format(server=rs["server"], user=rs["user"], password=rs["password"])
        cur.execute(query)
        conn.commit()

    query = """CREATE TABLE public.daily_transfers
        (
            sender text NOT NULL,
            receiver text NOT NULL,
            total_value double precision NOT NULL,
            date date NOT NULL
        ) PARTITION BY RANGE (date);"""

    cur.execute(query)
    conn.commit()

    cur.close()


def create_remote_partition(server, table):

    query = """CREATE TABLE public.daily_transfers_y{y}m{m}d{d}
    (
        sender text NOT NULL,
        receiver text NOT NULL,
        total_value double precision NOT NULL,
        date date NOT NULL,
        PRIMARY KEY (sender, receiver, date)
    )""".format(y=table['y'], m=table['m'], d=table['d'])

    conn = psycopg2.connect(
        host="localhost",  # change this values accordingly to connect to your postgresql database
        database=server["db"],
        user=server["user"],
        password=server["password"],
        port=server["port"])
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

    query = """CREATE INDEX sender_index_y{y}m{m}d{d}
    ON public.daily_transfers_y{y}m{m}d{d} USING btree
    (sender ASC NULLS LAST, date ASC NULLS LAST);""".format(y=table['y'], m=table['m'], d=table['d'])
    cur.execute(query)
    conn.commit()

    query = """CREATE INDEX receiver_index_y{y}m{m}d{d}
    ON public.daily_transfers_y{y}m{m}d{d} USING btree
    (receiver ASC NULLS LAST, date ASC NULLS LAST);""".format(y=table['y'], m=table['m'], d=table['d'])
    cur.execute(query)
    conn.commit()
    cur.close()


def create_local_partition(server, partition):
    query = """CREATE TABLE public.daily_transfers_y{y}m{m}d{d} 
        PARTITION of public.daily_transfers(
            sender,
            receiver,
            total_value,
            date,
            PRIMARY KEY (sender, receiver, date)
        ) 
        FOR VALUES FROM ('{y}-{m}-{d}T00:00:00Z') TO ('{y2}-{m2}-{d2}T00:00:00Z');""".format(
            y=partition['y'],
            m=partition['m'],
            d=partition['d'],
            y2=partition['y2'],
            m2=partition['m2'],
            d2=partition['d2'])

    conn = psycopg2.connect(
        host="localhost",
        database=server["db"],
        user=server["user"],
        password=server["password"],
        port=server["port"])
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

    query = """CREATE INDEX sender_index_y{y}m{m}d{d}
    ON public.daily_transfers_y{y}m{m}d{d} USING btree
    (sender ASC NULLS LAST, date ASC NULLS LAST);""".format(y=partition['y'], m=partition['m'], d=partition['d'])
    cur.execute(query)
    conn.commit()

    query = """CREATE INDEX receiver_index_y{y}m{m}d{d}
    ON public.daily_transfers_y{y}m{m}d{d} USING btree
    (receiver ASC NULLS LAST, date ASC NULLS LAST);""".format(y=partition['y'], m=partition['m'], d=partition['d'])
    cur.execute(query)
    conn.commit()


def map_remote_partition(server, partition, remote_server):
    query = """CREATE FOREIGN TABLE public.daily_transfers_y{y}m{m}d{d} 
	PARTITION of public.daily_transfers
    FOR VALUES FROM ('{y}-{m}-{d}T00:00:00Z') TO ('{y2}-{m2}-{d2}T00:00:00Z')
    SERVER {server};""".format(
        y=partition['y'],
        m=partition['m'],
        d=partition['d'],
        y2=partition['y2'],
        m2=partition['m2'],
        d2=partition['d2'],
        server=remote_server)

    conn = psycopg2.connect(
        host="localhost",
        database=server["db"],
        user=server["user"],
        password=server["password"],
        port=server["port"])
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()


def set_up_sharding(local_partitions, remote_partitions):
    global main_server, helper_servers
    setup_local_server(main_server, helper_servers)
    for p in local_partitions:
        create_local_partition(main_server, p)

    for p, s in remote_partitions:
        create_remote_partition(helper_servers[s], p)
        map_remote_partition(main_server, p, helper_servers[s]['server'])


def main():
    lp = [{'y': '2020', 'm': '11', 'd': '1', 'y2': '2020', 'm2': '11', 'd2': '2'},
          {'y': '2020', 'm': '10', 'd': '31', 'y2': '2020', 'm2': '11', 'd2': '1'}]

    rp = [({'y': '2020', 'm': '10', 'd': '30', 'y2': '2020', 'm2': '10', 'd2': '31'}, 0),
          ({'y': '2020', 'm': '10', 'd': '29',
            'y2': '2020', 'm2': '10', 'd2': '30'}, 0),
          ({'y': '2020', 'm': '10', 'd': '28',
            'y2': '2020', 'm2': '10', 'd2': '29'}, 1),
          ({'y': '2020', 'm': '10', 'd': '27',  'y2': '2020', 'm2': '10', 'd2': '28'}, 1)]
    set_up_sharding(lp, rp)


if __name__ == "__main__":
    main()
