import psycopg2
from flask import Flask, request
from google.cloud import bigquery
from google_auth_oauthlib import flow

conn = None
client = None
app = Flask(__name__)


@app.route('/authenticate',  methods=['GET'])
def authenticate():
    """This function needs to be called to sign in to the google cloud and
    be able to access the bigquery. It also logs into the postgresql database
    """
    global client, conn
    conn = psycopg2.connect(
        host="localhost",
        database="TRMLabs",
        user="user",
        password="password",
        port="5440")

    launch_browser = True
    appflow = flow.InstalledAppFlow.from_client_secrets_file(
        "client_secrets.json",
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )

    if launch_browser:
        appflow.run_local_server()
    else:
        appflow.run_console()

    credentials = appflow.credentials
    project = 'trm-takehome-greg-d'
    client = bigquery.Client(project=project, credentials=credentials)

    return {"data": "authentication was successful"}


@app.route('/fetch_bigquery_data',  methods=['GET'])
def fetch():
    """
    This function is used to fetch data from the bigquery and save it
    to PostgreSQL database so that it can be used for answering queries.

    This function will need to be called daily to retrieve new updates to
    the blockchain.
    """

    start_date = request.args.get('startdate', '2020-10-27T00:00:00Z')
    end_date = request.args.get('enddate', '2020-11-02T00:00:00Z')

    query_string = """
  SELECT
    input_address AS sender,
    output_address AS receiver,
    SUM(SAFE_DIVIDE(LEAST(input_value, output_value), POW(10, 8))) AS total_value,
    CAST(DATE(block_timestamp) AS DATE) as day
  FROM
    `bigquery-public-data.crypto_bitcoin.transactions`,
    UNNEST(inputs) AS input,
    UNNEST(input.addresses) AS input_address,
    UNNEST(outputs) AS output,
    UNNEST(output.addresses) AS output_address
  WHERE
    block_timestamp >= @start_date AND
    block_timestamp <= @end_date
  GROUP BY
    sender,
    receiver,
    day
  ORDER BY
    total_value desc
  LIMIT 10000
  """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )

    query_job = client.query(query_string, job_config=job_config)

    # Print the results.
    out = []
    for row in query_job.result():  # Wait for the job to complete.
        out.append((row["sender"],
                    row["receiver"],
                    row["total_value"],
                    row["day"]))

    query = "INSERT INTO daily_transfers (sender, receiver, total_value, date) VALUES (%s, %s, %s, %s)"
    cur = conn.cursor()
    cur.executemany(query, out)
    conn.commit()
    cur.close()

    return {"data": 'fetch', "success": True}


@app.route('/address/exposure/direct',  methods=['GET'])
def address_exposure_direct():
    global conn
    address = request.args.get('address', None)
    start_date = request.args.get('startdate', '0001-01-01T00:00:00Z')
    end_date = request.args.get('enddate', '9999-12-31T23:59:59Z')
    flow_type = request.args.get('flowtype', 'both')
    limit = request.args.get('limit', 100)
    offset = request.args.get('offset', 0)

    if address is None:
        return {"data": "Error: missing address", "success": False}

    output_ordering = "totalflow"
    if flow_type == "inflow":
        output_ordering = "inflow"
    elif flow_type == "outflow":
        output_ordering = "outflow"

    query_string = f"""
  WITH outflow_data AS (
  SELECT
    sender,
    receiver,
    sum(total_value) as total_value
  FROM
    daily_transfers
  WHERE
    (sender = '{address}' OR receiver = '{address}') AND
    date >= '{start_date}' AND
    date <= '{end_date}'
  GROUP BY
    sender,
    receiver
  ORDER BY
    total_value desc
  ),

  outflow_table AS (
  SELECT
    od.sender as address_of_interest,
    od.receiver as counterpart,
    od.total_value as outflow,
    0 as inflow,
    od.total_value as total_flow
  FROM
    outflow_data as od
  WHERE od.sender= '{address}'
  ),
  inflow_table AS (
  SELECT
    od.receiver as address_of_interest,
    od.sender as counterpart,
    od.total_value as inflow,
    0 as outflow,
    od.total_value as total_flow
  FROM
    outflow_data as od
  WHERE od.receiver= '{address}'
  )

  SELECT
    address_of_interest,
    counterpart,
    sum(d.outflow) as outflow,
    sum(d.inflow) as inflow,
    sum(d.total_flow) as totalflow
  FROM (
    SELECT
      address_of_interest,
      counterpart,
      outflow,
      inflow,
      total_flow
    FROM inflow_table
      UNION ALL
    SELECT
      address_of_interest,
      counterpart,
      outflow,
      inflow,
      total_flow FROM outflow_table
    ) as d
  GROUP BY
    address_of_interest,
    counterpart
  order by
    {output_ordering} desc
  LIMIT {limit}
  OFFSET {offset}
  """

    cur = conn.cursor()
    cur.execute(query_string)
    out = []
    for _, c, o, i, t in cur:
        out.append({"address": c,
                    "inflows": i,
                    "outflows": o,
                    "total_flows": t})

    return {"data": out, "success": True}
