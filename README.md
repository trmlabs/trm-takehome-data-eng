## Overview

This is a prototype that is using database sharding.
In this setup we have 3 seprate database servers running.
There is one master database to which the web server communicates.
The other two database servers are hidden away from the web server.

The data is partitioned over the three database servers.
We have partitioned the data by day and each server holds partitions for two seprate days.
All together we are storing data for six days.
With this setup, when the master server recives a query that spans 6 days, it will forward the query to the
remote servers that contain the relevant days and then combine the results together.
Afterwards, the result is returned to the web server. 

To run this prototype you will need to have Docker and Docker-compose installed on your computer.


## Getting Started

**1. Create your Conda Environment**
`conda create -n <env_name> python=3.8`

**2. Install the required dependencies**
`conda install -n <env_name> requirements.txt`

**3. Start the postgres database cluster**
`docker-compose up`

**4. Setup the data partitions on the cluster**
`python initialize_db.py`

**5. Run the local flask Development Server**
```
export FLASK_APP=app.py
export FLASK_ENV=development
flask run
```

**6. Authenticate the web server with the TRM Labs account**
Enter `http://127.0.0.1:5000/authenticate` into your browser.
You will be able to select account with which you want to authenticate.

**7. Extract data from Big Query and load it into postgresql cluster**
Enter `http://127.0.0.1:5000/fetch_bigquery_data` into your browser.


**8. Hit the endpoint from your local browser**
Enter `http://127.0.0.1:5000/address/exposure/direct?address=1BQAPyku1ZibWGAgd8QePpW1vAKHowqLez` into your browser!

You should see the following response:
```
{
  "data": [
    { "address": "1FGhgLbMzrUV5mgwX9nkEeqHbKbUK29nbQ", "inflows": "0", "outflows": "0.01733177", "total_flows": "0.01733177" },
    { "address": "1Huro4zmi1kD1Ln4krTgJiXMYrAkEd4YSh", "inflows": "0.01733177", "outflows": "0", "total_flows": "0.01733177" },
  ],
  "success": True
}
```

**9. Shut down the postgresql cluster**
`docker-compose down`
