# aDB

aDB is a very rudimentary document-bsed database system.  The 
first release is still in development. Currently, it supports 
the following operations:

- Retrieve document by primary key
- Index document collection by integer field
- Index document collection by string field

Running the `server.py` script will populate the database with
some sample data and start a Flask server containing a small
application where data can be retrieved through simple queries.
For example,

`{'pk': 5}`

will query the database to fetch the record with the primary
key equal to 5.  

`{'user_id': 5}`

will fetch all records where the "user_id" field is set to 5.

`{'user_name': 'al'}`

will fetch all records where the "user_name" field begins with
"al".  Any fields to be searched should ideally be indexed 
(except for the primary keys).