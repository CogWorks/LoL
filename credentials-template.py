
from sshtunnel import SSHTunnelForwarder

server = SSHTunnelForwarder(
    ('ssh-address', 22),
    ssh_username="username",
    ssh_password="password",
    remote_bind_address=('127.0.0.1', 3306))
    

def config(port):
 return({ 
   'user': 'sql-database-username',
   'port': '%s' % port,
   'password': 'sql-database-pass',
   'host': 'localhost',
   'database': 'database-name',
   'raise_on_warnings': True
   })
  
  
  
keys = ['api-key1','api-key2','...etc']