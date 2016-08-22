
from sshtunnel import SSHTunnelForwarder

##Make sure to make a copy of this file, name it credentials.py, place it in this subfolder 
##   ('lol', it should be in the same folder as LoL.py and utils.py)  and change the appropriate things

##if you are locally running the scripts, use ssh=False

##anything with a double asterisk in front of the variable name is something you should change, 
##obviously remove the asterisks as well and keep things inside quotes, inside the quotes
##you only need to fill out the information for the method you plan to use (ssh tunneling or just regular database connection)
##for us at RPI, make sure your VPN is open before you try either method. 
def config(ssh=False, port=3306):
 if ssh==True:
  global server
  server = SSHTunnelForwarder(
      ('**ssh-address', 22),
      ssh_username="**username",
      ssh_password="**password",
      remote_bind_address=('127.0.0.1', 3306))  
  server.start()
  port = server.local_bind_port
  return({ 
    'user': '**sql-database-username',
    'port': '%s' % port,   
    'password': '**sql-database-pass',
    'host': 'localhost',  
    'database': '**database-name',
    'raise_on_warnings': True   
    })
 else:
  ##in order to use this, you must change mysql/my.cnf bind-address from 127.0.0.1 to 0.0.0.0 
  ##which is not necessarily the most secure thing. changes to iptables may also be required. 
  return({
     'user': '**sql-database-username',
     'password': '**sql-database-pass',
     ##if you're locally accessing the server, this should just be 'localhost'
     'host': '**ip-or-web-address',
     'database': '**database-name',
     'raise_on_warnings': True
     })
 
##please try to use at least 2 codes as the scripts function better that way.   
keys = ['**api-key1','**api-key2','**...etc']
