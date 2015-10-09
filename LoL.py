
import ssl
import riotwatcher
import urllib2
import json

from utils import todict
import mysql.connector
from mysql.connector import errorcode


from time import sleep

import credentials

# using SSH Tunnel because to connect directly to MySQL on server we need to comment out 
# 'skip-networking' in /etc/mysql/my.cnf which allows non-local connections and is generally less secure
# 
# server = credentials.server
# server.start()



try:
 cnx = mysql.connector.connect(**credentials.config())
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)
else:
  print "Connected to %s database" % credentials.config()['database']

cursor = cnx.cursor()

keys = credentials.keys

key = keys[1]


        
def  get_master(queue="RANKED_TEAM_5x5"):
  url = "https://na.api.pvp.net/api/lol/na/v2.5/league/master?type=%s&api_key=%s" % (queue, key)
  print "Calling: %s" % url
  return json.loads(urllib2.urlopen(url).read())





def create_tables():
 
 DB_NAME = 'lol'

 TABLES = {}
#  TABLES['challenger'] = (
#     "CREATE TABLE `challenger` ("
#     "  `isFreshBlood` bool NOT NULL,"
#     "  `division` varchar(2) NOT NULL,"
#     "  `isVeteran` bool NOT NULL,"
#     "  `wins` int(8) NOT NULL,"
#     "  `losses` int(8) NOT NULL,"
#     "  `playerOrTeamId` varchar(50) NOT NULL,"
#     "  `playerOrTeamName` varchar(25) NOT NULL,"
#     "  `isInactive` bool NOT NULL,"
#     "  `isHotStreak` bool NOT NULL,"
#     "  `leaguePoints` int(8) NOT NULL,"
#     "  PRIMARY KEY (`playerOrTeamId`)"
#     ") CHARACTER SET utf8 ENGINE=InnoDB")
#  
#  TABLES['master'] = (
#     "CREATE TABLE `master` ("
#     "  `isFreshBlood` bool NOT NULL,"
#     "  `division` varchar(2) NOT NULL,"
#     "  `isVeteran` bool NOT NULL,"
#     "  `wins` int(8) NOT NULL,"
#     "  `losses` int(8) NOT NULL,"
#     "  `playerOrTeamId` varchar(50) NOT NULL,"
#     "  `playerOrTeamName` varchar(25) NOT NULL,"
#     "  `isInactive` bool NOT NULL,"
#     "  `isHotStreak` bool NOT NULL,"
#     "  `leaguePoints` int(8) NOT NULL,"
#     "  PRIMARY KEY (`playerOrTeamId`)"
#     ") CHARACTER SET utf8 ENGINE=InnoDB") 
    
 TABLES['by_league'] = (
    "CREATE TABLE `by_league` ("
    "  `isFreshBlood` bool NOT NULL,"
    "  `division` varchar(5) NOT NULL,"
    "  `isVeteran` bool NOT NULL,"
    "  `wins` int(8) NOT NULL,"
    "  `losses` int(8) NOT NULL,"
    "  `playerOrTeamId` varchar(50) NOT NULL,"
    "  `playerOrTeamName` varchar(25) NOT NULL,"
    "  `isInactive` bool NOT NULL,"
    "  `isHotStreak` bool NOT NULL,"
    "  `leaguePoints` int(8) NOT NULL,"
    "  `league` varchar(25) NOT NULL,"
    "  `team` bool NOT NULL,"
    "  `queue` varchar(25) NOT NULL,"
    "  CONSTRAINT id_queue PRIMARY KEY (`playerOrTeamId`, `queue`)"
    ") CHARACTER SET utf8 ENGINE=InnoDB") 
    
    
    
 TABLES['team'] = (
    "CREATE TABLE `team` ("
    "  `createDate` BIGINT NOT NULL,"
    "  `fullId` varchar(50) NOT NULL,"
    "  `lastGameDate` BIGINT NOT NULL,"
    "  `lastJoinDate` BIGINT NOT NULL,"
    "  `lastJoinedRankedTeamQueueDate` BIGINT NOT NULL,"
    "  `modifyDate` BIGINT NOT NULL,"
    "  `name` varchar(25) NOT NULL,"
    "  `secondLastJoinDate` BIGINT NOT NULL,"
    "  `status` varchar(25) NOT NULL,"
    "  `tag` varchar(25) NOT NULL,"
    "  `thirdLastJoinDate` BIGINT NOT NULL,"
    "  `averageGamesPlayed3v3` int(6) NOT NULL,"
    "  `losses3v3` int(6) NOT NULL,"
    "  `wins3v3` int(6) NOT NULL,"
    "  `averageGamesPlayed5v5` int(6) NOT NULL,"
    "  `losses5v5` int(6) NOT NULL,"
    "  `wins5v5` int(6) NOT NULL,"
    "  PRIMARY KEY (`fullId`)"
    ") CHARACTER SET utf8 ENGINE=InnoDB") 
 

 TABLES['team_history'] = (
     "CREATE TABLE `team_history` ("
     "  `fullId` varchar(50) NOT NULL,"
     "  `assists` int(6) NOT NULL,"
     "  `date` BIGINT NOT NULL,"
     "  `deaths` int(6) NOT NULL,"
     "  `gameId` int(18) NOT NULL,"
     "  `gameMode` varchar(25) NOT NULL,"
     "  `invalid` bool NOT NULL,"
     "  `kills` int(6) NOT NULL,"
     "  `mapId` int(4) NOT NULL,"
     "  `opposingTeamKills` int(6) NOT NULL,"
     "  `opposingTeamName` varchar(25) NOT NULL,"
     "  `win` bool NOT NULL,"
     "  CONSTRAINT game_team PRIMARY KEY (`gameId`, `fullId`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB") 
    
 TABLES['team_roster'] = (
     "CREATE TABLE `team_roster` ("
     "  `inviteDate` BIGINT NOT NULL,"
     "  `joinDate` BIGINT NOT NULL,"
     "  `playerId` int(18) NOT NULL,"
     "  `status` varchar(25) NOT NULL,"
     "  `isCaptain` bool NOT NULL,"
     "  `teamId` varchar(50) NOT NULL,"
     "  CONSTRAINT player_team PRIMARY KEY (`playerId`, `teamId`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB")
      
      
 def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

 try:
     cnx.database = DB_NAME    
 except mysql.connector.Error as err:
     if err.errno == errorcode.ER_BAD_DB_ERROR:
         create_database(cursor)
         cnx.database = DB_NAME
     else:
         print(err)
         exit(1)   
 for name, ddl in TABLES.iteritems():
     try:
         print("Creating table {}: ".format(name))
         cursor.execute(ddl)
     except mysql.connector.Error as err:
         if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
             print("already exists.")
         else:
             print(err.msg)
     else:
         print("OK")


 
 




w = riotwatcher.RiotWatcher(key)



def update_table(table, queue="RANKED_TEAM_5x5", create=False):
 if create == True:
  create_tables()

 if table=="challenger":
  add_challenger = ("INSERT INTO by_league "
               "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
               "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")

  challenger = w.get_challenger(queue=queue)
 
  for x in todict(challenger)['entries']:
   x['league'] = "challenger"
   x['team'] = (True if queue!="RANKED_SOLO_5x5" else False)
   x['queue'] = queue
   try:
    cursor.execute(add_challenger, x)
   except mysql.connector.Error as err:
    print(err.msg)
   else:
    print "Updated Challenger"
  
  
 if table=="master":
  add_master = ("INSERT INTO by_league "
               "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
               "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")

#   master = w.get_master(queue=queue)
  ## TEMPORARILY CREATED MY OWN "Get_Master" Function
  master = get_master(queue=queue)
  for x in todict(master)['entries']:
   x['league'] = "master"
   x['team'] = (True if queue!="RANKED_SOLO_5x5" else False)
   x['queue'] = queue
   try:
    cursor.execute(add_master, x)
   except mysql.connector.Error as err:
    print(err.msg)
   else:
    print "Updated Master"
    
    
    
 if table=="checkteams":
   add_league = ("INSERT INTO by_league "
               "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
               "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")

   cursor.execute("SELECT fullId FROM team" )  
  
   team_ids_raw = []   
   team_ids_raw = cursor.fetchall()
   team_ids = [] 
  
   for x in team_ids_raw:
    for y in x:
     team_ids.append(y)
   
   for x in xrange(0,int(round(len(team_ids), -1)/10)):
    stop = ((x+1)*10)
    if x == int(round(len(team_ids), -1)/10) - 1:
     stop = (len(team_ids))

  
    league_entries = w.get_league_entry(team_ids=team_ids[(x*10):stop])
    

    for z in league_entries:
     for y in league_entries[z]:
      for v in y['entries']:
       v['league'] = y['tier']
       v['team'] = (True if y['queue']!="RANKED_SOLO_5x5" else False)
       v['queue'] = y['queue']
       try:
 #        print x['playerOrTeamId']
        cursor.execute(add_league, v)
       except mysql.connector.Error as err:
        print(err.msg)
       else:
        print "Updated By-League"    

    print "Finished %s of %s" % (stop, len(team_ids))
        
        
        
 if table=="team":
   
   add_team = ("INSERT INTO team "
               "(createDate, fullId, lastGameDate, lastJoinDate, lastJoinedRankedTeamQueueDate, modifyDate, name, secondLastJoinDate, status, tag, thirdLastJoinDate, averageGamesPlayed3v3, losses3v3, wins3v3, averageGamesPlayed5v5, losses5v5, wins5v5) " 
               "VALUES (%(createDate)s, %(fullId)s, %(lastGameDate)s, %(lastJoinDate)s, %(lastJoinedRankedTeamQueueDate)s, %(modifyDate)s, %(name)s, %(secondLastJoinDate)s, %(status)s, %(tag)s, %(thirdLastJoinDate)s, %(averageGamesPlayed3v3)s, %(losses3v3)s, %(wins3v3)s, %(averageGamesPlayed5v5)s, %(losses5v5)s, %(wins5v5)s) " )
 
   add_team_history = ("INSERT INTO team_history "
            "(fullId, assists, date, deaths, gameId, gameMode, invalid, kills, mapId, opposingTeamKills, opposingTeamName, win)" 
            "VALUES (%(fullId)s, %(assists)s, %(date)s, %(deaths)s, %(gameId)s, %(gameMode)s, %(invalid)s, %(kills)s, %(mapId)s, %(opposingTeamKills)s, %(opposingTeamName)s, %(win)s)" )
   
   add_team_roster = ("INSERT INTO team_roster "
            "(inviteDate, joinDate, playerId, status, isCaptain, teamId)"
            "VALUES (%(inviteDate)s, %(joinDate)s, %(playerId)s, %(status)s, %(isCaptain)s, %(teamId)s)")
   
   cursor.execute("SELECT playerOrTeamId FROM by_league WHERE team = True" )         
   

   
   team_ids_raw = cursor.fetchall()
   
   team_ids = [] 
   
   for x in team_ids_raw:
    for y in x:
     team_ids.append(y)

    
   teams_data = []
   
   for x in xrange(0,int(round(len(team_ids), -1)/10)):
    stop = ((x+1)*10)
    if x == int(round(len(team_ids), -1)/10) - 1:
     stop = (len(team_ids))

    teams_data = w.get_teams(team_ids[(x*10):stop])
    
    for y in team_ids[(x*10):stop]:
     cur_team_full = todict(teams_data).get(y)
     cur_team = {}
     for n in ['createDate', 'fullId', 'lastGameDate', 'lastJoinDate', 'lastJoinedRankedTeamQueueDate', 'modifyDate', 'name', 'secondLastJoinDate', 'status', 'tag', 'thirdLastJoinDate']:
      cur_team[n] = cur_team_full[n]     
#      averageGamesPlayed3v3, losses3v3, wins3v3, averageGamesPlayed5v5, losses5v5, wins5v5
     team_stats =  cur_team_full['teamStatDetails']
     
     if team_stats[0]['teamStatType'] == "RANKED_TEAM_3x3":
      cur_team['averageGamesPlayed3v3'] = team_stats[0]['averageGamesPlayed']
      cur_team['losses3v3'] = team_stats[0]['losses']
      cur_team['wins3v3'] = team_stats[0]['wins']
      cur_team['averageGamesPlayed5v5'] = team_stats[1]['averageGamesPlayed']      
      cur_team['losses5v5'] = team_stats[1]['losses']
      cur_team['wins5v5'] = team_stats[1]['wins']
     elif team_stats[0]['teamStatType'] == "RANKED_TEAM_5x5":
      cur_team['averageGamesPlayed3v3'] = team_stats[1]['averageGamesPlayed']
      cur_team['losses3v3'] = team_stats[1]['losses']
      cur_team['wins3v3'] = team_stats[1]['wins']
      cur_team['averageGamesPlayed5v5'] = team_stats[0]['averageGamesPlayed']      
      cur_team['losses5v5'] = team_stats[0]['losses']
      cur_team['wins5v5'] = team_stats[0]['wins']
     else:
      print "error, team stats messed up"
     try:
      print cursor.execute(add_team, cur_team)
     except mysql.connector.Error as err:
      print(err.msg)
#       print add_team % cur_team
     else:
      print "Updated Team"
     

     cur_team_history = {}
     for n in cur_team_full['matchHistory']:
      cur_team_history['fullId'] = y
      for z in ['assists', 'date', 'deaths', 'gameId', 'gameMode', 'invalid', 'kills', 'mapId', 'opposingTeamKills', 'opposingTeamName', 'win']:
#        print n[z]
       cur_team_history[z] = n[z]
      try:
#        print(add_team_history, cur_team_history)
       cursor.execute(add_team_history, cur_team_history)
       
      except mysql.connector.Error as err:
       print(err.msg)
      else:
       print "Updated Team-History"
      

     cur_team_roster = {}
     for n in cur_team_full['roster']['memberList']:
      for z in ['inviteDate', 'joinDate', 'playerId', 'status']:
#        print n[z]
       cur_team_roster[z] = n[z]
      if cur_team_full['roster']['ownerId'] == n['playerId']:
       cur_team_roster['isCaptain'] = True
      else:
       cur_team_roster['isCaptain'] = False
      cur_team_roster['teamId'] = y
      try:
       cursor.execute(add_team_roster, cur_team_roster)
      except mysql.connector.Error as err:
       print(err.msg)
      else:
       print "Updated Team-Roster"
    
    if stop == len(team_ids):
        print "Finished %s of %s" % (stop, len(team_ids))
    else:
        print "Finished %s of %s" % (stop + 1, len(team_ids))



 cnx.commit()


update_table("team", create = False)



cursor.close()
cnx.close()
# server.stop()




# cursor.execute("ALTER TABLE challenger CONVERT TO CHARACTER SET UTF8")

# cursor.execute("ALTER TABLE by_league DROP PRIMARY KEY")
# cursor.execute("ALTER TABLE by_league ADD CONSTRAINT id_queue PRIMARY KEY (`playerOrTeamId`, `queue`)")
# 
# 
# cursor.execute("ALTER TABLE team_history DROP PRIMARY KEY")
# cursor.execute("ALTER TABLE team_history ADD CONSTRAINT game_team PRIMARY KEY (`gameId`, `fullId`)")
# 
# cursor.execute("ALTER TABLE team_roster DROP PRIMARY KEY")
# cursor.execute("ALTER TABLE team_roster ADD CONSTRAINT player_team PRIMARY KEY (`playerId`, `teamId`)")
     
     
# cursor.execute("ALTER TABLE by_league MODIFY division varchar(5) NOT NULL")
# cursor.execute("DROP TABLE IF EXISTS by_league")
