
import ssl
import riotwatcher
import urllib2
import json
import requests

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


##only use this if you know what you're doing.
requests.packages.urllib3.disable_warnings()

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

key = keys[0]


        
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
    "  `lastGameDate` BIGINT DEFAULT NULL,"
    "  `lastJoinDate` BIGINT NOT NULL,"
    "  `lastJoinedRankedTeamQueueDate` BIGINT NOT NULL,"
    "  `modifyDate` BIGINT NOT NULL,"
    "  `name` varchar(25) NOT NULL,"
    "  `secondLastJoinDate` BIGINT DEFAULT NULL,"
    "  `status` varchar(25) NOT NULL,"
    "  `tag` varchar(25) NOT NULL,"
    "  `thirdLastJoinDate` BIGINT DEFAULT NULL,"
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
     "  `joinDate` BIGINT DEFAULT NULL,"
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


 
 



def new_key (t):
 global w
 w = riotwatcher.RiotWatcher(t)




def update_table(table, queue="RANKED_TEAM_5x5", iteratestart=1, iterate=100, create=False, teamIds=False, checkTeams= False, hangwait=False):
 key = keys[0]
 new_key(key)
 

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
    print "%s, Team: %s" % (err.msg, x['playerOrTeamId'])
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
    print "%s, Team: %s" % (err.msg, x['playerOrTeamId'])
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
   
   for x in xrange(0,(int(len(team_ids)/10)+1)):
    stop = ((x+1)*10)

    if x == int(len(team_ids)/10):
     stop = (len(team_ids))
    if stop == x*10:
     continue
  
#     print "%s, %s" % ((x*10), stop)

    def get_leagues(team_ids=team_ids,x=x, stop=stop, key=key, unauthorized_cycle=False):
     finished = False
     unauthorized_key = False
     while finished == False:
      err = []
      try:

       league_entries = w.get_league_entry(team_ids=team_ids[(x*10):stop]) if unauthorized_cycle==False else w.get_league_entry(team_ids=team_ids)
      except riotwatcher.riotwatcher.LoLException as err:
       if str(err) == "Unauthorized" :
#         or str(err) == "Internal server error"
#          print "%s, using new key" % ("Unauthorized" if str(err) == "Unauthorized" else "Server Error")

        print "Unauthorized, using new key" 
 # This to ensure that you only try one new key for unauthorized, just switch truth value
       
        unauthorized_key= (True if unauthorized_key == False else False)
#         print unauthorized_key

 #       print str(err)
       if str(err) == "Too many requests":
 #        print "New Key"

 # Make sure to reset 'unauthorized_key' because this new key was due to rate-limit
        unauthorized_key=False


        if key == keys[len(keys)-1]:
         key = keys[0]
        else:
         if len(keys)>1:
          key = key = keys[keys.index(key)+1]
         else:
          print "Too many requests, not enough keys."
          if hangwait == False:
           print "Break, hangwait is off."
           break 
         
        if str(err) != "Unauthorized" and str(err) != "Too many requests":
         print "Break, %s" % (str(err))
         break 

# Checks to make sure the error is not just coming from missing data     
       if str(err) == "Game data not found":
        print "No data, skipping %s" % (team_ids if unauthorized_cycle == True else team_ids[(x*10):stop])
        league_entries = {}
        finished = True   
# Basically this checks to see if unauthorized_key has been used and switched back to false. This should only happen if you have two unauthorized key changes in a row
       elif unauthorized_key == True or str(err) != "Unauthorized":
#         and str(err) != "Internal server error"
        print "New key assigned, %s" % str(err)
#         print str(err)
        new_key(key)
       elif unauthorized_key == False and str(err) == "Unauthorized":
#          or str(err) == "Internal server error"
        league_entries = {}
        if unauthorized_cycle== False:
         print "Unauthorized, checking individual"
         for s in team_ids[(x*10):stop]:
          unauthorized_key=False
#           print s
          cur_entry = get_leagues(team_ids=[s], unauthorized_cycle=True)
          if cur_entry != {}:
           league_entries[s] = cur_entry[s]
          else:
#            print "No entry for %s" % s
           continue

          finished = True
        else:         
#          print "skipping %s" % team_ids
         league_entries = {}
         finished = True
       
 
      else: 
#        print "Success"
#        print league_entries
       finished = True
     
     return league_entries



    league_entries = get_leagues()
    
    for z in league_entries:
     for y in league_entries[z]:
      for v in y['entries']:
       v['league'] = y['tier']
       v['team'] = (True if y['queue']!="RANKED_SOLO_5x5" else False)
       v['queue'] = y['queue']
#  for right now we're just going to discard miniSeries data
       if "miniSeries" in v:
        del v['miniSeries']
       try:
 #        print x['playerOrTeamId']
        cursor.execute(add_league, v)
       except mysql.connector.Error as err:
        if err.errno != 1062:
         print "%s, Team: %s" % (err.msg, v['playerOrTeamId']) 


       else:
        print "Updated By-League"    

    if stop==(len(team_ids)):
     print "Finished %s of %s" % (stop, len(team_ids))
    else:
     print "Finished %s of %s" % (stop+1, len(team_ids))
        
        
        
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
   
   if(teamIds==False):
    print "No list of team ids, defaulting to search by_league"
    cursor.execute("SELECT playerOrTeamId FROM by_league WHERE team = True" )         
   

    team_ids_raw = cursor.fetchall()
   
    team_ids = [] 
   
    for x in team_ids_raw:
     for y in x:
      team_ids.append(y)
     
   else:
    print "Given list of team ids."
    team_ids = teamIds
 
   teams_data = []

   for x in xrange(0,(int(len(team_ids)/10)+1)):

    stop = ((x+1)*10)


    if x == int(len(team_ids)/10) and (x+1)*10 != int(len(team_ids)/10):
     stop = (len(team_ids))
#     print "%s, %s" % (x,stop)
    if x*10 == stop:
     continue


#     print "%s, %s" % (x*10, stop)
    finished = False 
    while finished == False:
     try: 
      teams_data = w.get_teams(team_ids[(x*10):stop])
     
     except riotwatcher.riotwatcher.LoLException as err:
      if str(err) == "Too many requests" or str(err) == "Unauthorized":
#         print "New Key" 
        if str(err) == "Unauthorized":
         print "Unauthorized, using new key"
        if key == keys[len(keys)-1]:
         key = keys[0]
        else:
         if len(keys)>1:
          key = keys[keys.index(key)+1]
         else:
          print "Too many requests, not enough keys."
          if hangwait == False:
           break 
        new_key(key)
        
      else:

       print "%s, Team: %s" % (str(err), team_ids[(x*10):stop])
       break
     except:
      print "Other Error"
     else:
      finished = True

    for y in team_ids[(x*10):stop]:
#      print y
     cur_team_full = todict(teams_data).get(y)
     cur_team = {}
     for n in ['createDate', 'fullId', 'lastGameDate', 'lastJoinDate', 'lastJoinedRankedTeamQueueDate', 'modifyDate', 'name', 'secondLastJoinDate', 'status', 'tag', 'thirdLastJoinDate']:
      try:
#        print "tried"
       cur_team[n] = cur_team_full[n]

      except:
#        print "set to null"
       cur_team[n] = None

#      print cur_team      
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
      if err.errno != 1062:
       print "%s, Team: %s" % (err.msg, y)
#       print add_team % cur_team
     else:
      print "Updated Team"
     

     cur_team_history = {}
     try:
      for n in cur_team_full['matchHistory']:
       cur_team_history['fullId'] = y
       for z in ['assists', 'date', 'deaths', 'gameId', 'gameMode', 'invalid', 'kills', 'mapId', 'opposingTeamKills', 'opposingTeamName', 'win']:
#         print n[z]
#         print y
        cur_team_history[z] = n[z]

       try:
#         print(add_team_history, cur_team_history)
        cursor.execute(add_team_history, cur_team_history)
       
       except mysql.connector.Error as err:
        if err.errno != 1062:
         print "%s, Team: %s" (err.msg, y)
      
       else:
        print "Updated Team-History"
        
     except:
      print "No Team-History"


     cur_team_roster = {}
     try:  
      for n in cur_team_full['roster']['memberList']:
#        print(n)
       for z in ['inviteDate', 'joinDate', 'playerId', 'status']:
#         print n[z]
        try:
         cur_team_roster[z] = n[z]
        except:
         cur_team_roster[z] = None
       if cur_team_full['roster']['ownerId'] == n['playerId']:
        cur_team_roster['isCaptain'] = True
       else:
        cur_team_roster['isCaptain'] = False
       cur_team_roster['teamId'] = y
       try:
        cursor.execute(add_team_roster, cur_team_roster)
       except mysql.connector.Error as err:
        if err.errno != 1062:
         print "%s, Team: %s" % (err.msg, y)
       else:
        print "Updated Team-Roster"
     except:
      if (cur_team_full['status']=="DISBANDED"):
       print "No Team-Roster -- Team Disbanded"
      else:
       print "No Team-Roster"
    

    print "Finished %s of %s" % (stop, len(team_ids))
        
   if checkTeams==True:
    print "Checking Teams"
    update_table("checkteams")
    

 if table=="iterate":
    team_ids = []
    err = []
    for x in xrange(int(round(iteratestart, -1)/10), int((round(iteratestart+iterate, -1)/10))):
     
     stop = (((x+1)*10) if ((x+1)*10) <= int(iteratestart+iterate) else (int(iteratestart+iterate)))
     if x*10 == stop:
      continue

     ids = []
     for y in xrange((x*10), (x+1)*10):
      ids.append(y)
#      print ids
     
     finished = False
     while finished == False:
      try:
#        print "trying"
       teams = w.get_teams_for_summoners(ids)
      
      except riotwatcher.riotwatcher.LoLException as err:
 #       print str(err)
       if str(err) == "Game data not found":
        finished = True
       elif str(err) == "Too many requests" or str(err) == "Unauthorized":
#         print "New Key" 
        if str(err) == "Unauthorized":
         print "Unauthorized, using new key"
        if key == keys[len(keys)-1]:
         key = keys[0]
        else:
         if len(keys)>1:
          key = keys[keys.index(key)+1]
         else:
          print "Too many requests, not enough keys."
          if hangwait == False:
           break 
        new_key(key)
        
       else:

        print "%s, Team: %s" % (str(err), ids)
        break
      else:
       finished = True

     if err == "Game data not found":
       err = []
       continue
       
#      print teams 
     for y in teams:
      for z in teams[y]:
       team_ids.append(z['fullId'])
#         print v
     print "Finished %s of %s, %s teams found." % (int(stop-iteratestart), int(iterate), len(team_ids)) 
    print "Updating Team Table"
    update_table("team", teamIds = team_ids, checkTeams = checkTeams)  
      
  



 cnx.commit()



# HOW TO USE.

# update_table(create = False)
# creates tables default is false, using create_tables, you can set this true while using any other pseudo-function below


# update_table("challenger",queue = "RANKED_TEAM_5x5")
# pulls down all members of division I of challenger in given queue, queue is defaulted to 'RANKED_TEAM_5x5' 
# set queue to:
# RANKED_SOLO_5x5
# RANKED_TEAM_3x3
# RANKED_TEAM_5x5


# update_table("master", queue = "RANKED_TEAM_5x5")
# same as above, but for master division I


# update_table("checkteams", hangwait=False)
# this checks the team table and grabs a list of all the ids. it then gets the league information for each team. 
# This is no longer the only function that will iterate through your list of keys.
# Hangwait=True enables the function to keep attempting the call until it is allowed through with the current key. 
# Hangwait is default false, but that only applies if there is only 1 key in your credentials file.  


# update_table("team", teamIds=False, checkTeams=False)
# this is the primary mechanism, it grabs all the team ids from the by-league table and gets all the information from the team api
# it then sorts it into team, team-history, and team-roster tables.
# Additionally, this table can be supplied with pretty much any length of team ids and it will iterate through those instead


# update_table("iterate",iteratestart=1, iterate=100, checkTeams=False,  hangwait=False)
# give this function a starting id and it will search for all team-ids associated with that id. 
# it will do this [iterate] number of times. Once the list is compiled, it sends it through update_table("team")
# optionally you can set it so that it will also automatically run update_table("checkteams") to verify [by setting checkTeams=True]. 
# Note: if you want to start at id "1" and end at id "100" you would need to set iteratestart=1, iterate=100
# This function will now iterate through keys in the same way check teams does, hangwait is also an option.



# update_table("iterate",iteratestart=300, iterate=700, checkTeams=True)
# update_table("checkteams")




cursor.close()
cnx.close()

# server.stop()



##For personal reference, just to keep track of alterations i've made after table creation
# cursor.execute("ALTER TABLE team_roster MODIFY joinDate BIGINT DEFAULT NULL")
# cursor.execute("ALTER TABLE team MODIFY secondLastJoinDate BIGINT DEFAULT NULL")
# cursor.execute("ALTER TABLE team MODIFY thirdLastJoinDate BIGINT DEFAULT NULL")
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
