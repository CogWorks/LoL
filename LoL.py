
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

ssh = True

# using SSH Tunnel because to connect directly to MySQL on server we need to comment out 
# 'skip-networking' in /etc/mysql/my.cnf which allows non-local connections and is generally less secure

# server = credentials.server
# server.start()


##only use this if you know what you're doing.
requests.packages.urllib3.disable_warnings()

try:
 cnx = mysql.connector.connect(**credentials.config(ssh=ssh))
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
    
    
      
 TABLES['matches'] = ( 
     "CREATE TABLE `matches` ("
     "		`mapId`		int(4)		NOT NULL	,"
     "		`matchCreation`		BIGINT		NOT NULL	,"
     "		`matchDuration`	int(6)		NOT NULL	,"
     "		`matchId`	varchar(20)		NOT NULL	,"
     "		`matchMode`	varchar(20)		DEFAULT NULL	,"
     "		`matchType`	varchar(20)		DEFAULT NULL	,"
     "		`matchVersion`	varchar(20)		DEFAULT NULL	,"
     "		`platformId`	varchar(8)		DEFAULT NULL	,"
     "		`queueType`	varchar(20)		DEFAULT NULL	,"
     "		`region`	varchar(8)		DEFAULT NULL	,"
     "		`season`	varchar(20)		DEFAULT NULL	,"
     " PRIMARY KEY (`matchId`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB")
 
 
 
 


 TABLES['match_participants'] = (
     "CREATE TABLE `match_participants` ("
     "		`matchId`	varchar(20)		NOT NULL	,"
     "		`championId`	int(4)		NOT NULL	,"
     "		`highestAchievedSeasonTier`	varchar(16)		DEFAULT NULL	,"
     "		`participantId`	tinyint		NOT NULL	,"
     "		`profileIcon`	smallint		DEFAULT NULL	,"
     "		`matchHistoryUri`	varchar(45)		DEFAULT NULL	,"
     "		`summonerName`	varchar(25)		DEFAULT NULL	,"
     "		`summonerId`	varchar(16)		NOT NULL	,"
     "		`spell1Id`	smallint		DEFAULT NULL	,"
     "		`spell2Id`	smallint		DEFAULT NULL	,"
     "		`assists`	tinyint		DEFAULT NULL	,"
     "		`champLevel`	tinyint		DEFAULT NULL	,"
     "		`combatPlayerScore`	varchar(8)		DEFAULT NULL	,"
     "		`deaths`	tinyint		DEFAULT NULL	,"
     "		`doubleKills`	tinyint		DEFAULT NULL	,"
     "		`firstBloodAssist`	bool		DEFAULT NULL	,"
     "		`firstBloodKill`	bool		DEFAULT NULL	,"
     "		`firstInhibitorAssist`	bool		DEFAULT NULL	,"
     "		`firstInhibitorKill`	bool		DEFAULT NULL	,"
     "		`firstTowerAssist`	bool		DEFAULT NULL	,"
     "		`firstTowerKill`	bool		DEFAULT NULL	,"
     "		`goldEarned`	int(8)		DEFAULT NULL	,"
     "		`goldSpent`	int(8)		DEFAULT NULL	,"
     "		`inhibitorKills`	tinyint		DEFAULT NULL	,"
     "		`item0`		smallint		DEFAULT NULL	,"
     "		`item1`		smallint		DEFAULT NULL	,"
     "		`item2`	smallint		DEFAULT NULL	,"
     "		`item3`	smallint		DEFAULT NULL	,"
     "		`item4`	smallint		DEFAULT NULL	,"
     "		`item5`	smallint		DEFAULT NULL	,"
     "		`item6`	smallint		DEFAULT NULL	,"
     "		`killingSprees`	tinyint		DEFAULT NULL	,"
     "		`kills`	tinyint		DEFAULT NULL	,"
     "		`largestCriticalStrike`	mediumint		DEFAULT NULL	,"
     "		`largestKillingSpree`	tinyint		DEFAULT NULL	,"
     "		`largestMultiKill`	tinyint		DEFAULT NULL	,"
     "		`magicDamageDealt`	mediumint		DEFAULT NULL	,"
     "		`magicDamageDealtToChampions`	mediumint		DEFAULT NULL	,"
     "		`magicDamageTaken`	mediumint		DEFAULT NULL	,"
     "		`minionsKilled`	smallint		DEFAULT NULL	,"
     "		`neutralMinionsKilled`	smallint		DEFAULT NULL	,"
     "		`neutralMinionsKilledEnemyJungle`	smallint		DEFAULT NULL	,"
     "		`neutralMinionsKilledTeamJungle`	smallint		DEFAULT NULL	,"
     "		`nodeCapture`	smallint		DEFAULT NULL	,"
     "		`nodeCaptureAssist`	smallint		DEFAULT NULL	,"
     "		`nodeNeutralize`	smallint		DEFAULT NULL	,"
     "		`nodeNeutralizeAssist`	smallint		DEFAULT NULL	,"
     "		`objectivePlayerScore`	smallint		DEFAULT NULL	,"
     "		`pentaKills`	tinyint		DEFAULT NULL	,"
     "		`physicalDamageDealt`	mediumint		DEFAULT NULL	,"
     "		`physicalDamageDealtToChampions`	mediumint		DEFAULT NULL	,"
     "		`physicalDamageTaken`	mediumint		DEFAULT NULL	,"
     "		`quadrakills`	tinyint		DEFAULT NULL	,"
     "		`sightWardsBoughtInGame`	smallint		DEFAULT NULL	,"
     "		`teamObjective`	smallint		DEFAULT NULL	,"
     "		`totalDamageDealt`	mediumint		DEFAULT NULL	,"
     "		`totalDamageDealtToChampions`	mediumint		DEFAULT NULL	,"
     "		`totalDamageTaken`	mediumint		DEFAULT NULL	,"
     "		`totalHeal`	mediumint		DEFAULT NULL	,"
     "		`totalPlayerScore`	smallint		DEFAULT NULL	,"
     "		`totalScoreRank`	smallint		DEFAULT NULL	,"
     "		`totalTimeCrowdControlDealt`	smallint		DEFAULT NULL	,"
     "		`totalUnitsHealed`	tinyint		DEFAULT NULL	,"
     "		`towerKills`	tinyint		DEFAULT NULL	,"
     "		`tripleKills`	tinyint		DEFAULT NULL	,"
     "		`trueDamageDealt`	mediumint		DEFAULT NULL	,"
     "		`trueDamageDealtToChampions`	mediumint		DEFAULT NULL	,"
     "		`trueDamageTaken`	mediumint		DEFAULT NULL	,"
     "		`unrealKills`	tinyint		DEFAULT NULL	,"
     "		`visionWardsBoughtInGame`	tinyint		DEFAULT NULL	,"
     "		`wardsKilled`	tinyint		DEFAULT NULL	,"
     "		`wardsPlaced`	tinyint		DEFAULT NULL	,"
     "		`winner`	bool		DEFAULT NULL	,"
     "		`teamId`	varchar(35)		NOT NULL	,"
     "		`lane`	varchar(16)		DEFAULT NULL	,"
     "		`role`	varchar(16)		DEFAULT NULL	,"
     "  CONSTRAINT match_player PRIMARY KEY (`matchId`, `summonerId`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB")
     
 TABLES['match_participant_masteries'] = (
     "CREATE TABLE `match_participant_masteries` ("
     " `matchId` varchar(20) NOT NULL,"
     " `summonerId` varchar(20) NOT NULL,"
     " `rank` smallint NOT NULL,"
     "	`masteryId`	smallint		NOT NULL	,"
     " CONSTRAINT match_participant_mastery PRIMARY KEY (`matchId`, `summonerId`, `masteryId`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB")
           
 TABLES['match_participant_runes'] = (
     "CREATE TABLE `match_participant_runes` ("
     " `matchId` varchar(20) NOT NULL,"
     " `summonerId` varchar(20) NOT NULL,"
     " `rank` smallint NOT NULL,"
     "	`runeId`	smallint		NOT NULL	,"
     " CONSTRAINT match_participant_rune PRIMARY KEY (`matchId`, `summonerId`, `runeId`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB")
     
     
 TABLES['match_participant_deltas'] = (
     "CREATE TABLE `match_participant_deltas` ("
     " `matchId` varchar(20) NOT NULL,"
     " `summonerId` varchar(20) NOT NULL,"
     " `deltaName` varchar(40) NOT NULL,"
     " `deltaTimeframe` varchar(20) NOT NULL,"
     " `value` double NOT NULL,"
     " CONSTRAINT match_participant_delta PRIMARY KEY (`matchID`, `summonerId`, `deltaName`, `deltaTimeframe`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB")


     

     
  
 TABLES['match_teams'] = (
     "CREATE TABLE `match_teams` ("
     "		`matchId`	varchar(20)		NOT NULL	,"
     "		`teamId`	varchar(35)		NOT NULL	,"
     "		`baronKills`	tinyint		DEFAULT NULL	,"
     "		`dominionVictoryScore`	varchar(10)		DEFAULT NULL	,"
     "		`dragonKills`	tinyint		DEFAULT NULL	,"
     "		`firstBaron`	bool		DEFAULT NULL	,"
     "		`firstBlood`	bool		DEFAULT NULL	,"
     "		`firstDragon`	bool		DEFAULT NULL	,"
     "		`firstInhibitor`	bool		DEFAULT NULL	,"
     "		`firstRiftHerald`	bool		DEFAULT NULL	,"
     "		`firstTower`	bool		DEFAULT NULL	,"
     "		`inhibitorKills`	tinyint		DEFAULT NULL	,"
     "		`riftHeraldKills`	tinyint		DEFAULT NULL	,"
     "		`towerKills`	tinyint		DEFAULT NULL	,"
     "		`vilemawKills`	tinyint		DEFAULT NULL	,"
     "		`winner`	bool		DEFAULT NULL	,"
     "  CONSTRAINT match_team PRIMARY KEY (`matchId`, `teamId`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB")
 
 

 TABLES['match_team_bans'] = (
     "CREATE TABLE `match_team_bans` ("
     " `matchId` varchar(20) NOT NULL ,"
     " `teamId` varchar(35) NOT NULL ,"
     " `pickTurn` tinyint NOT NULL ,"
     " `championId` smallint NOT NULL ,"
     "CONSTRAINT match_team_ban PRIMARY KEY (`matchId`, `teamId`, `pickTurn`)"
     ") CHARACTER SET utf8 ENGINE=InnoDB")


# match_timeline


      
 def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

 try:
     cnx.database = DB_NAME 
#      print "Test"   
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




def update_table(table, queue="RANKED_TEAM_5x5", iteratestart=1, iterate=100, create=False, teamIds=False, matchIds=False, checkTeams= False, hangwait=False):
 key = keys[0]
 new_key(key)

 if create== True:
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
 
 
 if table=="all":
  update_table("challenger")
  update_table("master")
  update_table("team", checkTeams =True)
  
 if table=="match":
   add_match = ("INSERT INTO matches "
              "(mapId, matchCreation, matchDuration, matchId, matchMode, matchType, matchVersion, platformId, queueType, region, season) " 
              "VALUES (%(mapId)s, %(matchCreation)s, %(matchDuration)s, %(matchId)s, %(matchMode)s, %(matchType)s, %(matchVersion)s, %(platformId)s, %(queueType)s, %(region)s, %(season)s)")

   add_match_participants = ("INSERT INTO match_participants "
              "(matchId, championId, highestAchievedSeasonTier, participantId, profileIcon, matchHistoryUri, summonerName, summonerId, spell1Id, spell2Id, assists, champLevel, combatPlayerScore, deaths, doubleKills, firstBloodAssist, firstBloodKill, firstInhibitorAssist, firstInhibitorKill, firstTowerAssist, firstTowerKill, goldEarned, goldSpent, inhibitorKills, item0, item1, item2, item3, item4, item5, item6, killingSprees, kills, largestCriticalStrike, largestKillingSpree, largestMultiKill, magicDamageDealt, magicDamageDealtToChampions, magicDamageTaken, minionsKilled, neutralMinionsKilled, neutralMinionsKilledEnemyJungle, neutralMinionsKilledTeamJungle, nodeCapture, nodeCaptureAssist, nodeNeutralize, nodeNeutralizeAssist, objectivePlayerScore, pentaKills, physicalDamageDealt, physicalDamageDealtToChampions, physicalDamageTaken, quadrakills, sightWardsBoughtInGame, teamObjective, totalDamageDealt, totalDamageDealtToChampions, totalDamageTaken, totalHeal, totalPlayerScore, totalScoreRank, totalTimeCrowdControlDealt, totalUnitsHealed, towerKills, tripleKills, trueDamageDealt, trueDamageDealtToChampions, trueDamageTaken, unrealKills, visionWardsBoughtInGame, wardsKilled, wardsPlaced, winner, teamId, lane, role) " 
              "VALUES (%(matchId)s, %(championId)s, %(highestAchievedSeasonTier)s,  %(participantId)s, %(profileIcon)s, %(matchHistoryUri)s, %(summonerName)s, %(summonerId)s, %(spell1Id)s, %(spell2Id)s, %(assists)s, %(champLevel)s, %(combatPlayerScore)s, %(deaths)s, %(doubleKills)s, %(firstBloodAssist)s, %(firstBloodKill)s, %(firstInhibitorAssist)s, %(firstInhibitorKill)s, %(firstTowerAssist)s, %(firstTowerKill)s, %(goldEarned)s, %(goldSpent)s, %(inhibitorKills)s, %(item0)s, %(item1)s, %(item2)s, %(item3)s, %(item4)s, %(item5)s, %(item6)s, %(killingSprees)s, %(kills)s, %(largestCriticalStrike)s, %(largestKillingSpree)s, %(largestMultiKill)s, %(magicDamageDealt)s, %(magicDamageDealtToChampions)s, %(magicDamageTaken)s, %(minionsKilled)s, %(neutralMinionsKilled)s, %(neutralMinionsKilledEnemyJungle)s, %(neutralMinionsKilledTeamJungle)s, %(nodeCapture)s, %(nodeCaptureAssist)s, %(nodeNeutralize)s, %(nodeNeutralizeAssist)s, %(objectivePlayerScore)s, %(pentaKills)s, %(physicalDamageDealt)s, %(physicalDamageDealtToChampions)s, %(physicalDamageTaken)s, %(quadrakills)s, %(sightWardsBoughtInGame)s, %(teamObjective)s, %(totalDamageDealt)s, %(totalDamageDealtToChampions)s, %(totalDamageTaken)s, %(totalHeal)s, %(totalPlayerScore)s, %(totalScoreRank)s, %(totalTimeCrowdControlDealt)s, %(totalUnitsHealed)s, %(towerKills)s, %(tripleKills)s, %(trueDamageDealt)s, %(trueDamageDealtToChampions)s, %(trueDamageTaken)s, %(unrealKills)s, %(visionWardsBoughtInGame)s, %(wardsKilled)s, %(wardsPlaced)s, %(winner)s, %(teamId)s, %(lane)s, %(role)s)")

   add_match_participant_rune = ("INSERT INTO match_participant_runes "
              "(matchId, summonerId, rank, runeId)"
              "VALUES (%(matchId)s, %(summonerId)s, %(rank)s, %(runeId)s)")
              
   add_match_participant_mastery = ("INSERT INTO match_participant_masteries "
              "(matchId, summonerId, rank, masteryId)"
              "VALUES (%(matchId)s, %(summonerId)s, %(rank)s, %(masteryId)s)")

   add_match_participant_delta = ("INSERT INTO match_participant_deltas "
              "(matchId, summonerId, deltaName, deltaTimeframe, value)"
              "VALUES (%(matchId)s, %(summonerId)s, %(deltaName)s, %(deltaTimeframe)s, %(value)s)")
																
   add_match_teams = ("INSERT INTO match_teams "
               "(matchId, teamId, baronKills, dominionVictoryScore, dragonKills, firstBaron, firstBlood, firstDragon, firstInhibitor, firstRiftHerald, firstTower, inhibitorKills, riftHeraldKills, towerKills, vilemawKills, winner) " 
               "VALUES (%(matchId)s, %( teamId)s, %( baronKills)s, %( dominionVictoryScore)s, %( dragonKills)s, %( firstBaron)s, %( firstBlood)s, %( firstDragon)s, %( firstInhibitor)s, %( firstRiftHerald)s, %( firstTower)s, %( inhibitorKills)s, %( riftHeraldKills)s, %( towerKills)s, %( vilemawKills)s, %( winner)s	)")
   
   add_match_bans = ("INSERT INTO match_team_bans "
               "(matchId, teamId, pickTurn, championId) " 
               "VALUES (%(matchId)s, %(teamId)s, %(pickTurn)s, %(championId)s)")
   
   
   if(matchIds==False):
    print "No list of match ids, defaulting to search team_history"
    cursor.execute("SELECT gameId FROM team_history" )         
   

    match_ids_raw = cursor.fetchall()
   
    match_ids = [] 
   
    for x in match_ids_raw:
     for y in x:
      match_ids.append(y)
     
   else:
    print "Given list of match ids."
    match_ids = matchIds
 
#    print match_ids
   teams_data = []
   
   for x in match_ids:
#     print x
    finished = False 
    while finished == False:
     try: 
      cur_match_raw = w.get_match( x, region=None, include_timeline=False)
     
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

       print "%s, Match: %s" % (str(err), x)
       break
#      except:
#       print "Other Error"
      
     else:
      finished = True
      
    cur_match = {}
      
    for y in ["mapId", "matchCreation", "matchDuration", "matchId", "matchMode", "matchType", "matchVersion", "platformId", "queueType", "region", "season"]:
     try:
      cur_match[y] = cur_match_raw[y]
     except:
      cur_match[y] = None
    
#     print cur_match
    
    try:
     cursor.execute(add_match, cur_match)
    except mysql.connector.Error as err:

      if err.errno != 1062:
       print "%s, Match: %s" % (err.msg, x)
#       print add_team % cur_team
    else:
      print "Updated Match"
    
    cur_match_participants_raw = cur_match_raw["participants"]
    cur_match_pi = {}
    for y in cur_match_raw["participantIdentities"]:
     cur_match_pi[y["participantId"]] = y["player"]
     
    
#     print cur_match_pi

    for y in cur_match_participants_raw:
     cur_match_participant = {}
     for z in cur_match_pi[y["participantId"]]:
      cur_match_participant[z] = cur_match_pi[y["participantId"]][z]
#       print cur_match_participant
     cur_match_participant["matchId"] = x
     for z in y:
#       print z
      if isinstance(y[z], list) or isinstance(y[z], dict):
       
       if z == "runes":
        cur_rune = {}
        cur_rune["matchId"] = x
        cur_rune["summonerId"] = cur_match_pi[y["participantId"]]["summonerId"]
        for r in y[z]:
         for r2 in r:
          cur_rune[r2] = r[r2]
          
         try:
          cursor.execute(add_match_participant_rune, cur_rune)
         except mysql.connector.Error as err:
          if err.errno != 1062:
           print "%s, Match: %s, Player %s" % (err.msg, x, cur_match_pi[y["participantId"]]["summonerId"])
 #          print add_team % cur_team
         else:
          print "Updated Rune"
         
         
       elif z == "masteries": 
        cur_mastery = {}  
        cur_mastery["matchId"] = x
        cur_mastery["summonerId"] = cur_match_pi[y["participantId"]]["summonerId"]
        for r in y[z]:
         for r2 in r:
          cur_mastery[r2] = r[r2]
          
         try:
          cursor.execute(add_match_participant_mastery, cur_mastery)
         except mysql.connector.Error as err:
          if err.errno != 1062:
           print "%s, Match: %s, Player: %s" % (err.msg, x, cur_match_pi[y["participantId"]]["summonerId"])
 #          print add_team % cur_team
         else:
          print "Updated Mastery"        
       
       elif z == "stats": 
        for r in ["assists", "champLevel", "combatPlayerScore", "deaths", "doubleKills", "firstBloodAssist", "firstBloodKill", "firstInhibitorAssist", "firstInhibitorKill", "firstTowerAssist", "firstTowerKill", "goldEarned", "goldSpent", "inhibitorKills", "item0", "item1", "item2", "item3", "item4", "item5", "item6", "killingSprees", "kills", "largestCriticalStrike", "largestKillingSpree", "largestMultiKill", "magicDamageDealt", "magicDamageDealtToChampions", "magicDamageTaken", "minionsKilled", "neutralMinionsKilled", "neutralMinionsKilledEnemyJungle", "neutralMinionsKilledTeamJungle", "nodeCapture", "nodeCaptureAssist", "nodeNeutralize", "nodeNeutralizeAssist", "objectivePlayerScore", "pentaKills", "physicalDamageDealt", "physicalDamageDealtToChampions", "physicalDamageTaken", "quadrakills", "sightWardsBoughtInGame", "teamObjective", "totalDamageDealt", "totalDamageDealtToChampions", "totalDamageTaken", "totalHeal", "totalPlayerScore", "totalScoreRank", "totalTimeCrowdControlDealt", "totalUnitsHealed", "towerKills", "tripleKills", "trueDamageDealt", "trueDamageDealtToChampions", "trueDamageTaken", "unrealKills", "visionWardsBoughtInGame", "wardsKilled", "wardsPlaced", "winner"]:
         try:
          cur_match_participant[r] = y[z][r]
         except:
          cur_match_participant[r] = None
       elif z == "timeline":
        for r in y[z]:
         if r=="role" or r=="lane":
          cur_match_participant[r] = y[z][r]
         else:
          cur_delta = {}
          for r2 in y[z][r]:
           cur_delta["matchId"] = x
           cur_delta["summonerId"] = cur_match_pi[y["participantId"]]["summonerId"]
           cur_delta["deltaName"] = r
           cur_delta["deltaTimeframe"] = r2
           cur_delta["value"] = y[z][r][r2]
           try:
            cursor.execute(add_match_participant_delta, cur_delta)
           except mysql.connector.Error as err:
            if err.errno != 1062:
             print "%s, Match: %s, Player: %s" % (err.msg, x, cur_match_pi[y["participantId"]]["summonerId"])
   #          print add_team % cur_team
           else:
            print "Updated Mastery"  
       
       else:
        print z
       
       
      else:
       cur_match_participant[z] = y[z]
     
#      print cur_match_participant_raw     
     print cur_match_participant["nodeCapture"]
     try: 
      cursor.execute(add_match_participants, cur_match_participant)
#       print "Try"
     
     except mysql.connector.Error as err:
      if err.errno != 1062:
       print "%s, Match: %s" % (err.msg, x)
#       print add_team % cur_team
     else:
      print "Updated Match-Participant"




 
 
																																																																																																					
																																																																																																					
																																																																																																					
																																																																																											
																																																																																																					


 cnx.commit()



# HOW TO USE.

# update_table(table= "create", create = False)
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


# update_table("all")
# this function will cycle from updating challenger -> master -> team -> checkteam, will not do iterate

# update_table("match", matchIds=[], create=False)
# this function will import all non-timeline data from a given list of matchIds. if no matchIds are supplied, it will automatically search through the list of matchIds in 'team-history'


# update_table("iterate",iteratestart=300, iterate=700, checkTeams=True)



# update_table("match", matchIds=[2044253864], create=False)




cursor.close()
cnx.close()
if ssh == True:
 credentials.server.stop()



##For personal reference, just to keep track of alterations i've made after table creation
# cursor.execute("ALTER TABLE team_roster MODIFY joinDate BIGINT DEFAULT NULL")

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
