#!/usr/bin/python

## MUST INSTALL:
## SSHTUNNEL
## RIOTWATCHER
## mysql-connector-python

import ssl
import riotwatcher
import urllib2
import json
import requests
import time
import csv

from utils import todict
import mysql.connector
from mysql.connector import errorcode


from time import sleep

import credentials

##making ssh option a command line option
import sys, getopt
import argparse
if __name__ == '__main__' :

 parser = argparse.ArgumentParser(description="LoL Scrapper", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
 parser.add_argument("-s", dest="ssh", required=False, help="using ssh or not", metavar="ssh")
 args = parser.parse_args()
 ssh = args.ssh
 
 

def strip_to_list (data_raw):
 data = []
 for x in data_raw:
  for y in x:
   data.append(y)
 return data

class Scrapper:
 
 
 def __init__(self, ssh=True):

  if ssh == "True":
   ssh = True
  elif ssh == "False":
   ssh = False

# using SSH Tunnel because to connect directly to MySQL on server we need to comment out 
# 'skip-networking' in /etc/mysql/my.cnf which allows non-local connections and is generally less secure
# and additionally we'd have to change bind-address from 127.0.0.1 to 0.0.0.0

# server = credentials.server
# server.start()


##only use this if you know what you're doing.
  requests.packages.urllib3.disable_warnings()

  try:
   self.cnx = mysql.connector.connect(**credentials.config(ssh=ssh))
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)
  else:
    print "Connected to %s database" % credentials.config()['database']
  self.cursor = self.cnx.cursor()
  global keys
 
  keys = credentials.keys

  self.key = keys[0]





 def create_tables(self):
 
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
     "  `retrieved` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
     "  CONSTRAINT id_queue PRIMARY KEY (`playerOrTeamId`, `queue`, `retrieved`)"
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


  TABLES['match_timeline'] = (
      "CREATE TABLE `match_timeline` ("
      " `matchId` varchar(20) NOT NULL,"
      " `summonerId` varchar(20) NOT NULL,"
      " `timestamp` mediumint NOT NULL,"
      " `currentGold` mediumint DEFAULT NULL,"
      " `positionX` smallint DEFAULT NULL,"
      " `positionY` smallint DEFAULT NULL,"
      " `minionsKilled` smallint DEFAULT NULL,"
      " `level` tinyint DEFAULT NULL,"
      " `jungleMinionsKilled` smallint DEFAULT NULL,"
      " `totalGold` mediumint DEFAULT NULL,"
      " `dominionScore` mediumint DEFAULT NULL,"
      " `participantId` tinyint DEFAULT NULL,"
      " `xp` int DEFAULT NULL,"
      " `teamScore` mediumint DEFAULT NULL,"
      " `timelineInterval` int NOT NULL,"
      "CONSTRAINT match_summoner_time PRIMARY KEY (`matchId`, `summonerId`, `timestamp`)"
      ") CHARACTER SET utf8 ENGINE=InnoDB")
     
  TABLES['match_timeline_events'] = (
      "CREATE TABLE `match_timeline_events` ("
      " `eventId` varchar(10) NOT NULL,"
      " `matchId` varchar(20) NOT NULL,"
      " `summonerId` varchar(20) DEFAULT NULL,"
      " `timelineTimestamp` mediumint NOT NULL,"
      " `eventTimestamp` mediumint NOT NULL,"
      " `ascendedType` varchar(25) DEFAULT NULL,"
      " `assistingParticipants` bool DEFAULT NULL,"
      " `buildingType` varchar(25) DEFAULT NULL,"
      " `creatorId` varchar(20) DEFAULT NULL,"
      " `eventType` varchar(25) NOT NULL,"
      " `itemAfter` smallint DEFAULT NULL,"
      " `itemBefore` smallint DEFAULT NULL,"
      " `itemId` smallint DEFAULT NULL,"
      " `killerId` varchar(20) DEFAULT NULL,"
      " `laneType` varchar(10) DEFAULT NULL,"
      " `levelUpType` varchar(8) DEFAULT NULL,"
      " `monsterType` varchar(15) DEFAULT NULL,"
      " `pointCaptured` varchar(8) DEFAULT NULL,"
      " `positionX` smallint DEFAULT NULL,"
      " `positionY` smallint DEFAULT NULL,"
      " `skillSlot` smallint DEFAULT NULL,"
      " `teamId` smallint DEFAULT NULL,"
      " `towerType` varchar(20) DEFAULT NULL,"
      " `victimId` varchar(20) DEFAULT NULL,"
      " `wardType` varchar(25) DEFAULT NULL,"
      "CONSTRAINT match_event PRIMARY KEY (`matchId`, `eventId`)"
      ") CHARACTER SET utf8 ENGINE=InnoDB")
     
     
     
     
    
     
  TABLES['match_timeline_events_assist'] = (
      "CREATE TABLE `match_timeline_events_assist` ("
      " `eventId` varchar(10) NOT NULL,"
      " `matchId` varchar(20) NOT NULL,"
      " `assistId` varchar(20) NOT NULL,"
      "CONSTRAINT event_assist PRIMARY KEY (`eventId`, `assistId`)"
      ") CHARACTER SET utf8 ENGINE=InnoDB")

     


      
  def create_database(cursor):
     try:
         self.cursor.execute(
             "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
     except mysql.connector.Error as err:
         print("Failed creating database: {}".format(err))
         exit(1)

  try:
      self.cnx.database = DB_NAME 
 #      print "Test"   
  except mysql.connector.Error as err:
      if err.errno == errorcode.ER_BAD_DB_ERROR:
          create_database(cursor)
          self.cnx.database = DB_NAME
         
      else:
          print(err)
          exit(1)   
  for name, ddl in TABLES.iteritems():
      try:
          print("Creating table {}: ".format(name))
          self.cursor.execute(ddl)
      except mysql.connector.Error as err:
          if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
              print("already exists.")
          else:
              print(err.msg)
      else:
          print("OK")



 def new_key (self, t=None, rate=0, drop=False):
  global w
#   global key

  if t:
   self.key = t
 
  if self.key == keys[len(keys)-1]:
   if drop == True:
    del keys[len(keys)-1]
   self.key = keys[0]
  else:
   if len(keys)>1:
    if drop == True:
     new_key = keys[keys.index(self.key)+1] 
     del keys[keys.index(key)]
     self.key = new_key
    else:
     self.key = keys[keys.index(self.key)+1] 
   else:
    print "Only one key."
    if drop == True:
     print "Can't drop only key. Breaking."
     stop
    self.key = keys[0]
 
 
 
 #  if riotwatcher.RiotWatcher(key).can_make_request():
  self.w = riotwatcher.RiotWatcher(self.key, limits=(riotwatcher.RateLimit(3000,10), riotwatcher.RateLimit(180000,600)))
 
 #  print self.w.can_make_request()
 #  else: 
 #   if rate == len(keys):
 #    time.sleep(3)
 #   else:
 #    print "Rate Limit Reached"
 #    rate += 1
 #    self.new_key(t = key, rate=rate)
 
 
 def get_leagues(self, team_ids=None,x=None, stop=None, key=None, unauthorized_cycle=False, team=True, feedback="all"):
  finished = False
  unauthorized_key = False
  while finished == False:
   err = []
   try:
    if team==True:
     league_entries = self.w.get_league_entry(team_ids=team_ids[(x*10):stop]) if unauthorized_cycle==False else self.w.get_league_entry(team_ids=team_ids)
    else:
     league_entries = self.w.get_league_entry(summoner_ids=team_ids[(x*10):stop]) if unauthorized_cycle==False else self.w.get_league_entry(summoner_ids=team_ids)


   except riotwatcher.riotwatcher.LoLException as err:
    drop = False
    if str(err) == "Unauthorized" :

     if feedback == "all":
      print "Unauthorized, using new key" 
 # This to ensure that you only try one new key for unauthorized, just switch truth value
   
     unauthorized_key= (True if unauthorized_key == False else False)
 #         print unauthorized_key

 #       print str(err)
    if str(err) == "Too many requests" or str(err) == "Blacklisted key":
     if str(err) == "Blacklisted key":
      if feedback == "all":
       print "Blacklisted key, using new key, dropping current."
      drop = True
     unauthorized_key=False
     
     if str(err) != "Unauthorized" and str(err) != "Too many requests" and str(err) != "Blacklisted key":
      if feedback != "silent":
       print "Break, %s" % (str(err))
      break 


   
 # Checks to make sure the error is not just coming from missing data     
    if str(err) == "Game data not found":
     if feedback == "all":
      print "No data, skipping %s" % (team_ids if unauthorized_cycle == True else team_ids[(x*10):stop])
     league_entries = {}
     finished = True   
 # Basically this checks to see if unauthorized_key has been used and switched back to false. This should only happen if you have two unauthorized key changes in a row
    elif unauthorized_key == True or str(err) != "Unauthorized":
     if feedback == "all": 
      print "New key assigned, %s" % str(err)
 #         print str(err)
    
     self.new_key(drop = drop)
    elif unauthorized_key == False and str(err) == "Unauthorized":
     league_entries = {}
     if unauthorized_cycle== False:
      if feedback == "all": 
       print "Unauthorized, checking individual"
      for s in team_ids[(x*10):stop]:
       if team == False:
        time.sleep(5)
       unauthorized_key=False
 #       print team
       cur_entry = self.get_leagues(team_ids=[s], x=x, stop=stop, key=self.key, unauthorized_cycle=True, team=team)
       print cur_entry 
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
 


 def update_table(self, table, queue="RANKED_TEAM_5x5", iteratestart=1, iterate=100, create=False, teamIds=False, matchIds=False, checkTeams= False, hangwait=False, feedback="all", suppress_duplicates = False, timeline = False, allow_updates=False):
  
  feedback = feedback.lower()
  if feedback != "all" and feedback != "quiet" and feedback != "silent":
   print "Invalid value for 'Feedback' option, reverting to default. All feedback will be shown."
   feedback = "all"
  if feedback == "quiet" or feedback == "silent":
   suppress_duplicates = True
  
  self.key = keys[0]
  self.new_key()

  if create== True:
   create_tables()

  if table=="challenger":
   add_challenger = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")

   challenger = self.w.get_challenger(queue=queue)

   s = []
   for x in todict(challenger)['entries']:
    x['league'] = "challenger"
    x['team'] = (True if queue!="RANKED_SOLO_5x5" else False)
    x['queue'] = queue
    s.append(x)
   
   try:
    self.cursor.executemany(add_challenger, s)


   except mysql.connector.Error as err:
   ##error 1062 is duplicate entry error for mysql
    if err.errno == 1062:
     if feedback == "all" and suppress_duplicates == False :
      print "Error %s" % (err.errno)
    else:
     if feedback != "silent":
      print "Error %s" % (err.errno)
         

    
   else:
    if feedback != "silent" :
     print "Updated Challenger"
  
   if err.errno == 1062 and feedback != "silent":
    print "Finished Challenger"
   
  
  
  
  if table=="master":
   add_master = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")

   master = self.w.get_master(queue=queue)


   s = []
   for x in todict(master)['entries']:
    x['league'] = "master"
    x['team'] = (True if queue!="RANKED_SOLO_5x5" else False)
    x['queue'] = queue
    s.append(x)
   
   try:
      self.cursor.executemany(add_master, s)
   
   except mysql.connector.Error as err:
   ##error 1062 is duplicate entry error for mysql
    if err.errno == 1062:
     if feedback == "all" and suppress_duplicates == False:
      print "Error %s" % (err.errno)
    else:
     if feedback != "silent":
      print "Error %s" % (err.errno)
    
    
   else:
    if feedback != "silent" :
     print "Updated Master"

    
   if err.errno == 1062 and feedback != "silent":
    print "Finished Master"  
    
  if table=="checkteams":
    add_league = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")


    self.cursor.execute("SELECT playerOrTeamID, queue FROM by_league")
    existing_entries = self.cursor.fetchall()
   
    if(teamIds==False):
     if feedback == "all":
      print "No list of team ids, defaulting to search by_league"
     self.cursor.execute("SELECT fullId FROM team" )    
   

     team_ids_raw = [] 
     team_ids_raw = self.cursor.fetchall()
   
     team_ids = [] 
   
     for x in team_ids_raw:
      for y in x:
       team_ids.append(y)
     
    else:
     if feedback == "all":
      print "Given list of team ids."
     team_ids = teamIds
 
   
    for x in xrange(0,(int(len(team_ids)/10)+1)):
     stop = ((x+1)*10)

     if x == int(len(team_ids)/10):
      stop = (len(team_ids))
     if stop == x*10:
      continue
  
 #     print "%s, %s" % ((x*10), stop)





     league_entries = self.get_leagues(team_ids=team_ids,x=x, stop=stop, key=self.key, unauthorized_cycle=False, team=True, feedback=feedback)
     by_leagues = []
     for z in league_entries:
      for y in league_entries[z]:
       if 'entries' in y:
        for v in y['entries']:
         if allow_updates == True or ( (v['playerOrTeamId'], y['queue']) not in existing_entries):
          v['league'] = y['tier']
          v['team'] = (True if y['queue']!="RANKED_SOLO_5x5" else False)
          v['queue'] = y['queue']
   #  for right now we're just going to discard miniSeries data
          if "miniSeries" in v:
           del v['miniSeries']
          by_leagues.append(v)
     try:
      self.cursor.executemany(add_league, by_leagues)
     except mysql.connector.Error as err:
      if err.errno != 1062 or suppress_duplicates == False:
      
       if feedback != "silent":
        print err.errno


     else:
      if feedback != "silent":
       print "Updated By-League"  
       
 #        OLD METHOD
 #        try:
 #  #        print x['playerOrTeamId']
 #         self.cursor.execute(add_league, v)
 #        except mysql.connector.Error as err:
 #         if err.errno != 1062 or feedback == "all":
 #          if feedback != "silent":
 #           print "%s, Team: %s" % (err.msg, v['playerOrTeamId']) 
 # 
 # 
 #        else:
 #         if feedback != "silent":
 #          print "Updated By-League"    

     if stop==(len(team_ids)):
      if feedback != "silent":
       print "Finished %s of %s" % (stop, len(team_ids))
     else:
      if feedback == "all":
       print "Finished %s of %s" % (stop+1, len(team_ids))
        
  if table=="membertiers":
    add_league = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")
    summoner_ids_raw = [] 
    if matchIds == False:
     if feedback != "silent":
      print "No matches given, using all."
     self.cursor.execute("SELECT matchId FROM matches")
     matchIds= strip_to_list(self.cursor.fetchall())

    for x in matchIds:
 #     self.cursor.execute("SELECT summonerId, teamId FROM match_participants where matchId = %s" % x)

     self.cursor.execute("SELECT summonerId FROM match_participants where matchId = %s" % x)
     summoner_ids_raself.w.append(self.cursor.fetchall())
    
    summoner_ids = [] 
  
    for x in summoner_ids_raw:
     for y in x:
      for z in y:
       summoner_ids.append(z)
   

   
    for x in xrange(0,(int(len(summoner_ids)/10)+1)):
     stop = ((x+1)*10)

     if x == int(len(summoner_ids)/10):
      stop = (len(summoner_ids))
     if stop == x*10:
      continue
    
 #     print summoner_ids[(x*10):stop]
    
     league_entries = self.get_leagues(team_ids=summoner_ids,x=x, stop=stop, key=self.key, unauthorized_cycle=False, team=False, feedback=feedback)

     by_leagues = []
     for z in league_entries:
      for y in league_entries[z]:
       if "entries" in y:
        for v in y['entries']:
         v['league'] = y['tier']
         v['team'] = (True if y['queue']!="RANKED_SOLO_5x5" else False)
         v['queue'] = y['queue']
  #  for right now we're just going to discard miniSeries data
         if "miniSeries" in v:
          del v['miniSeries']
         by_leagues.append(v)

    
     try:
      self.cursor.executemany(add_league, by_leagues)
     except mysql.connector.Error as err:
      if err.errno != 1062 or suppress_duplicates == False:
      
       if feedback != "silent":
        print "%s - Member-Tiers" % err.msg

     else:
      if feedback != "silent":
       print "Updated Member-Tiers"  
    
     self.cnx.commit()
     if stop==(len(summoner_ids)):
      if feedback != "silent":
       print "Finished %s of %s" % (stop, len(summoner_ids))
     else:
      if feedback == "all":
       print "Finished %s of %s" % (stop+1, len(summoner_ids))       
        
  if table=="team":
   
    add_team = ("INSERT IGNORE INTO team "
                "(createDate, fullId, lastGameDate, lastJoinDate, lastJoinedRankedTeamQueueDate, modifyDate, name, secondLastJoinDate, status, tag, thirdLastJoinDate, averageGamesPlayed3v3, losses3v3, wins3v3, averageGamesPlayed5v5, losses5v5, wins5v5) " 
                "VALUES (%(createDate)s, %(fullId)s, %(lastGameDate)s, %(lastJoinDate)s, %(lastJoinedRankedTeamQueueDate)s, %(modifyDate)s, %(name)s, %(secondLastJoinDate)s, %(status)s, %(tag)s, %(thirdLastJoinDate)s, %(averageGamesPlayed3v3)s, %(losses3v3)s, %(wins3v3)s, %(averageGamesPlayed5v5)s, %(losses5v5)s, %(wins5v5)s) " )
 
    add_team_history = ("INSERT IGNORE INTO team_history "
             "(fullId, assists, date, deaths, gameId, gameMode, invalid, kills, mapId, opposingTeamKills, opposingTeamName, win)" 
             "VALUES (%(fullId)s, %(assists)s, %(date)s, %(deaths)s, %(gameId)s, %(gameMode)s, %(invalid)s, %(kills)s, %(mapId)s, %(opposingTeamKills)s, %(opposingTeamName)s, %(win)s)" )
   
    add_team_roster = ("INSERT IGNORE INTO team_roster "
             "(inviteDate, joinDate, playerId, status, isCaptain, teamId)"
             "VALUES (%(inviteDate)s, %(joinDate)s, %(playerId)s, %(status)s, %(isCaptain)s, %(teamId)s)")
   
    if(teamIds==False):
     if feedback == "all":
      print "No list of team ids, defaulting to search by_league"
     self.cursor.execute("SELECT playerOrTeamId FROM by_league WHERE team = True" )         
   

     team_ids_raw = self.cursor.fetchall()
   
     team_ids = [] 
   
     for x in team_ids_raw:
      for y in x:
       team_ids.append(y)
     
    else:
     if feedback == "all":
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
     service = 0
     while finished == False:
      try: 
       teams_data = self.w.get_teams(team_ids[(x*10):stop])
     
      except riotwatcher.riotwatcher.LoLException as err:
       if str(err) == "Too many requests" or str(err) == "Service unavailable" or str(err) == "Unauthorized" or str(err) == "Internal server error" or str(err) == "Blacklisted key":
 #         print "New Key" 
         drop = False
         if str(err) == "Blacklisted key":
          if feedback == "all":
           print "Blacklisted key, using new key, dropping current."
          drop = True

         if str(err) == "Service unavailable" and service != 10:
          if feedback == "all":
           print "Service unavailable, using new key"
          service += 1
         if str(err) == "Unauthorized" or "Internal server error":
          if feedback == "all":
           print "Unauthorized, using new key"
         if len(keys)==1:
          if feedback == "all":
           print "Too many requests, not enough keys."
          if hangwait == False:
           break 
          else:
           time.sleep(5)
         self.new_key(drop = drop)
        
       else:
        if feedback == "all":
         print "%s, Teams: %s" % (str(err), team_ids[(x*10):stop])
        break
      except:
       if feedback != "silent":
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

      try:
       if "teamStatDetails" in cur_team_full:
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
         if feedback != "silent":
          print "Error, team stats messed up"
      
        try:
         self.cursor.execute(add_team, cur_team)
        except mysql.connector.Error as err:
         if err.errno != 1062 or suppress_duplicates == False:
          if feedback != "silent":
           print "%s, Team: %s" % (err.errno, y)
   #       print add_team % cur_team
        else:
         if feedback == "all":
          print "Updated Team"

     
       all_teams = []
       team_history = True
       try:
        for n in cur_team_full['matchHistory']:
         cur_team_history = {}
         cur_team_history['fullId'] = y
         for z in ['assists', 'date', 'deaths', 'gameId', 'gameMode', 'invalid', 'kills', 'mapId', 'opposingTeamKills', 'opposingTeamName', 'win']:
          cur_team_history[z] = n[z]
         all_teams.append(cur_team_history)

     
  #        OLD METHOD
  #        try:
  # #         print(add_team_history, cur_team_history)
  #         self.cursor.execute(add_team_history, cur_team_history)
  #        
  #        except mysql.connector.Error as err:
  #         if err.errno != 1062:
  #          print "%s, Team: %s" (err.msg, y)
  #       
  #        else:
  #         print "Updated Team-History"
        
       except:
        team_history = False
        if feedback == "all":
         print "No Team-History"


       if team_history == True:
        try:
         self.cursor.executemany(add_team_history, all_teams)
   #       print test_team
        except mysql.connector.Error as err:
         if err.errno != 1062 or suppress_duplicates == False:
          if feedback != "silent":
           print "Error %s" % err.errno
        else:
         if feedback == "all":
          print "Updated Team-History" 
      
      
      

       all_teams = []
       team_roster = True
       try:  
        for n in cur_team_full['roster']['memberList']:
         cur_team_roster = {}
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
         all_teams.append(cur_team_roster)
       
  #      OLD METHOD
  #        try:
  #         self.cursor.execute(add_team_roster, cur_team_roster)
  #        except mysql.connector.Error as err:
  #         if err.errno != 1062:
  #          print "%s, Team: %s" % (err.msg, y)
  #        else:
  #         print "Updated Team-Roster"
       except:
        team_roster = False
        if feedback == "all":
         if (cur_team_full['status']=="DISBANDED"):
          print "No Team-Roster -- Team Disbanded"
         else:
          print "No Team-Roster"
     
     
       if team_roster == True:
        try:
         self.cursor.executemany(add_team_roster, all_teams)
   #       print test_team
        except mysql.connector.Error as err:
         if err.errno != 1062 or suppress_duplicates == False:
          if feedback != "silent":
           print "Error %s" % err.errno
        else:
         if feedback == "all":
          print "Updated Team-Roster"
     
      except:
       print "Error, no team information; %s" % (y) 
    

     if feedback == "all":
      print "Finished %s of %s" % (stop, len(team_ids))
     if feedback == "quiet" and stop == len(team_ids):
      print "Finished %s of %s" % (stop, len(team_ids))
    
     self.cnx.commit()
        
    if checkTeams==True:
     if feedback != "silent":
      print "Checking Teams"
     self.update_table("checkteams", feedback=feedback, suppress_duplicates = suppress_duplicates)
    

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
        teams = self.w.get_teams_for_summoners(ids)
      
       except riotwatcher.riotwatcher.LoLException as err:
  #       print str(err)
        if str(err) == "Game data not found":
         finished = True
        elif str(err) == "Too many requests" or str(err) == "Unauthorized" or str(err) == "Blacklisted key":
 #         print "New Key" 
         drop = False
         if str(err) == "Blacklisted key":
          if feedback == "all":
           print "Blacklisted key, using new key, dropping current."
          drop = True
         if str(err) == "Unauthorized":
          print "Unauthorized, using new key"
         if len(keys)==1:
          print "Too many requests, not enough keys."
          if hangwait == False:
           break 
          else:
           time.sleep(5)
         self.new_key(drop=drop)
        
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
     self.update_table("team", teamIds = team_ids, checkTeams = checkTeams, feedback=feedback, suppress_duplicates = suppress_duplicates)  
 
 
  if table=="all":
   self.update_table("challenger", feedback=feedback, queue=queue, suppress_duplicates = suppress_duplicates)
   self.update_table("master", feedback=feedback, queue=queue, suppress_duplicates = suppress_duplicates)
   self.update_table("team", checkTeams =True, feedback=feedback, suppress_duplicates = suppress_duplicates)
  
  if table=="match":
     
     
     
     

  self.cnx.commit()
 def __exit__(self):
  self.cnx.commit()
  self.cursor.close()
  self.cnx.close()
  if ssh == True:
   credentials.server.stop()



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


# update_table("checkteams", teamIds= False, hangwait=False)
# this checks the team table and grabs a list of all the ids. it then gets the league information for each team. 
# if you supply this with a list of teamIds it will just do those.
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

# update_table("match", matchIds=[], timeline=False, create=False)
# this function will import all non-timeline data from a given list of matchIds. if no matchIds are supplied, it will automatically search through the list of matchIds in 'team-history'
# timeline=TRUE will now import timeline data too



# update_table("membertiers", matchIds=[], create=False)
# this function is essentially the same as the 'checkteams' functionality however this will search a given match and scrape the league data for all the players in that match
# if you want to just do all the matches in the database (match table), don't set matchIds to anything.



## FOR ANY FUNCTION
# setting feedback="all" will print all errors, print a completion statement when a step is finished, and print updates
# setting feedback="quiet" will print only uncommon problem errors (duplicate entry errors are silenced), and will print completion statements when long steps are finished
# setting feedback="silent" will suppress all printing
# setting suppress_duplicates=True will suppress printing of duplicate entry errors. this only effects feedback="all" as the script overrides this setting for quiet and silent modes




## functions for actual use:
# update_table("iterate",iteratestart=10000, iterate=10000, checkTeams=True, suppress_duplicates=True)
# start_time = time.time()

# update_table("match", matchIds=[1976289359], suppress_duplicates=True)
# update_table("membertiers", matchIds=[2044253864,1976289359], suppress_duplicates=True)
# update_table("checkteams", feedback="all", suppress_duplicates=True)
# print "Took", time.time() - start_time, "to run"

# update_table("team", checkTeams=True, feedback="all", suppress_duplicates=True)
# 
# cursor.execute("SELECT gameId FROM team_history")
# matches= strip_to_list(cursor.fetchall())
# 
# 
# update_table("match", matchIds=matches, timeline=True, create=False, suppress_duplicates=True)
# update_table("membertiers", suppress_duplicates=True)



# update_table("checkteams", teamIds = ["TEAM-f8aac4c0-16de-11e5-87b6-c81f66dd45c9"])


# 
# cursor.execute("SELECT playerOrTeamID, queue FROM by_league LIMIT 10")
# test = cursor.fetchall()
# print test

   
   
# new_key()






### MAKE SURE ALL SCRIPT EXECUTIONS OCCUR BEFORE THE NEXT 5 LINES







##For personal reference, just to keep track of alterations i've made after table creation, all are reflected in the table creation now though. 
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
# cursor.execute("ALTER TABLE by_league DROP PRIMARY KEY, ADD CONSTRAINT id_queue PRIMARY KEY (`playerOrTeamId`, `queue`, `retrieved`)")
     
     
# cursor.execute("ALTER TABLE by_league MODIFY division varchar(5) NOT NULL")
# cursor.execute("DROP TABLE IF EXISTS by_league")
# cursor.execute("ALTER TABLE `by_league` ADD `retrieved` DATETIME NOT NULL")




#### FOR ANALYSIS
# cursor.execute("SELECT summonerId, matchId, teamId, winner, highestAchievedSeasonTier FROM match_participants")
# all_raw = cursor.fetchall()
# cursor.execute("SELECT t1.summonerId, t2.division, t2.league FROM match_participants AS t1 INNER JOIN by_league AS t2 ON t1.summonerId = t2.playerOrTeamId")
# people_raw = cursor.fetchall()
# 
# # people = {}
# people = []
# for x in people_raw:
#  people.append(x[0])
# 
# all = []
# for x in all_raw:
#  if x[0] in people:
#   all.append([x[1], x[2], x[3], people_raw[people.index(x[0])][1], people_raw[people.index(x[0])][2]])
#  else:
#   all.append([x[1], x[2], x[3], 0, x[4]])
#  
# 
# 
# 
# with open('/Users/Matthew/Documents/League of Legends/testfile.csv', 'wb+') as csvfile:
#     swriter = csv.writer(csvfile, delimiter=' ',
#                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
#     swriter.writerow(["matchId"] + ["teamId"] + ["winner"] + ["division"] + ["league"])
#     for x in all:
#      swriter.writerow(x)





# cursor.execute("SELECT summonerId, matchId, teamId, winner, highestAchievedSeasonTier, kills FROM match_participants")
# all_raw = cursor.fetchall()
# cursor.execute("SELECT t1.summonerId, t2.division, t2.league FROM match_participants AS t1 INNER JOIN by_league AS t2 ON t1.summonerId = t2.playerOrTeamId")
# people_raw = cursor.fetchall()
# 
# # people = {}
# people = []
# for x in people_raw:
#  people.append(x[0])
# 
# all = []
# for x in all_raw:
#  if x[0] in people:
#   all.append([x[1], x[2], x[3], x[5], people_raw[people.index(x[0])][1], people_raw[people.index(x[0])][2]])
#  else:
#   all.append([x[1], x[2], x[3], x[5], 0, x[4]])
#  
# 
# 
# 
# with open('/Users/Matthew/Documents/League of Legends/testfile.csv', 'wb+') as csvfile:
#     swriter = csv.writer(csvfile, delimiter=' ',
#                             quotechar='|', quoting=csv.QUOTE_MINIMAL)
#     swriter.writerow(["matchId"] + ["teamId"] + ["winner"] + ["kills"] + ["division"] + ["league"])
#     for x in all:
#      swriter.writerow(x)

