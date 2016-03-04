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
import curses

from utils import todict
import mysql.connector
from mysql.connector import errorcode


from time import sleep

import credentials

##making ssh option a command line option
import sys, getopt
import argparse
if __name__ == '__main__' :

 parser = argparse.ArgumentParser(description="LoL Scraper", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
 parser.add_argument("-s", dest="ssh", required=False, help="using ssh or not", metavar="ssh")
 args = parser.parse_args()
 ssh = args.ssh
 
 

def strip_to_list (data_raw):
 data = []
 for x in data_raw:
  for y in x:
   data.append(y)
 return data

class Scraper:
 
 
 def __init__(self, ssh=True, use_curses=False, rate_limiting=False):
  self.rate_limiting = rate_limiting
  if ssh == "True":
   ssh = True
  elif ssh == "False":
   ssh = False
  elif ssh != False and ssh != True:
   print "Not a valid value for ssh, defaulting to True."
   ssh = True
  if use_curses == "True":
   self.use_curses = True
  elif use_curses == "False":
   self.use_curses = False
  elif use_curses != False and use_curses != True:
   print "Not a valid value for use_curses, defaulting to False."
   self.use_curses = False
  elif use_curses == False or use_curses == True:
   self.use_curses = use_curses
  
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
  
  self.skipfiler = open('skiplist.tsv', "rb+")
  self.skiplist = self.skipfiler.read()
  self.skipfiler.close()
  self.old_count = 0
  if self.use_curses == True:
   self.stdscr = curses.initscr()
   self.stdscr.addstr(0, 0, "League of Legends Scraper", curses.A_BOLD)
   self.stdscr.refresh()
   self.msgs = []
   self.errormsg = []
  
  
  



 def wait(self):
  if self.rate_limiting == True:
   while not self.w.can_make_request():
    time.sleep(1)
    
 def write_to_skip(self, teams):
  with open('skiplist.tsv', "ab+") as skipfilew:
   for x in teams:
    skipfilew.write("%s\n" % (x))
  skipfilew.close()
 
  
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
     "  CONSTRAINT id_queue PRIMARY KEY (`playerOrTeamId`, `queue`, `retrieved`),"
     "  INDEX id_team (playerOrTeamId, team)"
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
    
   
  TABLES['individual_history'] = (
      "CREATE TABLE `individual_history` ("
      "  `summonerId` varchar(20) NOT NULL,"
      "		`championId`	int(4)		DEFAULT NULL	,"
      "  `lane` varchar(8) DEFAULT NULL,"
      "  `matchId` varchar(20) NOT NULL,"
      "  `platformId` varchar(6) DEFAULT NULL,"
      "  `queue` varchar(32) DEFAULT NULL,"
      "  `region` varchar(6) DEFAULT NULL,"
      "  `role` varchar(15) DEFAULT NULL,"
      "  `season` varchar(15) DEFAULT NULL,"
      "  `timestamp` BIGINT DEFAULT NULL,"
      "  CONSTRAINT player_game PRIMARY KEY (`summonerId`, `matchId`),"
      "  INDEX k_season (season)"
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
      "		`queueType`	varchar(30)		DEFAULT NULL	,"
      "		`region`	varchar(8)		DEFAULT NULL	,"
      "		`season`	varchar(20)		DEFAULT NULL	,"
      " PRIMARY KEY (`matchId`),"
      " INDEX k_season (season)"
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
      "		`summonerId`	varchar(20)		NOT NULL	,"
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
      "  INDEX (`matchId`,`eventId`),"
      "  FOREIGN KEY (`matchId`, `eventId`) "
      "     REFERENCES `match_timeline_events` (`matchId`,`eventId`) "
      "     ON UPDATE CASCADE ON DELETE CASCADE"
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

 def print_stuff(self, msg=None, header1 = False, header2 = False, clear = False, error = False, progress = False, override = False):
  feedback = self.feedback
  if msg is None:
    msg = " "
  if self.use_curses == True:
   if clear == True:
    self.stdscr.erase()
    self.stdscr.addstr(0, 0, "League of Legends Scraper", curses.A_BOLD)
   if feedback != "silent" and header1 == True:
    self.stdscr.erase()
    self.stdscr.addstr(0, 0, "League of Legends Scraper", curses.A_BOLD)
    self.stdscr.addstr(1, 1, msg) 
   elif (feedback != "silent" and header2 == True) or progress == True:
    if (progress == True and feedback == "all") or progress == False:
     self.stdscr.addstr(2, 1, msg)  
   elif feedback != "silent" and error == True:
    self.errormsg.append(msg)
    if len(self.errormsg) > 2:
     del self.errormsg[1]
    for x in self.errormsg:
     self.stdscr.addstr(9+self.msgs.index(x),2,x)
   if header1 == False and header2 == False and (feedback == "all" or (override == True and feedback != "silent")):
    self.msgs.append(msg)
    if len(self.msgs) > 5:
     del self.msgs[1]
    for x in self.msgs:
     self.stdscr.addstr(4+self.msgs.index(x),3,x)

   self.stdscr.refresh()
  elif self.use_curses == False:
   if feedback != "silent" and (header1 == True or header2 == True or error == True or override == True):
    print msg
   elif feedback == "all" and not (header1 == True or header2 == True or error == True or override == True):
    print msg

    
     
    
   
    
    

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
    self.print_stuff("Only one key.")
    if drop == True:
     self.print_stuff("Can't drop only key. Breaking.")
     stop
    self.key = keys[0]
 
 
 
 #  if riotwatcher.RiotWatcher(key).can_make_request():
  self.wait()
  self.w = riotwatcher.RiotWatcher(self.key, limits=(riotwatcher.RateLimit(1500,10), riotwatcher.RateLimit(90000,600)))
 
 #  print self.w.can_make_request()
 #  else: 
 #   if rate == len(keys):
 #    time.sleep(3)
 #   else:
 #    print "Rate Limit Reached"
 #    rate += 1
 #    self.new_key(t = key, rate=rate)
 
 def get_membertiers(self, matchIds=None, summonerIds=None, old_count = 0, full_count=None):
    add_league = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")

    summoner_ids_raw = [] 
    if matchIds:
     self.print_stuff("Extracting participant ids from %s matches" % len(matchIds))

     self.cursor.execute('SELECT DISTINCT(summonerId) FROM match_participants where matchId in ({0})'.format(', '.join(str(x) for x in matchIds)))
     summoner_ids_raw = self.cursor.fetchall()

     self.print_stuff("Finished extracting participants.")
     summoner_ids = [x[0] for x in summoner_ids_raw] 
    
    
  
#      for x in summoner_ids_raw:
#       for y in x:
#        summoner_ids.append(y)
     summoner_ids =  list(set(summoner_ids)-set(self.existing_entries))
   
    elif summonerIds:
     self.print_stuff("Using supplied list of summoners.")
     summoner_ids = summonerIds
     old_count = 0 
     summoner_ids =  list(set(summoner_ids)-set(self.existing_entries))
     full_count = len(summoner_ids)



    
    for x in xrange(0,(int(len(summoner_ids)/10)+1)):
     stop = ((x+1)*10)

     if x == int(len(summoner_ids)/10):
      stop = (len(summoner_ids))
     if stop == x*10:
      continue
    
 #     print summoner_ids[(x*10):stop]
    
     league_entries = self.get_leagues(team_ids=summoner_ids,x=x, stop=stop, key=self.key, unauthorized_cycle=False, team=False)

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
       self.print_stuff("%s - Member-Tiers" % err.msg, error = True)

     else:
      self.print_stuff("Updated Member-Tiers", override = True)
    
     self.cnx.commit()
     if stop==(len(summoner_ids)):
      self.print_stuff("Finished %s of %s" % (stop+old_count, full_count), header2 = True)
     else:
      self.print_stuff("Finished %s of %s" % (stop+1+old_count, full_count), progress = True)
     self.old_count = len(summoner_ids) + old_count
     self.cnx.commit() 
       
         
 def get_leagues(self, team_ids=None,x=None, stop=None, key=None, unauthorized_cycle=False, team=True):
  finished = False
  unauthorized_key = False
  while finished == False:
   err = []
   self.wait()
   try:
    if team==True:
     league_entries = self.w.get_league_entry(team_ids=team_ids[(x*10):stop]) if unauthorized_cycle==False else self.w.get_league_entry(team_ids=team_ids)
    else:
     league_entries = self.w.get_league_entry(summoner_ids=team_ids[(x*10):stop]) if unauthorized_cycle==False else self.w.get_league_entry(summoner_ids=team_ids)


   except riotwatcher.riotwatcher.LoLException as err:
    drop = False
    if str(err) == "Unauthorized" :

     
     self.print_stuff("Unauthorized, using new key") 
 # This to ensure that you only try one new key for unauthorized, just switch truth value
   
     unauthorized_key= (True if unauthorized_key == False else False)
 #         print unauthorized_key

 #       print str(err)
    if str(err) == "Too many requests" or str(err) == "Blacklisted key":
     if str(err) == "Blacklisted key":
      self.print_stuff("Blacklisted key, using new key, dropping current.")
      drop = True
     unauthorized_key=False
     
     if str(err) != "Unauthorized" and str(err) != "Too many requests" and str(err) != "Blacklisted key":
      
      self.print_stuff("Break, %s" % (str(err)), error = True)
      break 


   
 # Checks to make sure the error is not just coming from missing data     
    if str(err) == "Game data not found":
     
     self.print_stuff("No data, skipping %s" % (team_ids if unauthorized_cycle == True else team_ids[(x*10):stop]))
     league_entries = {}
     if unauthorized_cycle == True:
      self.write_to_skip(team_ids)
     else: 
      self.write_to_skip(team_ids[(x*10):stop])
     finished = True   
 # Basically this checks to see if unauthorized_key has been used and switched back to false. This should only happen if you have two unauthorized key changes in a row
    elif unauthorized_key == True or str(err) != "Unauthorized":
     self.print_stuff("New key assigned, %s" % str(err))
 #         print str(err)
    
     self.new_key(drop = drop)
    elif unauthorized_key == False and str(err) == "Unauthorized":
     league_entries = {}
     if unauthorized_cycle== False:
      self.print_stuff("Unauthorized, checking individual")
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
 
 
 
 
 def get_indhistory(self, summoner_id=None, season=None, end_time=None):
 
     add_history = ("INSERT IGNORE INTO individual_history"
             "(summonerId, championId, lane, matchId, platformId, queue, region, role, season, timestamp)" 
             "VALUES (%(summonerId)s, %(championId)s, %(lane)s, %(matchId)s, %(platformId)s, %(queue)s, %(region)s, %(role)s, %(season)s, %(timestamp)s)" )
     err = []
     finished = False
     while finished == False:
      self.wait()
      try:
       cur_matchlist = self.w.get_match_list(summoner_id, season=season ,end_time=end_time)
          
      
      except riotwatcher.riotwatcher.LoLException as err:
 #       print str(err)
       if str(err) == "Game data not found":
        finished = True
       elif str(err) == "Too many requests" or str(err) == "Unauthorized" or str(err) == "Blacklisted key":
#         print "New Key" 
        drop = False
        if str(err) == "Blacklisted key":
         self.print_stuff("Blacklisted key, using new key, dropping current.")
         drop = True
        if str(err) == "Unauthorized":
         self.print_stuff("Unauthorized, using new key")
        if len(keys)==1:
         self.print_stuff("Too many requests, not enough keys.", error=True)
         if hangwait == False:
          break 
         else:
          time.sleep(5)
        self.new_key(drop=drop)
       
       else:

        self.print_stuff("%s, Summoner: %s" % (str(err), summoner_id), error= True)
        break
      else:
       finished = True

     if err == "Game data not found":
       err = []
       return
     
     
     
     ind_history = True 
     all_history = []
     try:
      for n in cur_matchlist['matches']:
       cur_history = {}
       cur_history['summonerId'] = summoner_id
       cur_history['championId'] = n['champion']
       for z in ['lane', 'matchId', 'platformId', 'queue', 'region', 'role', 'season', 'timestamp']:
        cur_history[z] = n[z]
       all_history.append(cur_history)
     except:
      ind_history = False
      self.print_stuff("No Individual History")
     
     if ind_history == True:
      try:
       self.cursor.executemany(add_history, all_history)
 #       print test_team
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff( "Error %s : %s" % (err.errno,summoner_id), error = True)
      else:
       self.print_stuff("Updated Individual History")
       
        
      
   
     

 def update_table(self, table, queue="RANKED_TEAM_5x5", iteratestart=1, iterate=100, create=False, teamIds=False, matchIds=False, summonerIds=False, checkTeams= False, hangwait=False, feedback="all", suppress_duplicates = False, timeline = False, allow_updates=False, ignore_skiplist=False, just_teams = True, timeline_update=False, season=None, end_time=None):
  feedback = feedback.lower()
  if feedback != "all" and feedback != "quiet" and feedback != "silent":
   self.feedback="all"
   self.print_stuff("Invalid value for 'Feedback' option, reverting to default. All feedback will be shown.")
   feedback = "all"
  if feedback == "quiet" or feedback == "silent":
   suppress_duplicates = True
  self.feedback = feedback
  self.key = keys[0]
  self.new_key()

  if create== True:
   self.create_tables()

  if table=="challenger":
   add_challenger = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")
   self.print_stuff("Checking Challenger tier League API", header1 = True)
   self.wait()
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
     if suppress_duplicates == False :
      self.print_stuff("Error %s" % (err.errno), error=True)
    else:
     self.print_stuff("Error %s" % (err.errno), error=True)
         

    
   else:
    self.print_stuff("Updated Challenger", header2 = True)
  
   if err.errno == 1062:
    self.print_stuff("Finished Challenger", header2 = True)
   
  
  
  
  if table=="master":
   add_master = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")
   self.print_stuff("Checking Master tier League API", header1 = True)
   self.wait()
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
     if suppress_duplicates == False:
      self.print_stuff("Error %s" % (err.errno), error = True)
    else:
     self.print_stuff("Error %s" % (err.errno), error = True)
    
    
   else:

    self.print_stuff("Updated Master", header2 = True)

    
   if err.errno == 1062:
    self.print_stuff("Finished Master", header2 = True) 
    
  if table=="checkteams":
    add_league = ("INSERT IGNORE INTO by_league "
                "(isFreshBlood, division, isVeteran, wins, losses, playerOrTeamId, playerOrTeamName, isInactive, isHotStreak, leaguePoints, league, team, queue) "
                "VALUES (%(isFreshBlood)s, %(division)s, %(isVeteran)s, %(wins)s, %(losses)s, %(playerOrTeamId)s, %(playerOrTeamName)s, %(isInactive)s, %(isHotStreak)s, %(leaguePoints)s, %(league)s, %(team)s, %(queue)s)")

    self.print_stuff("Checking Teams on League API", header1 = True)
    self.cursor.execute("SELECT playerOrTeamID, queue FROM by_league")
    existing_entries = self.cursor.fetchall()
   
    if(teamIds==False):
     self.print_stuff("No list of team ids, defaulting to search by_league")
     self.cursor.execute("SELECT fullId FROM team" )    
   

     team_ids_raw = [] 
     team_ids_raw = self.cursor.fetchall()
   
     team_ids = [x[0] for x in team_ids_raw] 
   
#      for x in team_ids_raw:
#       for y in x:
#        team_ids.append(y)

    else:
     self.print_stuff("Given list of team ids.")
     team_ids = teamIds
 
    if ignore_skiplist == False:
     team_ids = [x for x in team_ids if x not in self.skiplist]
    for x in xrange(0,(int(len(team_ids)/10)+1)):
     stop = ((x+1)*10)

     if x == int(len(team_ids)/10):
      stop = (len(team_ids))
     if stop == x*10:
      continue
  
 #     print "%s, %s" % ((x*10), stop)


     league_entries = self.get_leagues(team_ids=team_ids,x=x, stop=stop, key=self.key, unauthorized_cycle=False, team=True)
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
       self.print_stuff(err.errno, error = True)


     else:
      self.print_stuff("Updated By-League", override = True)
       
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
      self.print_stuff("Finished %s of %s" % (stop, len(team_ids)), header2=True)
     else:
      self.print_stuff("Finished %s of %s" % (stop+1, len(team_ids)), progress=True)
        
  if table=="membertiers":
    self.print_stuff("Updating Member-tiers.", header1 = True)
    if matchIds == False:
       self.print_stuff("No matches given, searching all summoners from all matches.")
       self.cursor.execute("SELECT DISTINCT(summonerId) FROM match_participants")
       sum_id_raw = self.cursor.fetchall()
       summonerIds= [x[0] for x in sum_id_raw]
       
#           old method
#      self.print_stuff("No matches given, using all.")
#      self.cursor.execute("SELECT matchId FROM matches")
#      matchIds= strip_to_list(self.cursor.fetchall())
#      
#      self.cursor.execute("SELECT count(summonerId) FROM match_participants")
#      fullcount=self.cursor.fetchall()
#      fullcount=fullcount[0][0]
    else:
     self.cursor.execute('SELECT count(summonerId) FROM match_participants where matchId in ({0})'.format(', '.join(str(x) for x in matchIds)))
     fullcount=self.cursor.fetchall()
     fullcount=fullcount[0][0]
    
    self.old_count = 0 
    
    self.existing_entries = []
    
    self.cursor.execute("SELECT DISTINCT(playerOrTeamId) from by_league where team = 0")
    existing_entries_raw = self.cursor.fetchall()

    
    if allow_updates == False:
     self.existing_entries = [x[0] for x in existing_entries_raw]
#      for x in existing_entries_raw:
#       for y in x:
#        self.existing_entries.append(y)
    if ignore_skiplist == False:
     self.existing_entries.append(x for x in self.skiplist)

    
    if matchIds == False: 
     self.get_membertiers(summonerIds=summonerIds)
     
    else:
     while len(matchIds) > 250:
      self.get_membertiers(matchIds = matchIds[:250], old_count = self.old_count, full_count=fullcount)
      del matchIds[:250]

    
    
     self.get_membertiers(matchIds, old_count = self.old_count, full_count=fullcount)
  
  if table=="individualhistory":
   self.print_stuff("Updating Individual History", header1 = True)
   if summonerIds == False:         
    if just_teams == True:
     self.print_stuff("No IDs supplied, searching teams.")
     self.cursor.execute("SELECT DISTINCT(playerId) FROM team_roster")

    else:
     self.print_stuff("No IDs supplied, searching matches.")
     self.cursor.execute("SELECT DISTINCT(summonerId) FROM match_participants")
    summoner_ids_raw = self.cursor.fetchall()
    summoner_ids = [x[0] for x in summoner_ids_raw] 
#     for x in summoner_ids_raw:
#      for y in x:
#       summoner_ids.append(y)
   else:
    self.print_stuff("IDs supplied.")

    summoner_ids = summonerIds   
   
   if allow_updates == False:
    self.cursor.execute("SELECT DISTINCT(summonerId) FROM individual_history")
    existing_ids_raw = self.cursor.fetchall()
    existing_ids = [x[0] for x in existing_ids_raw]
    summoner_ids = list(set(summoner_ids)-set(existing_ids))
    
   curcount = 0
   for x in summoner_ids:
     
    self.get_indhistory(x, season=season, end_time=end_time)
    if summoner_ids.index(x) % 10 == 0:
     self.print_stuff("Finished %s of %s" % (summoner_ids.index(x), len(summoner_ids)), progress=True)
    
    
   self.print_stuff("Finished %s of %s" % (summoner_ids.index(x), len(summoner_ids)), header2=True)
   
     
   
        
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
    self.print_stuff("Updating Team Tables.", header1 = True)
    if(teamIds==False):
     self.print_stuff("No list of team ids, defaulting to search by_league")
     self.cursor.execute("SELECT DISTINCT(playerOrTeamId) FROM by_league WHERE team = True" )         
   

     team_ids_raw = self.cursor.fetchall()
   
     team_ids = [x[0] for x in team_ids_raw] 
   
#      for x in team_ids_raw:
#       for y in x:
#        team_ids.append(y)
     
    else:
     self.print_stuff("Given list of team ids.")
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
      self.wait()
      try: 
       teams_data = self.w.get_teams(team_ids[(x*10):stop])
     
      except riotwatcher.riotwatcher.LoLException as err:
       if str(err) == "Too many requests" or str(err) == "Service unavailable" or str(err) == "Unauthorized" or str(err) == "Internal server error" or str(err) == "Blacklisted key":
 #         print "New Key" 
         drop = False
         if str(err) == "Blacklisted key":
          self.print_stuff("Blacklisted key, using new key, dropping current.")
          drop = True

         if str(err) == "Service unavailable" and service != 10:
          self.print_stuff("Service unavailable, using new key")
          service += 1
         if str(err) == "Unauthorized" or "Internal server error":
          self.print_stuff("Unauthorized, using new key")
         if len(keys)==1:
          self.print_stuff("Too many requests, not enough keys.")
          if hangwait == False:
           break 
          else:
           time.sleep(5)
         self.new_key(drop = drop)
        
       else:
        self.print_stuff("%s, Teams: %s" % (str(err), team_ids[(x*10):stop]), error= True)
        break
      except:
       self.print_stuff("Other Error Requesting Teams",error=True)
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
         self.print_stuff("Error, team stats messed up", error = True)
      
        try:
         self.cursor.execute(add_team, cur_team)
        except mysql.connector.Error as err:
         if err.errno != 1062 or suppress_duplicates == False:
          self.print_stuff("%s, Team: %s" % (err.errno, y), error= True)
   #       print add_team % cur_team
        else:
         self.print_stuff("Updated Team", override = True)

     
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
        self.print_stuff( "No Team-History")


       if team_history == True:
        try:
         self.cursor.executemany(add_team_history, all_teams)
   #       print test_team
        except mysql.connector.Error as err:
         if err.errno != 1062 or suppress_duplicates == False:
          self.print_stuff("Error %s" % err.errno, error = True)
        else:
         self.print_stuff("Updated Team-History")
      
      
      

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
        if (cur_team_full['status']=="DISBANDED"):
         self.print_stuff("No Team-Roster -- Team Disbanded")
        else:
         self.print_stuff("No Team-Roster")
     
     
       if team_roster == True:
        try:
         self.cursor.executemany(add_team_roster, all_teams)
   #       print test_team
        except mysql.connector.Error as err:
         if err.errno != 1062 or suppress_duplicates == False:
          self.print_stuff("Error %s" % err.errno, error = True)
        else:
         self.print_stuff("Updated Team-Roster")
     
      except:
       self.print_stuff("Error, no team information; %s" % (y), error=True)
    

     self.print_stuff("Finished %s of %s" % (stop, len(team_ids)), progress=True)
     if stop == len(team_ids):
      self.print_stuff("Finished %s of %s" % (stop, len(team_ids)),  header2=True)
    
     self.cnx.commit()
        
    if checkTeams==True:
     self.print_stuff("Checking Teams")
     self.update_table("checkteams", feedback=feedback, suppress_duplicates = suppress_duplicates, ignore_skiplist=ignore_skiplist, allow_updates=allow_updates)
    

  if table=="iterate":
     self.print_stuff("Iterating through possible summonerIds.", header1 = True)
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
       self.wait()
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
          self.print_stuff("Blacklisted key, using new key, dropping current.")
          drop = True
         if str(err) == "Unauthorized":
          self.print_stuff("Unauthorized, using new key")
         if len(keys)==1:
          self.print_stuff("Too many requests, not enough keys.", error=True)
          if hangwait == False:
           break 
          else:
           time.sleep(5)
         self.new_key(drop=drop)
        
        else:

         self.print_stuff("%s, Team: %s" % (str(err), ids), error= True)
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
      self.print_stuff("Finished %s of %s, %s teams found." % (int(stop-iteratestart), int(iterate), len(team_ids)), progress = True)
     self.print_stuff("Updating Team Table", header2 = True)
     self.update_table("team", teamIds = team_ids, checkTeams = checkTeams, feedback=feedback, suppress_duplicates = suppress_duplicates, allow_updates = allow_updates, ignore_skiplist=ignore_skiplist)  
 
 
  if table=="all":
   self.update_table("challenger", feedback=feedback, queue= queue, suppress_duplicates = suppress_duplicates)
   self.update_table("master", feedback=feedback, queue = queue, suppress_duplicates = suppress_duplicates)
   self.update_table("team", checkTeams =True, feedback=feedback, suppress_duplicates = suppress_duplicates, allow_updates = allow_updates, ignore_skiplist=ignore_skiplist)
  
  if table=="match":
    add_match = ("INSERT IGNORE INTO matches "
               "(mapId, matchCreation, matchDuration, matchId, matchMode, matchType, matchVersion, platformId, queueType, region, season) " 
               "VALUES (%(mapId)s, %(matchCreation)s, %(matchDuration)s, %(matchId)s, %(matchMode)s, %(matchType)s, %(matchVersion)s, %(platformId)s, %(queueType)s, %(region)s, %(season)s)")

    add_match_participants = ("INSERT IGNORE INTO match_participants "
               "(matchId, championId, highestAchievedSeasonTier, participantId, profileIcon, matchHistoryUri, summonerName, summonerId, spell1Id, spell2Id, assists, champLevel, combatPlayerScore, deaths, doubleKills, firstBloodAssist, firstBloodKill, firstInhibitorAssist, firstInhibitorKill, firstTowerAssist, firstTowerKill, goldEarned, goldSpent, inhibitorKills, item0, item1, item2, item3, item4, item5, item6, killingSprees, kills, largestCriticalStrike, largestKillingSpree, largestMultiKill, magicDamageDealt, magicDamageDealtToChampions, magicDamageTaken, minionsKilled, neutralMinionsKilled, neutralMinionsKilledEnemyJungle, neutralMinionsKilledTeamJungle, nodeCapture, nodeCaptureAssist, nodeNeutralize, nodeNeutralizeAssist, objectivePlayerScore, pentaKills, physicalDamageDealt, physicalDamageDealtToChampions, physicalDamageTaken, quadrakills, sightWardsBoughtInGame, teamObjective, totalDamageDealt, totalDamageDealtToChampions, totalDamageTaken, totalHeal, totalPlayerScore, totalScoreRank, totalTimeCrowdControlDealt, totalUnitsHealed, towerKills, tripleKills, trueDamageDealt, trueDamageDealtToChampions, trueDamageTaken, unrealKills, visionWardsBoughtInGame, wardsKilled, wardsPlaced, winner, teamId, lane, role) " 
               "VALUES (%(matchId)s, %(championId)s, %(highestAchievedSeasonTier)s,  %(participantId)s, %(profileIcon)s, %(matchHistoryUri)s, %(summonerName)s, %(summonerId)s, %(spell1Id)s, %(spell2Id)s, %(assists)s, %(champLevel)s, %(combatPlayerScore)s, %(deaths)s, %(doubleKills)s, %(firstBloodAssist)s, %(firstBloodKill)s, %(firstInhibitorAssist)s, %(firstInhibitorKill)s, %(firstTowerAssist)s, %(firstTowerKill)s, %(goldEarned)s, %(goldSpent)s, %(inhibitorKills)s, %(item0)s, %(item1)s, %(item2)s, %(item3)s, %(item4)s, %(item5)s, %(item6)s, %(killingSprees)s, %(kills)s, %(largestCriticalStrike)s, %(largestKillingSpree)s, %(largestMultiKill)s, %(magicDamageDealt)s, %(magicDamageDealtToChampions)s, %(magicDamageTaken)s, %(minionsKilled)s, %(neutralMinionsKilled)s, %(neutralMinionsKilledEnemyJungle)s, %(neutralMinionsKilledTeamJungle)s, %(nodeCapture)s, %(nodeCaptureAssist)s, %(nodeNeutralize)s, %(nodeNeutralizeAssist)s, %(objectivePlayerScore)s, %(pentaKills)s, %(physicalDamageDealt)s, %(physicalDamageDealtToChampions)s, %(physicalDamageTaken)s, %(quadrakills)s, %(sightWardsBoughtInGame)s, %(teamObjective)s, %(totalDamageDealt)s, %(totalDamageDealtToChampions)s, %(totalDamageTaken)s, %(totalHeal)s, %(totalPlayerScore)s, %(totalScoreRank)s, %(totalTimeCrowdControlDealt)s, %(totalUnitsHealed)s, %(towerKills)s, %(tripleKills)s, %(trueDamageDealt)s, %(trueDamageDealtToChampions)s, %(trueDamageTaken)s, %(unrealKills)s, %(visionWardsBoughtInGame)s, %(wardsKilled)s, %(wardsPlaced)s, %(winner)s, %(teamId)s, %(lane)s, %(role)s)")

    add_match_participant_rune = ("INSERT IGNORE INTO match_participant_runes "
               "(matchId, summonerId, rank, runeId)"
               "VALUES (%(matchId)s, %(summonerId)s, %(rank)s, %(runeId)s)")
              
    add_match_participant_mastery = ("INSERT IGNORE INTO match_participant_masteries "
               "(matchId, summonerId, rank, masteryId)"
               "VALUES (%(matchId)s, %(summonerId)s, %(rank)s, %(masteryId)s)")

    add_match_participant_delta = ("INSERT IGNORE INTO match_participant_deltas "
               "(matchId, summonerId, deltaName, deltaTimeframe, value)"
               "VALUES (%(matchId)s, %(summonerId)s, %(deltaName)s, %(deltaTimeframe)s, %(value)s)")
                
    add_match_teams = ("INSERT IGNORE INTO match_teams "
                "(matchId, teamId, baronKills, dominionVictoryScore, dragonKills, firstBaron, firstBlood, firstDragon, firstInhibitor, firstRiftHerald, firstTower, inhibitorKills, riftHeraldKills, towerKills, vilemawKills, winner) " 
                "VALUES (%(matchId)s, %(teamId)s, %(baronKills)s, %(dominionVictoryScore)s, %(dragonKills)s, %(firstBaron)s, %(firstBlood)s, %(firstDragon)s, %(firstInhibitor)s, %(firstRiftHerald)s, %(firstTower)s, %(inhibitorKills)s, %(riftHeraldKills)s, %(towerKills)s, %(vilemawKills)s, %(winner)s	)")
   
    add_match_bans = ("INSERT IGNORE INTO match_team_bans "
                "(matchId, teamId, pickTurn, championId) " 
                "VALUES (%(matchId)s, %(teamId)s, %(pickTurn)s, %(championId)s)")
               
    add_match_timeline = ("INSERT IGNORE INTO match_timeline "
                "(matchId, summonerId, timestamp, currentGold, positionX, positionY, minionsKilled, level, jungleMinionsKilled, totalGold, dominionScore, participantId, xp, teamScore, timelineInterval) "
                "VALUES (%(matchId)s, %(summonerId)s, %(timestamp)s, %(currentGold)s, %(positionX)s, %(positionY)s, %(minionsKilled)s, %(level)s, %(jungleMinionsKilled)s, %(totalGold)s, %(dominionScore)s, %(participantId)s, %(xp)s, %(teamScore)s, %(timelineInterval)s )")
   
    add_match_timeline_event =  ("INSERT IGNORE INTO match_timeline_events "
                "(eventId, matchId, summonerId, timelineTimestamp, eventTimestamp, ascendedType, assistingParticipants, buildingType, creatorId, eventType, itemAfter, itemBefore, itemId, killerId, laneType, levelUpType, monsterType, pointCaptured, positionX, positionY, skillSlot, teamId, towerType, victimId, wardType) "
                "VALUES (%(eventId)s, %(matchId)s, %(summonerId)s, %(timelineTimestamp)s, %(eventTimestamp)s, %(ascendedType)s, %(assistingParticipants)s, %(buildingType)s, %(creatorId)s, %(eventType)s, %(itemAfter)s, %(itemBefore)s, %(itemId)s, %(killerId`)s, %(laneType)s, %(levelUpType)s, %(monsterType)s, %(pointCaptured)s, %(positionX)s, %(positionY)s, %(skillSlot)s, %(teamId)s, %(towerType)s, %(victimId)s, %(wardType)s )")
  
    add_match_timeline_event_assist = ("INSERT IGNORE INTO match_timeline_events_assist "
                "(eventId, matchId, assistId)"
                "VALUES (%(eventId)s, %(matchId)s, %(assistId)s)")

     

    self.print_stuff("Updating Match Tables.", header1 = True)

    self.cursor.execute("SELECT matchId FROM matches")
    existing_matches_raw = self.cursor.fetchall()
    existing_matches = [x[0] for x in existing_matches_raw]
#     for x in existing_matches_raw:
#      for y in x:
#       existing_matches.append(y)




#     for x in existing_timelines_raw:
#      for y in x:
#       existing_timelines.append(y)
      
    if timeline_update == True:
     self.cursor.execute("SELECT DISTINCT(matchId) FROM match_timeline")
     existing_timelines_raw = self.cursor.fetchall()
     existing_timelines = [x[0] for x in existing_timelines_raw]
     self.print_stuff("Overriding matchIds, updating timelines for existing matches.")     
     match_ids = list(set(existing_matches)-set(existing_timelines))
     
    elif(matchIds==False):
     self.print_stuff("No list of match ids, defaulting to search team_history.")
     self.cursor.execute("SELECT gameId FROM team_history" )         
     match_ids_raw = self.cursor.fetchall()
   
     match_ids = [x[0] for x in match_ids_raw] 
     match_ids = list(set(match_ids)-set(existing_matches))
   
#      for x in match_ids_raw:
#       for y in x:
#        match_ids.append(y)
     
    else:
     self.print_stuff("Given list of match ids.")
     match_ids = matchIds
     match_ids = list(set(match_ids)-set(existing_matches))
 
 #    print match_ids
    teams_data = []
    
    for x in match_ids:
     finished = False 
     cur_match_raw = []

     while finished == False:
      self.wait()
      try: 
       cur_match_raw = self.w.get_match( x, region=None, include_timeline=timeline)
     
      except riotwatcher.riotwatcher.LoLException as err:
       if str(err) == "Too many requests" or str(err) == "Unauthorized" or str(err) == "Blacklisted key":
 #         print "New Key" 
         drop = False
         if str(err) == "Blacklisted key":
          self.print_stuff("Blacklisted key, using new key, dropping current.")
          drop = True
         if str(err) == "Unauthorized":
          self.print_stuff("Unauthorized, using new key")
         if len(keys)==1:
          self.print_stuff("Too many requests, not enough keys.", error=True)
          if hangwait == False:
           break 
          else:
           time.sleep(5)
         self.new_key(drop=drop)
        
       else:
        self.print_stuff("%s, Match: %s -- Request" % (str(err), x),error=True)
        break
 #      except:
 #       print "Other Error"
      
      else:
       finished = True
    
    
     if cur_match_raw:
     
      
      cur_match = {}
      
      for y in ["mapId", "matchCreation", "matchDuration", "matchId", "matchMode", "matchType", "matchVersion", "platformId", "queueType", "region", "season"]:
       try:
        cur_match[y] = cur_match_raw[y]

       except:
        cur_match[y] = None
    
  #     print cur_match
      try:
       self.cursor.execute(add_match, cur_match)
      except mysql.connector.Error as err:

        if err.errno != 1062 or suppress_duplicates == False:
         self.print_stuff("%s, Match: %s -- Match" % (err.errno, x), error= True)
  #       print add_team % cur_team
      else:
        self.print_stuff("Updated Match")
      all_match_teams = []
      all_match_bans = []
    
  #     print cur_match_raw['teams']
      for y in cur_match_raw['teams']:
       cur_match_teams = {}
       for z in ("bans", "matchId", "teamId", "baronKills", "dominionVictoryScore", "dragonKills", "firstBaron", "firstBlood", "firstDragon", "firstInhibitor", "firstRiftHerald", "firstTower", "inhibitorKills", "riftHeraldKills", "towerKills", "vilemawKills", "winner"):
        if z == "bans" and "bans" in y:
         for s in y['bans']:
          cur_match_bans = {}
          for t in s:
           cur_match_bans[t] = s[t]
          cur_match_bans['matchId'] = x
          cur_match_bans['teamId'] = y['teamId']
          all_match_bans.append(cur_match_bans)

     
        else:
         try:
          cur_match_teams[z] = y[z]
         except:
          cur_match_teams[z] = None
     
       cur_match_teams['matchId'] = x
       all_match_teams.append(cur_match_teams)
      
      try:
       self.cursor.executemany(add_match_teams, all_match_teams)
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff("%s, Match: %s -- Teams" % (err.errno, x), error=True)
      else:
       self.print_stuff("Updated Match-Teams")
   

      try:
       self.cursor.executemany(add_match_bans, all_match_bans)
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff("%s, Match: %s -- Bans" % (err.errno, x), error=True)
      else:
       self.print_stuff("Updated Match-Bans")
      
       
           
  #     print cur_match_raw
      cur_match_participants_raw = cur_match_raw["participants"]
    
    
     if cur_match_raw:
      cur_match_pi = {}
      for y in cur_match_raw["participantIdentities"]:
       cur_match_pi[y["participantId"]] = y["player"]
    
    
      
      
      
      
      
     if timeline == True and cur_match_raw:  
      if "timeline" in cur_match_raw:
       cur_match_timeline_raw = cur_match_raw["timeline"]   
       intervals = cur_match_timeline_raw["frameInterval"]
    
       for y in cur_match_timeline_raw["frames"]:

        cur_timeline = []


   #      print cur_time
   #      print y
        for z in y["participantFrames"]:
         cur_participant_timeline = {}
      
   #       print "z %s" % z
         for s in ["currentGold", "position", "minionsKilled", "level", "jungleMinionsKilled", "totalGold", "dominionScore", "participantId", "xp", "teamScore", "timelineInterval"]:
          if s != "position":
           try:
            cur_participant_timeline[s] = y["participantFrames"][z][s]
           except:
            cur_participant_timeline[s] = None
          else:
           try:
            cur_participant_timeline['positionX'] = y["participantFrames"][z][s]["x"]
           except:
            cur_participant_timeline['positionX'] = None
           try:
            cur_participant_timeline['positionY'] = y["participantFrames"][z][s]["y"]
           except:
            cur_participant_timeline['positionY'] = None
        
         cur_participant_timeline['matchId'] = x
         cur_participant_timeline['summonerId'] = cur_match_pi[int(z)]["summonerId"]
         cur_participant_timeline['timestamp'] = y["timestamp"]
         cur_participant_timeline['timelineInterval'] = intervals
         cur_timeline.append(cur_participant_timeline)
      
     
   #      timeline_events = []
  #       print x
        if "events" in y:
         timeline_events = []
         assists = []

         for z in y["events"]:
          cur_timeline_event = {}
   #        print z

          for s in ["ascendedType", "assistingParticipants", "buildingType", "creatorId", "eventType", "itemAfter", "itemBefore", "itemId", "killerId`", "laneType", "levelUpType", "monsterType", "pointCaptured", "positionX", "positionY", "skillSlot", "teamId", "towerType", "victimId", "wardType"]:
        
       
           if s=="assistingParticipants":
        
            if "assistingParticipantIds" in z:
             cur_timeline_event["assistingParticipants"]=True
             for t in z["assistingParticipantIds"]:
              cur_assists = {}
              cur_assists["eventId"] = "%s - %s" % (cur_match_timeline_raw["frames"].index(y),  y["events"].index(z))
              cur_assists["matchId"] = x
              cur_assists["assistId"] = cur_match_pi[t]["summonerId"]
              assists.append(cur_assists)

          
          
            else:
             cur_timeline_event["assistingParticipants"]=False
           elif s=="creatorId" or s=="killerId" or s=="victimId":
            try: 
             z[s]
            except:
             cur_timeline_event[s] = None
            else:
             if z[s] == 0:
              if s == "killerId":
               cur_timeline_event[s] = "minion"
              if s == "creatorId":
  #              we're not sure why creatorId would be set to 0, and no one online is 100% sure either. 
               cur_timeline_event[s] = "undefined"
             else:
  #             print z
              cur_timeline_event[s] = cur_match_pi[z[s]]["summonerId"]
           elif s == "position":
            try:
             cur_timeline_event['positionX'] = z[s]["x"]
            except:
             cur_timeline_event['positionX'] = None
            try:
             cur_timeline_event['positionY'] = z[s]["y"]
            except:
             cur_timeline_event['positionY'] = None
           else:
            try:
             z[s]
            except:
             cur_timeline_event[s] = None
            else:
             cur_timeline_event[s] = z[s]
          cur_timeline_event["eventId"] = "%s - %s" % (cur_match_timeline_raw["frames"].index(y),  y["events"].index(z))
          cur_timeline_event["matchId"] = x
          if "participantId" in z:
           if z["participantId"] != 0:
            cur_timeline_event["summonerId"] =  cur_match_pi[z["participantId"]]["summonerId"]
           else:
            cur_timeline_event["summonerId"] = None
          else:
           cur_timeline_event["summonerId"] = None
          cur_timeline_event["timelineTimestamp"] = y["timestamp"]
          cur_timeline_event["eventTimestamp"] = z["timestamp"]
          timeline_events.append(cur_timeline_event)
       
   #        print cur_timeline_event

   #       print timeline_events 

         if assists != []:
          try:
   #         print assists
           self.cursor.executemany(add_match_timeline_event_assist, assists)

          except mysql.connector.Error as err:
           if err.errno != 1062 or suppress_duplicates == False:
            self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline-Events" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)),error=True)
          else:
           self.print_stuff("Updated Timeline-Assists")
         try:
          self.cursor.executemany(add_match_timeline_event, timeline_events)

         except mysql.connector.Error as err:
          if err.errno != 1062 or suppress_duplicates == False:
           self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline-Events" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)), error=True)
         else:
          self.print_stuff("Updated Timeline-Events")
    #       stop
       
        
        try:
         self.cursor.executemany(add_match_timeline, cur_timeline)
        except mysql.connector.Error as err:
         if err.errno != 1062 or suppress_duplicates == False:
          self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)), error=True)
        else:
         self.print_stuff("Updated Timeline")
      
      
      
      
   #       if killerid == 0, Minion

       self.print_stuff("Finished Timeline")
      else:
       self.print_stuff("No Timeline Data; %s" % (x))
    
     if cur_match_raw:
      for y in cur_match_participants_raw:
       cur_match_participant = {}
  #      print y
  #      break
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
            self.cursor.execute(add_match_participant_rune, cur_rune)
           except mysql.connector.Error as err:
            if err.errno != 1062 or suppress_duplicates == False:
             self.print_stuff("%s, Match: %s, Player: %s -- Rune" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]),error=True)
   #          print add_team % cur_team
           else:
            self.print_stuff("Updated Rune")
         
         
         elif z == "masteries": 
          cur_mastery = {}  
          cur_mastery["matchId"] = x
          cur_mastery["summonerId"] = cur_match_pi[y["participantId"]]["summonerId"]
          for r in y[z]:
           for r2 in r:
            cur_mastery[r2] = r[r2]
          
           try:
            self.cursor.execute(add_match_participant_mastery, cur_mastery)
           except mysql.connector.Error as err:
            if err.errno != 1062 or suppress_duplicates == False:
             self.print_stuff("%s, Match: %s, Player: %s -- Mastery" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]),error=True)
   #          print add_team % cur_team
           else:
            self.print_stuff("Updated Mastery")     
       
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
              self.cursor.execute(add_match_participant_delta, cur_delta)
             except mysql.connector.Error as err:
              if err.errno != 1062 or suppress_duplicates == False:
               self.print_stuff("%s, Match: %s, Player: %s -- Delta" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]),error=True)
     #          print add_team % cur_team
             else:
              self.print_stuff("Updated Participant Deltas") 
       
         else:
          self.print_stuff("Broken: %s" % z)
       
       
        else:
         cur_match_participant[z] = y[z]
     
  #      print cur_match_participant_raw     
  #      if feedback == "all":
  #       print cur_match_participant["nodeCapture"]
       try: 
        self.cursor.execute(add_match_participants, cur_match_participant)
  #       print "Try"
     
       except mysql.connector.Error as err:
        if err.errno != 1062 or suppress_duplicates == False:
         self.print_stuff("%s, Match: %s, Player: %s -- Participant" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]), error=True)
  #       print add_team % cur_team
       else:
        self.print_stuff("Updated Match-Participant")
     else:
      if suppress_duplicates == False:
       self.print_stuff("Duplicate Match; %s" % (x))

     
     
     
    
       
     self.print_stuff("Finished %s of %s; %s" % (match_ids.index(x)+1,len(match_ids),x), header2=True)
     self.cnx.commit()
     
  if table=="match2":
    add_match = ("INSERT IGNORE INTO matches "
               "(mapId, matchCreation, matchDuration, matchId, matchMode, matchType, matchVersion, platformId, queueType, region, season) " 
               "VALUES (%(mapId)s, %(matchCreation)s, %(matchDuration)s, %(matchId)s, %(matchMode)s, %(matchType)s, %(matchVersion)s, %(platformId)s, %(queueType)s, %(region)s, %(season)s)")

    add_match_participants = ("INSERT IGNORE INTO match_participants "
               "(matchId, championId, highestAchievedSeasonTier, participantId, profileIcon, matchHistoryUri, summonerName, summonerId, spell1Id, spell2Id, assists, champLevel, combatPlayerScore, deaths, doubleKills, firstBloodAssist, firstBloodKill, firstInhibitorAssist, firstInhibitorKill, firstTowerAssist, firstTowerKill, goldEarned, goldSpent, inhibitorKills, item0, item1, item2, item3, item4, item5, item6, killingSprees, kills, largestCriticalStrike, largestKillingSpree, largestMultiKill, magicDamageDealt, magicDamageDealtToChampions, magicDamageTaken, minionsKilled, neutralMinionsKilled, neutralMinionsKilledEnemyJungle, neutralMinionsKilledTeamJungle, nodeCapture, nodeCaptureAssist, nodeNeutralize, nodeNeutralizeAssist, objectivePlayerScore, pentaKills, physicalDamageDealt, physicalDamageDealtToChampions, physicalDamageTaken, quadrakills, sightWardsBoughtInGame, teamObjective, totalDamageDealt, totalDamageDealtToChampions, totalDamageTaken, totalHeal, totalPlayerScore, totalScoreRank, totalTimeCrowdControlDealt, totalUnitsHealed, towerKills, tripleKills, trueDamageDealt, trueDamageDealtToChampions, trueDamageTaken, unrealKills, visionWardsBoughtInGame, wardsKilled, wardsPlaced, winner, teamId, lane, role) " 
               "VALUES (%(matchId)s, %(championId)s, %(highestAchievedSeasonTier)s,  %(participantId)s, %(profileIcon)s, %(matchHistoryUri)s, %(summonerName)s, %(summonerId)s, %(spell1Id)s, %(spell2Id)s, %(assists)s, %(champLevel)s, %(combatPlayerScore)s, %(deaths)s, %(doubleKills)s, %(firstBloodAssist)s, %(firstBloodKill)s, %(firstInhibitorAssist)s, %(firstInhibitorKill)s, %(firstTowerAssist)s, %(firstTowerKill)s, %(goldEarned)s, %(goldSpent)s, %(inhibitorKills)s, %(item0)s, %(item1)s, %(item2)s, %(item3)s, %(item4)s, %(item5)s, %(item6)s, %(killingSprees)s, %(kills)s, %(largestCriticalStrike)s, %(largestKillingSpree)s, %(largestMultiKill)s, %(magicDamageDealt)s, %(magicDamageDealtToChampions)s, %(magicDamageTaken)s, %(minionsKilled)s, %(neutralMinionsKilled)s, %(neutralMinionsKilledEnemyJungle)s, %(neutralMinionsKilledTeamJungle)s, %(nodeCapture)s, %(nodeCaptureAssist)s, %(nodeNeutralize)s, %(nodeNeutralizeAssist)s, %(objectivePlayerScore)s, %(pentaKills)s, %(physicalDamageDealt)s, %(physicalDamageDealtToChampions)s, %(physicalDamageTaken)s, %(quadrakills)s, %(sightWardsBoughtInGame)s, %(teamObjective)s, %(totalDamageDealt)s, %(totalDamageDealtToChampions)s, %(totalDamageTaken)s, %(totalHeal)s, %(totalPlayerScore)s, %(totalScoreRank)s, %(totalTimeCrowdControlDealt)s, %(totalUnitsHealed)s, %(towerKills)s, %(tripleKills)s, %(trueDamageDealt)s, %(trueDamageDealtToChampions)s, %(trueDamageTaken)s, %(unrealKills)s, %(visionWardsBoughtInGame)s, %(wardsKilled)s, %(wardsPlaced)s, %(winner)s, %(teamId)s, %(lane)s, %(role)s)")

    add_match_participant_rune = ("INSERT IGNORE INTO match_participant_runes "
               "(matchId, summonerId, rank, runeId)"
               "VALUES (%(matchId)s, %(summonerId)s, %(rank)s, %(runeId)s)")
              
    add_match_participant_mastery = ("INSERT IGNORE INTO match_participant_masteries "
               "(matchId, summonerId, rank, masteryId)"
               "VALUES (%(matchId)s, %(summonerId)s, %(rank)s, %(masteryId)s)")

    add_match_participant_delta = ("INSERT IGNORE INTO match_participant_deltas "
               "(matchId, summonerId, deltaName, deltaTimeframe, value)"
               "VALUES (%(matchId)s, %(summonerId)s, %(deltaName)s, %(deltaTimeframe)s, %(value)s)")
                
    add_match_teams = ("INSERT IGNORE INTO match_teams "
                "(matchId, teamId, baronKills, dominionVictoryScore, dragonKills, firstBaron, firstBlood, firstDragon, firstInhibitor, firstRiftHerald, firstTower, inhibitorKills, riftHeraldKills, towerKills, vilemawKills, winner) " 
                "VALUES (%(matchId)s, %(teamId)s, %(baronKills)s, %(dominionVictoryScore)s, %(dragonKills)s, %(firstBaron)s, %(firstBlood)s, %(firstDragon)s, %(firstInhibitor)s, %(firstRiftHerald)s, %(firstTower)s, %(inhibitorKills)s, %(riftHeraldKills)s, %(towerKills)s, %(vilemawKills)s, %(winner)s	)")
   
    add_match_bans = ("INSERT IGNORE INTO match_team_bans "
                "(matchId, teamId, pickTurn, championId) " 
                "VALUES (%(matchId)s, %(teamId)s, %(pickTurn)s, %(championId)s)")
               
    add_match_timeline = ("INSERT IGNORE INTO match_timeline "
                "(matchId, summonerId, timestamp, currentGold, positionX, positionY, minionsKilled, level, jungleMinionsKilled, totalGold, dominionScore, participantId, xp, teamScore, timelineInterval) "
                "VALUES (%(matchId)s, %(summonerId)s, %(timestamp)s, %(currentGold)s, %(positionX)s, %(positionY)s, %(minionsKilled)s, %(level)s, %(jungleMinionsKilled)s, %(totalGold)s, %(dominionScore)s, %(participantId)s, %(xp)s, %(teamScore)s, %(timelineInterval)s )")
   
    add_match_timeline_event =  ("INSERT IGNORE INTO match_timeline_events "
                "(eventId, matchId, summonerId, timelineTimestamp, eventTimestamp, ascendedType, assistingParticipants, buildingType, creatorId, eventType, itemAfter, itemBefore, itemId, killerId, laneType, levelUpType, monsterType, pointCaptured, positionX, positionY, skillSlot, teamId, towerType, victimId, wardType) "
                "VALUES (%(eventId)s, %(matchId)s, %(summonerId)s, %(timelineTimestamp)s, %(eventTimestamp)s, %(ascendedType)s, %(assistingParticipants)s, %(buildingType)s, %(creatorId)s, %(eventType)s, %(itemAfter)s, %(itemBefore)s, %(itemId)s, %(killerId`)s, %(laneType)s, %(levelUpType)s, %(monsterType)s, %(pointCaptured)s, %(positionX)s, %(positionY)s, %(skillSlot)s, %(teamId)s, %(towerType)s, %(victimId)s, %(wardType)s )")
  
    add_match_timeline_event_assist = ("INSERT IGNORE INTO match_timeline_events_assist "
                "(eventId, matchId, assistId)"
                "VALUES (%(eventId)s, %(matchId)s, %(assistId)s)")

     

    self.print_stuff("Updating Match Tables.", header1 = True)

    self.cursor.execute("SELECT matchId FROM matches")
    existing_matches_raw = self.cursor.fetchall()
    existing_matches = [x[0] for x in existing_matches_raw]
#     for x in existing_matches_raw:
#      for y in x:
#       existing_matches.append(y)




#     for x in existing_timelines_raw:
#      for y in x:
#       existing_timelines.append(y)
      
    if timeline_update == True:
     self.cursor.execute("SELECT DISTINCT(matchId) FROM match_timeline")
     existing_timelines_raw = self.cursor.fetchall()
     existing_timelines = [x[0] for x in existing_timelines_raw]
     self.print_stuff("Overriding matchIds, updating timelines for existing matches.")     
     match_ids = list(set(existing_matches)-set(existing_timelines))
     
    elif(matchIds==False):
     self.print_stuff("No list of match ids, defaulting to search team_history.")
     self.cursor.execute("SELECT gameId FROM team_history" )         
     match_ids_raw = self.cursor.fetchall()
   
     match_ids = [x[0] for x in match_ids_raw] 
     match_ids = list(set(match_ids)-set(existing_matches))
   
#      for x in match_ids_raw:
#       for y in x:
#        match_ids.append(y)
     
    else:
     self.print_stuff("Given list of match ids.")
     match_ids = matchIds
     match_ids = list(set(match_ids)-set(existing_matches))
 
 #    print match_ids
    teams_data = []
    
    for x in match_ids:
     finished = False 
     cur_match_raw = []

     while finished == False:
      self.wait()
      try: 
       cur_match_raw = self.w.get_match( x, region=None, include_timeline=timeline)
     
      except riotwatcher.riotwatcher.LoLException as err:
       if str(err) == "Too many requests" or str(err) == "Unauthorized" or str(err) == "Blacklisted key":
 #         print "New Key" 
         drop = False
         if str(err) == "Blacklisted key":
          self.print_stuff("Blacklisted key, using new key, dropping current.")
          drop = True
         if str(err) == "Unauthorized":
          self.print_stuff("Unauthorized, using new key")
         if len(keys)==1:
          self.print_stuff("Too many requests, not enough keys.", error=True)
          if hangwait == False:
           break 
          else:
           time.sleep(5)
         self.new_key(drop=drop)
        
       else:
        self.print_stuff("%s, Match: %s -- Request" % (str(err), x),error=True)
        break
 #      except:
 #       print "Other Error"
      
      else:
       finished = True
    
    
     if cur_match_raw:
     
      
      cur_match = {}
      
      for y in ["mapId", "matchCreation", "matchDuration", "matchId", "matchMode", "matchType", "matchVersion", "platformId", "queueType", "region", "season"]:
       try:
        cur_match[y] = cur_match_raw[y]

       except:
        cur_match[y] = None
    
  #     print cur_match
      try:
       self.cursor.execute(add_match, cur_match)
      except mysql.connector.Error as err:

        if err.errno != 1062 or suppress_duplicates == False:
         self.print_stuff("%s, Match: %s -- Match" % (err.errno, x), error= True)
  #       print add_team % cur_team
      else:
        self.print_stuff("Updated Match")
      all_match_teams = []
      all_match_bans = []
    
  #     print cur_match_raw['teams']
      for y in cur_match_raw['teams']:
       cur_match_teams = {}
       for z in ("bans", "matchId", "teamId", "baronKills", "dominionVictoryScore", "dragonKills", "firstBaron", "firstBlood", "firstDragon", "firstInhibitor", "firstRiftHerald", "firstTower", "inhibitorKills", "riftHeraldKills", "towerKills", "vilemawKills", "winner"):
        if z == "bans" and "bans" in y:
         for s in y['bans']:
          cur_match_bans = {}
          for t in s:
           cur_match_bans[t] = s[t]
          cur_match_bans['matchId'] = x
          cur_match_bans['teamId'] = y['teamId']
          all_match_bans.append(cur_match_bans)

     
        else:
         try:
          cur_match_teams[z] = y[z]
         except:
          cur_match_teams[z] = None
     
       cur_match_teams['matchId'] = x
       all_match_teams.append(cur_match_teams)
      
      try:
       self.cursor.executemany(add_match_teams, all_match_teams)
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff("%s, Match: %s -- Teams" % (err.errno, x), error=True)
      else:
       self.print_stuff("Updated Match-Teams")
   

      try:
       self.cursor.executemany(add_match_bans, all_match_bans)
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff("%s, Match: %s -- Bans" % (err.errno, x), error=True)
      else:
       self.print_stuff("Updated Match-Bans")
      
       
           
  #     print cur_match_raw
      cur_match_participants_raw = cur_match_raw["participants"]
    
    
     if cur_match_raw:
      cur_match_pi = {}
      for y in cur_match_raw["participantIdentities"]:
       cur_match_pi[y["participantId"]] = y["player"]
    
    
      
      
      
      
      
     if timeline == True and cur_match_raw:  
      if "timeline" in cur_match_raw:
       cur_match_timeline_raw = cur_match_raw["timeline"]   
       intervals = cur_match_timeline_raw["frameInterval"]
    
       for y in cur_match_timeline_raw["frames"]:

        cur_timeline = []


   #      print cur_time
   #      print y
        for z in y["participantFrames"]:
         cur_participant_timeline = {}
      
   #       print "z %s" % z
         for s in ["currentGold", "position", "minionsKilled", "level", "jungleMinionsKilled", "totalGold", "dominionScore", "participantId", "xp", "teamScore", "timelineInterval"]:
          if s != "position":
           try:
            cur_participant_timeline[s] = y["participantFrames"][z][s]
           except:
            cur_participant_timeline[s] = None
          else:
           try:
            cur_participant_timeline['positionX'] = y["participantFrames"][z][s]["x"]
           except:
            cur_participant_timeline['positionX'] = None
           try:
            cur_participant_timeline['positionY'] = y["participantFrames"][z][s]["y"]
           except:
            cur_participant_timeline['positionY'] = None
        
         cur_participant_timeline['matchId'] = x
         cur_participant_timeline['summonerId'] = cur_match_pi[int(z)]["summonerId"]
         cur_participant_timeline['timestamp'] = y["timestamp"]
         cur_participant_timeline['timelineInterval'] = intervals
         cur_timeline.append(cur_participant_timeline)
      
     
   #      timeline_events = []
  #       print x
        if "events" in y:
         timeline_events = []
         assists = []

         for z in y["events"]:
          cur_timeline_event = {}
   #        print z

          for s in ["ascendedType", "assistingParticipants", "buildingType", "creatorId", "eventType", "itemAfter", "itemBefore", "itemId", "killerId`", "laneType", "levelUpType", "monsterType", "pointCaptured", "positionX", "positionY", "skillSlot", "teamId", "towerType", "victimId", "wardType"]:
        
       
           if s=="assistingParticipants":
        
            if "assistingParticipantIds" in z:
             cur_timeline_event["assistingParticipants"]=True
             for t in z["assistingParticipantIds"]:
              cur_assists = {}
              cur_assists["eventId"] = "%s - %s" % (cur_match_timeline_raw["frames"].index(y),  y["events"].index(z))
              cur_assists["matchId"] = x
              cur_assists["assistId"] = cur_match_pi[t]["summonerId"]
              assists.append(cur_assists)

          
          
            else:
             cur_timeline_event["assistingParticipants"]=False
           elif s=="creatorId" or s=="killerId" or s=="victimId":
            try: 
             z[s]
            except:
             cur_timeline_event[s] = None
            else:
             if z[s] == 0:
              if s == "killerId":
               cur_timeline_event[s] = "minion"
              if s == "creatorId":
  #              we're not sure why creatorId would be set to 0, and no one online is 100% sure either. 
               cur_timeline_event[s] = "undefined"
             else:
  #             print z
              cur_timeline_event[s] = cur_match_pi[z[s]]["summonerId"]
           elif s == "position":
            try:
             cur_timeline_event['positionX'] = z[s]["x"]
            except:
             cur_timeline_event['positionX'] = None
            try:
             cur_timeline_event['positionY'] = z[s]["y"]
            except:
             cur_timeline_event['positionY'] = None
           else:
            try:
             z[s]
            except:
             cur_timeline_event[s] = None
            else:
             cur_timeline_event[s] = z[s]
          cur_timeline_event["eventId"] = "%s - %s" % (cur_match_timeline_raw["frames"].index(y),  y["events"].index(z))
          cur_timeline_event["matchId"] = x
          if "participantId" in z:
           if z["participantId"] != 0:
            cur_timeline_event["summonerId"] =  cur_match_pi[z["participantId"]]["summonerId"]
           else:
            cur_timeline_event["summonerId"] = None
          else:
           cur_timeline_event["summonerId"] = None
          cur_timeline_event["timelineTimestamp"] = y["timestamp"]
          cur_timeline_event["eventTimestamp"] = z["timestamp"]
          timeline_events.append(cur_timeline_event)
       
   #        print cur_timeline_event

   #       print timeline_events 

         if assists != []:
          try:
   #         print assists
           self.cursor.executemany(add_match_timeline_event_assist, assists)

          except mysql.connector.Error as err:
           if err.errno != 1062 or suppress_duplicates == False:
            self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline-Events" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)),error=True)
          else:
           self.print_stuff("Updated Timeline-Assists")
         try:
          self.cursor.executemany(add_match_timeline_event, timeline_events)

         except mysql.connector.Error as err:
          if err.errno != 1062 or suppress_duplicates == False:
           self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline-Events" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)), error=True)
         else:
          self.print_stuff("Updated Timeline-Events")
    #       stop
       
        
        try:
         self.cursor.executemany(add_match_timeline, cur_timeline)
        except mysql.connector.Error as err:
         if err.errno != 1062 or suppress_duplicates == False:
          self.print_stuff("%s, Match: %s, Timeframe: %s-- Timeline" % (err.errno, x, cur_match_timeline_raw["frames"].index(y)), error=True)
        else:
         self.print_stuff("Updated Timeline")
      
      
      
      
   #       if killerid == 0, Minion

       self.print_stuff("Finished Timeline")
      else:
       self.print_stuff("No Timeline Data; %s" % (x))
    
     if cur_match_raw:
      runes = []
      masteries = []
      deltas = []
      all_match_participants = []
      for y in cur_match_participants_raw:
       cur_match_participant = {}
  #      print y
  #      break
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
            runes.append(cur_rune)
     
         
         
         elif z == "masteries": 
          cur_mastery = {}  
          cur_mastery["matchId"] = x
          cur_mastery["summonerId"] = cur_match_pi[y["participantId"]]["summonerId"]
          for r in y[z]: 
           for r2 in r:
            cur_mastery[r2] = r[r2]
            masteries.append(cur_mastery)
            
           
#            try:
#             self.cursor.execute(add_match_participant_mastery, cur_mastery)
#            except mysql.connector.Error as err:
#             if err.errno != 1062 or suppress_duplicates == False:
#              self.print_stuff("%s, Match: %s, Player: %s -- Mastery" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]),error=True)
#    #          print add_team % cur_team
#            else:
#             self.print_stuff("Updated Mastery")     
       
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
             deltas.append(cur_delta)

             ##OLD
             # try:
#               self.cursor.execute(add_match_participant_delta, cur_delta)
#              except mysql.connector.Error as err:
#               if err.errno != 1062 or suppress_duplicates == False:
#                self.print_stuff("%s, Match: %s, Player: %s -- Delta" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]),error=True)
#      #          print add_team % cur_team
#              else:
#               self.print_stuff("Updated Participant Deltas") 
       
         else:
          self.print_stuff("Broken: %s" % z)
       
       
        else:
         cur_match_participant[z] = y[z]
     
  #      print cur_match_participant_raw     
  #      if feedback == "all":
  #       print cur_match_participant["nodeCapture"]
       all_match_participants.append(cur_match_participant)
      try:
       self.cursor.executemany(add_match_participant_rune, runes)
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff("%s, Match: %s -- Rune" % (err.errno, x),error=True)
#          print add_team % cur_team
      else:
       self.print_stuff("Updated Rune")
      
      try:
       self.cursor.executemany(add_match_participant_mastery, masteries)
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff("%s, Match: %s -- Mastery" % (err.errno, x),error=True)
#          print add_team % cur_team
      else:
       self.print_stuff("Updated Mastery")    

      try:
       self.cursor.executemany(add_match_participant_delta, deltas)
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff("%s, Match: %s -- Delta" % (err.errno, x),error=True)
#          print add_team % cur_team
      else:
       self.print_stuff("Updated Participant Deltas") 

      try: 
       self.cursor.executemany(add_match_participants, all_match_participants)
 #       print "Try"
    
      except mysql.connector.Error as err:
       if err.errno != 1062 or suppress_duplicates == False:
        self.print_stuff("%s, Match: %s-- Participant" % (err.errno, x), error=True)
 #       print add_team % cur_team
      else:
       self.print_stuff("Updated Match-Participants")

         ##OLD
#        try: 
#         self.cursor.execute(add_match_participants, cur_match_participant)
#   #       print "Try"
#      
#        except mysql.connector.Error as err:
#         if err.errno != 1062 or suppress_duplicates == False:
#          self.print_stuff("%s, Match: %s, Player: %s -- Participant" % (err.errno, x, cur_match_pi[y["participantId"]]["summonerId"]), error=True)
#   #       print add_team % cur_team
#        else:
#         self.print_stuff("Updated Match-Participants")
        
        
        
     else:
      if suppress_duplicates == False:
       self.print_stuff("Duplicate Match; %s" % (x))

     
     
     
    
       
     self.print_stuff("Finished %s of %s; %s" % (match_ids.index(x)+1,len(match_ids),x), header2=True)
     self.cnx.commit()
          
     
     

  self.cnx.commit()
  if self.use_curses == True :
   curses.endwin()
 
 
 def __exit__(self):
  curses.endwin()
  self.cnx.commit()
  self.cursor.close()
  self.cnx.close()
  if ssh == True:
   credentials.server.stop()




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



# UPDATE matches SET queueType = 'TEAM_BUILDER_DRAFT_UNRANKED_5x5'  WHERE queueType = 'TEAM_BUILDER_DRAFT_U'  AND !isnull( queueType );
# CREATE INDEX id_team ON by_league (playerOrTeamId, team);


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

