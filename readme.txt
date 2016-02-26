Make copy of credentials-template.py and rename it credentials.py
create a file called skiplist.tsv

python LoL.py -s [True/False]

-s : option declares whether ssh is being used or not


or you can
import LoL

LoLS = LoL.Scraper(ssh = True)

##setting ssh = True uses the SSH = True ssh tunneling protocol from credentials.py, whereas False treats it as if its local host connection




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


# update_table("checkteams", teamIds= False, ignore_skiplist = False, allow_updates = False) -- teamIds takes list ; []
# this checks the team table and grabs a list of all the ids. it then gets the league information for each team. 
# if you supply this with a list of teamIds it will just do those.
# setting ignore_skiplist = True will ignore the list that generates whenever the code skips an entry because of no data (this feature was added because of the RANKED_TEAM_5x5 fiasco)
# setting allow_updates = True, will allow you ignore the skiplist and will ignore the 'existing entries' and re-call every team in teamIds. This is used to add a new entry if their ranking is different than in the database. 


# update_table("team", teamIds=False, checkTeams=False) -- teamIds takes list ; []
# this is the primary mechanism, it grabs all the team ids from the by-league table and gets all the information from the team api
# it then sorts it into team, team-history, and team-roster tables.
# Additionally, this table can be supplied with pretty much any length of team ids and it will iterate through those instead
# setting checkTeams to true, automatically calls the update_table("checkteams") function; you can pass any variables you'd pass to checkteams and it will pass through this. (but use default teamIds variable)


# update_table("iterate",iteratestart=1, iterate=100, checkTeams=False)
# give this function a starting id and it will search for all team-ids associated with that id. 
# it will do this [iterate] number of times. Once the list is compiled, it sends it through update_table("team")
# optionally you can set it so that it will also automatically run update_table("checkteams") to verify [by setting checkTeams=True]. 
# Note: if you want to start at id "1" and end at id "100" you would need to set iteratestart=1, iterate=100
# This function will now iterate through keys in the same way check teams does


# update_table("all")
# this function will cycle from updating challenger -> master -> team -> checkteam, will not do iterate
# this function takes all variables that challenger, master, team, and checkteam take.  with the exception of "teamIds", that uses default.

# update_table("match", matchIds=False, timeline=False, timeline_update=False) -- matchIds takes list ; []
# this function will import all non-timeline data from a given list of matchIds. if no matchIds are supplied, it will automatically search through the list of matchIds in 'team-history'
# timeline=True will now import timeline data too
# timeline and regular match data are treated differently. The function makes a skiplist for existing matches, and a separate one for existing timelines. all non-timeline data will be
#    skipped if the matchid is in the matches table. However, timeline data will still be processed if timeline is set to true. 
# timeline_update = True will override the default 'search through team history' and only update matches that have been collected and are missing timelines. 



# update_table("membertiers", matchIds=False, ignore_skiplist = False, allow_updates = False) -- matchIds takes list ; []
# this function is essentially the same as the 'checkteams' functionality however this will search a given match and scrape the league data for all the players in that match
# if you want to just do all the matches in the database (match table), don't set matchIds to anything.
# setting ignore_skiplist = True will ignore the list that generates whenever the code skips an entry because of no data (this feature was added because of the RANKED_TEAM_5x5 fiasco)
# setting allow_updates = True, will allow you ignore the skiplist and will ignore the 'existing entries' and re-call every team in teamIds. This is used to add a new entry if their ranking is different than in the database. 



# update_table("individualhistory", summonerIds = False, just_teams = True, allow_updates = Fals) -- summonerIds takes list ; []
# this function adds individual history to the individual_history table. you can supply it with a list of summonerIds if you wish
# additionally, if you do not supply with summonerIds, the function takes a look at the just_teams option. if just_teams is set to true
# we query all summoner ids in the 'team roster' list and update all of them. if just_teams is False, we use all the summoner ids in the match_participants table [much longer]
# allow_updates = True, will no longer skip 'existing entries' so that you can check to see if any people have played more matches. 





## FOR ANY "TABLE" in the UPDATE_TABLE() FUNCTION
# Hangwait -- default False
#     setting hangwait="true" enables the function to keep attempting the call until it is allowed through with the current key. 
#     Hangwait is default false, but that only applies if there is only 1 key in your credentials file. 
# Feedback -- default "all"
#     setting feedback="all" will print all errors, print a completion statement when a step is finished, and print updates
#     setting feedback="quiet" will print only uncommon problem errors (duplicate entry errors are silenced), and will print completion statements when long steps are finished
#     setting feedback="silent" will suppress all printing
# suppress_duplicates -- default False
#     setting suppress_duplicates=True will suppress printing of duplicate entry errors. this only effects feedback="all" as the script overrides this setting for quiet and silent modes
# create -- default False
#     setting create=True will look at the create_tables function within the Scraper class. create_tables will check if all tables in the function exist, and it will create any missing ones.
#     still debugging this to allow for initial database creation





## Helpful SQL Commands 
## to get data size
#SELECT  TABLE_NAME,  TABLE_ROWS,  DATA_LENGTH / (1024*1024) as "Data Length in MB",  INDEX_LENGTH / (1024*1024) as "Index Length in MB",  (DATA_LENGTH + INDEX_LENGTH) / (1024*1024) as "Total in MB"   FROM  information_schema.TABLES  WHERE  TABLE_SCHEMA = (SELECT DATABASE())  GROUP BY  TABLE_NAME  ORDER BY  "Total in MB";




##FUTURE UPDATES;;
#implement skiplist for other tables (matches, teams, ind_history [if such a case arises], etc.). 
 