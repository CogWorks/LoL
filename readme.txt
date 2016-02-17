Make copy of credentials-template.py and rename it credentials.py


python LoL.py -s [True/False]

-s : option declares whether ssh is being used or not


or you can
import LoL

LoLS = LoL.Scrapper(ssh = True)

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


# update_table("checkteams", teamIds= False, hangwait=False)
# this checks the team table and grabs a list of all the ids. it then gets the league information for each team. 
# if you supply this with a list of teamIds it will just do those.
# This is no longer the only function that will iterate through your list of keys.
# Hangwait=True enables the function to keep attempting the call until it is allowed through with the current key. 
# Hangwait is default false, but that only applies if there is only 1 key in your credentials file.  
# setting ignore_skiplist = True will ignore the list that generates whenever the code skips an entry because of no data (this feature was added because of the RANKED_TEAM_5x5 fiasco)


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



# update_table("individualhistory", summonerIds = [], just_teams = True, create = False)
# this function adds individual history to the individual_history table. you can supply it with a list of summonerIds if you wish
additionally, if you do not supply with summonerIds, the function takes a look at the just_teams option. if just_teams is set to true
we query all summoner ids in the 'team roster' list and update all of them. if just_teams is False, we use all the summoner ids in the match_participants table [much longer]






## FOR ANY FUNCTION
# setting feedback="all" will print all errors, print a completion statement when a step is finished, and print updates
# setting feedback="quiet" will print only uncommon problem errors (duplicate entry errors are silenced), and will print completion statements when long steps are finished
# setting feedback="silent" will suppress all printing
# setting suppress_duplicates=True will suppress printing of duplicate entry errors. this only effects feedback="all" as the script overrides this setting for quiet and silent modes








##SQL Command to get data size
#SELECT  TABLE_NAME,  TABLE_ROWS,  DATA_LENGTH / (1024*1024) as "Data Length in MB",  INDEX_LENGTH / (1024*1024) as "Index Length in MB",  (DATA_LENGTH + INDEX_LENGTH) / (1024*1024) as "Total in MB"   FROM  information_schema.TABLES  WHERE  TABLE_SCHEMA = (SELECT DATABASE())  GROUP BY  TABLE_NAME  ORDER BY  "Total in MB";


SELECT COUNT(*)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_catalog = 'lol';
 