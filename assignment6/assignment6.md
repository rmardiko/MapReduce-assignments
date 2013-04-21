a = load '/user/shared/tweets2011/tweets2011.txt' using PigStorage('\t') as (id, creationtime, user, text); 
b = foreach a generate ToDate(creationtime,'EEE MMM dd HH:mm:ss Z yyyy') as mytime; 
c = foreach b generate GetMonth(mytime) as mymonth, GetDay(mytime) as myday, GetHour(mytime) as myhour; 
d = group c by (mymonth,myday,myhour); 
e = foreach d generate group as thetime, COUNT(c) as count; 
 
dump e; 
 
p = load '/user/shared/tweets2011/tweets2011.txt' using PigStorage('\t') as (id, creationtime, user, text); 
q = foreach p generate ToDate(creationtime,'EEE MMM dd HH:mm:ss Z yyyy') as mytime, REGEX_EXTRACT(text, '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*',1) as foundegypt; 
qq = filter q by foundegypt is not null; 
r = foreach qq generate GetMonth(mytime) as mymonth, GetDay(mytime) as myday, GetHour(mytime) as myhour; 
s = group r by (mymonth,myday,myhour); 
t = foreach s generate group as thetime, COUNT(r) as count; 
 
dump t; 


Grading
=======

There's a bug somewhere that's not making the graphs come out correctly...
I'll send around my solutions I have so you can check.

Score: 20/25

-Jimmy
