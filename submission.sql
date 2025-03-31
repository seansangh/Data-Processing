create table query1 as
select g.name as name, count(g.name) as moviecount
from genres g, hasagenre h
where g.genreid= h.genreid
group by g.name;


create table query2 as
select g.name as name, avg(r.rating) as rating
from genres g, ratings r, hasagenre h
where g.genreid= h.genreid and h.movieid= r.movieid
group by g.name;


create table query3 as
select m.title as title, count(r.movieid) as countofratings
from ratings r, movies m
where r.movieid= m.movieid
group by m.title
having count(r.movieid)>9;


create table query4 as
select m.movieid as movieid, m.title as title
from genres g, movies m, hasagenre h
where m.movieid= h.movieid and h.genreid= g.genreid and g.name= 'Comedy';


create table query5 as
select m.title as title, avg(r.rating) as average
from movies m, ratings r
where m.movieid= r.movieid
group by m.movieid;


create table query6 as
select avg(r.rating) as average
from genres g, ratings r, hasagenre h
where g.genreid= h.genreid and h.movieid= r.movieid and g.name= 'Comedy'
group by g.name;


create table query7 as
select avg(r.rating) as average
from genres g, ratings r, hasagenre h
where g.genreid= h.genreid and h.movieid= r.movieid and g.name= 'Comedy' and r.movieid in(
select rs.movieid
from genres gs, ratings rs, hasagenre hs
where gs.genreid= hs.genreid and hs.movieid= rs.movieid and gs.name= 'Romance')
group by g.name;


create table query8 as
select avg(r.rating) as average
from genres g, ratings r, hasagenre h
where g.genreid= h.genreid and h.movieid= r.movieid and g.name= 'Romance' and r.movieid not in(
select rs.movieid
from genres gs, ratings rs, hasagenre hs
where gs.genreid= hs.genreid and hs.movieid= rs.movieid and gs.name= 'Comedy')
group by g.name;


create table query9 as
select r.movieid as movieid, r.rating as rating
from ratings r
where userid= :v1;
