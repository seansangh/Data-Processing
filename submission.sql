create table users(
    userid int primary key,
    name text not null

);


create table movies(
    movieid int primary key,
    title text not null

);


create table taginfo(
    tagid int primary key,
    context text unique

);


create table genres(
    genreid int primary key,
    name text unique

);


create table ratings(
    userid int references users(userid),
    movieid int references movies(movieid),
    rating numeric check(rating <= 5.0 and rating >= 0.0),
    timestamp bigint,
	primary key (userid, movieid)
	
);


create table tags(
    userid int references users(userid),
    movieid int references movies(movieid),
    tagid int references taginfo(tagid),
    timestamp bigint

);


create table hasagenre(
    movieid int references movies(movieid),
    genreid int references genres(genreid)

);




