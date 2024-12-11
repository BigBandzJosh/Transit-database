create table if not exists transit.transit_priority_corridor (
	identifier varchar(6) primary key,
	name varchar(100),
	corridor geometry(MultiLineString, 2961)
);

create table if not exists transit.transit_route_priority_corridor (
	route_number_full varchar(6) REFERENCES transit.transit_route(route_number_full),
	identifier varchar(6) REFERENCES transit.transit_priority_corridor(identifier),
	primary key(route_number_full, identifier)
);

ALTER TABLE transit.transit_route
ADD COLUMN IF NOT EXISTS route GEOMETRY(MultiLineString, 2961);

VACUUM ANALYZE transit.transit_priority_corridor;
VACUUM ANALYZE transit.transit_route_priority_corridor;

insert into transit.transit_route_priority_corridor(route_number_full, identifier)
SELECT 
    r.route_number_full AS route_id,
    tpc.identifier AS corridor_id
FROM 
    transit.transit_route r
JOIN 
    transit.transit_priority_corridor tpc
ON 
    ST_Intersects(r.route, tpc.corridor);


select * from transit.transit_route_priority_corridor;
SELECT * from transit.transit_route;
SELECT * from transit.transit_priority_corridor;