include "globals.mzn";

int: no_firetrucks;
set of int: FIRETRUCKS = 1..no_firetrucks;
array[FIRETRUCKS] of int: water_cap;
array[FIRETRUCKS] of int: service_time;

%% Decision Variables - You can change anything, BUT the names of the variables.
array[FIRETRUCKS] of var 0..1000: start_fill;
array[FIRETRUCKS] of var 0..1000: start_service;
var 0..1000: makespan; 
var 0..1000: total_service;

%%% Add your code here

var int: fire_hydrant = 3;
var int: fill_per_min = 50;
array[FIRETRUCKS] of var int: time_fill = [water_cap[i] div fill_per_min | i in FIRETRUCKS];
array[FIRETRUCKS] of var int: end_fill = [start_fill[i] + time_fill[i] | i in FIRETRUCKS];
array[FIRETRUCKS] of var int: end_service = [start_service[i] + service_time[i] | i in FIRETRUCKS];  

%Constraint 1 : fill happens first and after that it goes to service
%end_fill for each firetruck is the time start_service can begin
constraint forall(f in FIRETRUCKS) (start_service[f] >= end_fill[f]);

%Constraint 2 : only 1 firetruck can be for service (1 service at a time)
constraint disjunctive([start_service[f] | f in FIRETRUCKS], [service_time[f] | f in FIRETRUCKS]);

%Constraint 3 : no more than 3 firetrucks can fill at the same time
constraint cumulative([start_fill[f] | f in FIRETRUCKS], [time_fill[f] | f in FIRETRUCKS], [1 | f in FIRETRUCKS],fire_hydrant);

%Constraint 4: total time for fill and service is the maximum time of the end of service for the last truck, since service happens after the fill
constraint makespan = max([end_service[f] | f in FIRETRUCKS]);

%Constraint 5: total service time is from the moment the first truck goes to service(min start time)
%until the last truck finishes(max start time + service time)
constraint total_service = max(end_service) - min(start_service);

solve minimize makespan;

output [" start_fill=\(start_fill);\n start_service=\(start_service);\n"]
       ++ [" total_service = \(total_service);\n makespan=\(makespan);\n"]

