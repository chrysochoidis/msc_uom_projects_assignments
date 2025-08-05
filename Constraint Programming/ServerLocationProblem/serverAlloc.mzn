%% Number of Servers and Clients and related Sets
par int: Servers;
par int: Clients;

set of int:SERVERS = 1..Servers;
set of int:CLIENTS = 1..Clients;

%% Arrays stating Problem Data
array[SERVERS] of int: Capacity;
array[SERVERS] of int: DeploymentCost;
array[CLIENTS] of int: ClientDemands ;
array[CLIENTS,SERVERS] of int: NetworkingCost;

%% Add your code here
array[CLIENTS] of var SERVERS: client_at;
var int: total_cost;

array[SERVERS] of var int : serverLoad;
array[SERVERS] of var 0..1 : usedServers;


%Constraint 1:save in an array the total amount of services load on each server
constraint forall(s in SERVERS) (serverLoad[s] = sum(c in CLIENTS where client_at[c] = s) (ClientDemands[c]));

%Constraint 2: a server's total load should be less than the server's capacity 
constraint forall(s in SERVERS)(serverLoad[s] <= Capacity[s]);
     
%Constraint 3: if a server's load is greater than 0 then thr server is active (hence 1), else if the server's laod is 0 then is inactive (hence 0)  
constraint forall(s in SERVERS) (usedServers[s] = (if serverLoad[s] > 0 then 1 else 0 endif));
          
var int : totalNetworkingCost = sum(c in CLIENTS) (ClientDemands[c] * NetworkingCost[c, client_at[c]]);
var int : totalDeploymentCost = sum(s in SERVERS) (usedServers[s] * DeploymentCost[s]);

total_cost = totalNetworkingCost + totalDeploymentCost;

solve minimize total_cost;

%%% DO NOT DELETE THIS LINES
output [" client_at = \(client_at); \n total_cost=\(total_cost);\n"];