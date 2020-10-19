# Gossip Simulator for Distributed Systems
* Implemented Gossip Protocol for information propgation in distributed systems using Akka.NET asynchronous actor facility.
* Applied the algorithm on 4 different topologies (Line, 2D, Imperfect 2D, Full Network) to compare algorithm convergence and performance. 
* Achieved 90% convergence and extended the application for aggregate computation using Push-Sum algorithm.

### Largest network used: 
* For Gossip algorithm: 
    * Full network topology: 10000 nodes  
    * Imperfect 2D topology: 10000 nodes 
    * 2D topology: 10000 nodes 
    * Line topology: 10000 nodes 
 
* For Push-Sum algorithm: 
    * Full network topology: 10000 nodes  
    * Imperfect 2D topology: 10000 nodes 
    * 2D topology: 10000 nodes 
    * Line topology: 1000 nodes 
  
### Execution Command: 
(Example: Gossip algorithm on Full Network Topology with 100 Nodes): 
dotnet fsi --langversion:preview .\project2.fsx 100 full gossip   
