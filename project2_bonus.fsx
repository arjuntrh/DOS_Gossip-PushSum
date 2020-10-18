#time "on"
#r "nuget: Akka.FSharp" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic
open System.Diagnostics
open Akka.Configuration

let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            loglevel: ERROR
        }")

let system = System.create "system" <| configuration
let mutable mainRef = Unchecked.defaultof<_>
let mutable parentActorRefStore = Unchecked.defaultof<_>
let topologyModel = new Dictionary<IActorRef, List<IActorRef>>()
let myRandom = System.Random(System.DateTime.Now.Millisecond)
let counterMap = new Dictionary<IActorRef, int>()
let timer = Stopwatch()
let pushSumValues = new Dictionary<IActorRef, int>()

let mutable completedCount = 0
let mutable actorsInitiated = 0
let mutable globalGossipLimit = 15
let mutable numNodesGlobal = 0
let mutable globalTopology = ""


let mutable failedActorsCount = 0
let mutable algorithmConverged = false
let failureInducedNodes = 400

type ChildActorMessage = 
    |ProcessFirstGossipActor of string * IActorRef
    |ProccessGossip of string * IActorRef
    |ProcessFirstPushSumActor of IActorRef
    |ProccessPushSum of float * float
    |InitPushSumValues
    |KillGossipActor of IActorRef

type ParentActorMessage = 
    |ProcessInput of int * string * string
    |ProcessGossipActorsTermination of int
    |StoreActorsInitiated of int
    |ProcessPushSumActors of int
    |ProcessGossipFailureNodes of int


let childActor (mailbox: Actor<_>) =

    //Gossip algorithm related Variables
    let mutable actorExecutionCompleted = false
    let mutable gossipCount = 0
    let mutable selfcount = 0
    let mutable heardGossip = false

    //Push-Sum related Variables
    let mutable s = 0.0
    let mutable w = 1.0
    let mutable consecutiveCycleCount = 0
    let mutable initialRatio = s/w
    let mutable finalRatio = s/w

    //Actors Error Handling
    let mutable failedActor = false

    let rec childActorLoop () = actor {

        let! message = mailbox.Receive()
        let childActorSender = mailbox.Sender()

        match message with

        | ProcessFirstGossipActor (gossipMessage, curActorRef) ->
            parentActorRefStore <- childActorSender
            printfn "Gossip Algorithm initiated..."
            gossipCount <- gossipCount + 1
            curActorRef <! ProccessGossip(gossipMessage, curActorRef)

        | ProccessGossip (gossipMessage, curActorRef) ->
            if not failedActor then
                if(not heardGossip) then
                    heardGossip <- true
                    parentActorRefStore <! StoreActorsInitiated(1)

                if(childActorSender <> mailbox.Self) then
                    gossipCount <- gossipCount + 1
                    selfcount <- 0
                else
                    selfcount <- selfcount + 1

                //If Gossip Count = globalGossipLimit then terminate the actor and send the message to the parent
                if gossipCount = globalGossipLimit && not actorExecutionCompleted then
                    actorExecutionCompleted <- true
                    parentActorRefStore <! ProcessGossipActorsTermination(1)

                elif gossipCount < globalGossipLimit then
                    if selfcount = numNodesGlobal then
                        parentActorRefStore <! ProcessGossipActorsTermination(1)
                    else
                        let neighborList = topologyModel.[curActorRef]
                        let findRandomNeighbor = System.Random()
                        let randomNeighborHelper= findRandomNeighbor.Next(0, neighborList.Count)
                        let neighborActor = neighborList.[randomNeighborHelper]
                        neighborActor <! ProccessGossip(gossipMessage, neighborActor)
                        curActorRef <! ProccessGossip(gossipMessage, curActorRef)

        | KillGossipActor(curActorRef) ->
            parentActorRefStore <- childActorSender
            failedActor <- true
            if not actorExecutionCompleted then
                actorExecutionCompleted <- true
                // printfn "Killed the actor"
                parentActorRefStore <! ProcessGossipFailureNodes(1)
                

        | InitPushSumValues ->
            s <- pushSumValues.[mailbox.Self] |> float

        | ProcessFirstPushSumActor (curActorRef) ->
            parentActorRefStore <- childActorSender
            printfn "Push-Sum Algorithm initiated..."
            curActorRef <! ProccessPushSum(0.0, 0.0)

        | ProccessPushSum (sum, weight) ->

            if(not heardGossip) then
                heardGossip <- true
                parentActorRefStore <! StoreActorsInitiated(1)
            
            if not actorExecutionCompleted then

                if(childActorSender <> mailbox.Self) then
                    initialRatio <- s/w

                    s <- s + sum
                    w <- w + weight

                    finalRatio <- s/w

                    let difference = finalRatio - initialRatio
                    if(difference > 10.0**(-10.0)) then 
                        consecutiveCycleCount <- 0
                    else
                        consecutiveCycleCount <- consecutiveCycleCount + 1
                    
                    if consecutiveCycleCount = 3 then
                        actorExecutionCompleted <- true
                        parentActorRefStore <! ProcessPushSumActors(1)
                else
                    selfcount <- selfcount + 1
                
                // When cycle is not 3, check if actors have been called by neighbors or itself
                if selfcount = 1000 then
                    actorExecutionCompleted <- true
                    parentActorRefStore <! ProcessPushSumActors(1)
                else   
                    let neighborList = topologyModel.[mailbox.Self]
                    let findRandomNeighbor = System.Random()
                    let randomNeighbor= findRandomNeighbor.Next(0, neighborList.Count)
                    let neighborActor = neighborList.[randomNeighbor]
                    neighborActor <! ProccessPushSum(s/2.0, w/2.0)

                    s <- s/2.0
                    w <- w/2.0
                    mailbox.Self <! ProccessPushSum(0.0, 0.0)

        return! childActorLoop ()
    }
    childActorLoop ()


let createFullTopology(actorsDict:Dictionary<int,IActorRef>, numNodes:int) =
    for entry in actorsDict do
            let neighbors = new List<IActorRef>()
            let actorID = entry.Key
            if actorID = 1 then
                for i= 2 to numNodes do
                    neighbors.Add(actorsDict.[i])
            elif actorID = numNodes then
                for i= 1 to numNodes-1 do
                    neighbors.Add(actorsDict.[i])
            else 
                for i= 1 to actorID-1 do
                    neighbors.Add(actorsDict.[i])
                for i= actorID+1 to numNodes do
                    neighbors.Add(actorsDict.[i])

            topologyModel.Add(entry.Value, neighbors)


let createLineTopology(actorsDict:Dictionary<int,IActorRef>, numNodes:int) = 
    
    for entry in actorsDict do
        let neighbors = new List<IActorRef>()
        let actorID = entry.Key
        if actorID = 1 then
            neighbors.Add(actorsDict.[2])
        elif actorID = numNodes then
            neighbors.Add(actorsDict.[numNodes-1])
        else
            neighbors.Add(actorsDict.[actorID-1])
            neighbors.Add(actorsDict.[actorID+1])

        topologyModel.Add(entry.Value, neighbors)

let create2DTopology(actorsDict:Dictionary<int,IActorRef>, numNodes:int, randomFlag:bool) = 
    
    let countRows = sqrt (numNodes |> float) 
    let rows = countRows |> int

    for entry in actorsDict do
        let neighbors = new List<IActorRef>()
        let actorID = entry.Key

        let mutable rightNeighbor = -1
        let mutable leftNeighbor = -1
        let mutable topNeighbor = -1
        let mutable bottomNeighbor = -1

        // add right neighbor
        if (actorID % rows <> 0) then
            rightNeighbor <- actorID + 1
            neighbors.Add(actorsDict.[rightNeighbor])

        // add left neighbor
        if ((actorID - 1) % rows <> 0) then
            leftNeighbor <- actorID - 1;
            neighbors.Add(actorsDict.[leftNeighbor])

        // add top neighbor
        if ((actorID - rows) >= 1) then
            topNeighbor <- actorID - rows
            neighbors.Add(actorsDict.[topNeighbor])

        // add bottom neighbor
        if ((actorID + rows) <= numNodes) then
            bottomNeighbor <- actorID + rows
            neighbors.Add(actorsDict.[bottomNeighbor])

        // add random neighbor
        if (randomFlag) then
            let randomHelper () = myRandom.Next(1, numNodes + 1)
            let mutable myFlag = true

            while myFlag do
                let randomNeighbor = randomHelper()
                if (randomNeighbor <> actorID 
                    && randomNeighbor <> leftNeighbor && randomNeighbor <> rightNeighbor 
                    && randomNeighbor <> topNeighbor && randomNeighbor <> bottomNeighbor) then
                    myFlag <- false
                    neighbors.Add(actorsDict.[randomNeighbor])

        topologyModel.Add(entry.Value, neighbors)          

let createTopology(actorsDict:Dictionary<int,IActorRef>, topology:string, numNodes:int) = 
   
    if topology = "line" then
        createLineTopology(actorsDict, numNodes)
    elif topology = "full" then
        createFullTopology(actorsDict, numNodes)
    elif topology = "2D" then
        create2DTopology(actorsDict, numNodes, false)
    elif topology = "imp2D" then
        create2DTopology(actorsDict, numNodes, true)
    else
        printfn "No matching topology found"


let parentActor (mailbox: Actor<_>) =

    let rec parentActorLoop () = actor {
        
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()

        match message with
        | ProcessInput(numNodes, topology, algorithm) ->
            mainRef <- sender
            numNodesGlobal <- numNodes
            globalTopology <- topology

            let dict = new Dictionary<int, IActorRef>();
            for i = 1 to numNodes do
                dict.Add(i, spawn system (string(i)) childActor)
                counterMap.Add(dict.[i], 0)
                pushSumValues.Add(dict.[i], i)

            let topologyModel = createTopology(dict, topology, numNodes)


            // Select first node randomly to trigger gossip
            let startingActorId = myRandom.Next(1, numNodes + 1)
            let startingActorRef = dict.[startingActorId]

            let gossipMessage = "GOSSIP"
            timer.Start();
            if algorithm = "gossip" then
                startingActorRef <! ProcessFirstGossipActor(gossipMessage, startingActorRef)

            elif algorithm = "push-sum" then
                for entry in pushSumValues do
                    entry.Key <! InitPushSumValues
                    
                startingActorRef <! ProcessFirstPushSumActor(startingActorRef)
            else
                printfn "Unrecognized Algorithm"

            for i = 1 to failureInducedNodes do
                let findRandomActor = System.Random()
                let randomActorNumber= findRandomActor.Next(1, numNodesGlobal)
                let failureActor = dict.[randomActorNumber]
                if not algorithmConverged then
                    failureActor <! KillGossipActor(failureActor)

        | ProcessGossipActorsTermination(count) ->
            completedCount <- completedCount + 1
            let limit = 0.9 * (numNodesGlobal |> float )|> int
            if(completedCount + failedActorsCount = limit || completedCount = actorsInitiated) then
                algorithmConverged <- true
                mainRef <! "All Actors completed the Task"

        | ProcessGossipFailureNodes(count) ->
            failedActorsCount <- failedActorsCount + 1
            // printfn "failedActorsCount = %d" failedActorsCount

        | StoreActorsInitiated(count) ->
            actorsInitiated <- actorsInitiated + count

        | ProcessPushSumActors(count) ->
            completedCount <- completedCount + count
            let limit = 1.0 * (numNodesGlobal |> float )|> int
            if(completedCount = limit || completedCount = actorsInitiated) then
                if completedCount = actorsInitiated && globalTopology = "line" then
                    if numNodesGlobal >= 10000 then
                        Async.Sleep(5000) |> Async.RunSynchronously
                    else
                        Async.Sleep(500) |> Async.RunSynchronously
                mainRef <! "All Actors completed the Task"

        return! parentActorLoop()
    }

    parentActorLoop ()

let main() =
    try
        (* Command Line input - num of nodes, topology and algorithm *)
        let mutable numNodes = fsi.CommandLineArgs.[1] |> int
        let topology = fsi.CommandLineArgs.[2]
        let algorithm = fsi.CommandLineArgs.[3]
        let mutable rowCount = 0.0

        (* Rounding of the values to the nearest square *)
        if (topology = "2D" || topology = "imp2D") then
            rowCount <- ceil(Math.Sqrt(numNodes |> float))
            numNodes <- (rowCount * rowCount) |> int

        let parentActorRef = spawn system "parentActor" parentActor
        let parentTask = (parentActorRef <? ProcessInput(numNodes, topology, algorithm))
        let response = Async.RunSynchronously(parentTask)

        printfn "Convergence Time = %d ms" timer.ElapsedMilliseconds

    with :? TimeoutException ->
        printfn "Timeout!"

main()
system.Terminate()
