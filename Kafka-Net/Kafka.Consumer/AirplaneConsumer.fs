module AirplaneConsumer

open RdKafka
open System
open FSharp.Collections.ParallelSeq
open Newtonsoft.Json

//Step 3:  pull flight data from the cleansed queue and perform grouping.
//Flights are saved as JSON, so it should be easy to deserialize into a valid type.
//Group flights together by destination state
    //Total number of flights
    //Number of delayed flights
    //Average delay (decimal)

type Flight = { Year:int; Month:int; DayOfMonth:int; DayOfWeek:int; DepTime:Option<int>; CRSDepTime:int; ArrTime:Option<int>; CRSArrTime:int; 
                UniqueCarrier:string; FlightNum:string; TailNum:string; ActualElapsedTime:Option<int>; CRSElapsedTime:Option<int>; AirTime:Option<int>;
                ArrDelay:Option<int>; DepDelay:Option<int>; Origin:string; Dest:string; Distance:int; TaxiIn:Option<int>; TaxiOut:Option<int>; 
                Cancelled:bool; CancellationCode:string; Diverted:bool; CarrierDelay:int; WeatherDelay:int; NASDelay:int; SecurityDelay:int; 
                LateAircraftDelay:int; OriginCity:string; OriginState:string; DestinationCity:string; DestinationState:string; }

type FlightSolution = { DestinationState:string; ArrDelay:int }

let readFromBeginning (consumer:EventConsumer) =
    consumer.OnPartitionsAssigned.Add(fun(partitions) -> 
        printfn "Starting from the beginning..."
        let fromBeginning = List.ofSeq partitions
                               |> List.map(fun(x) -> new TopicPartitionOffset(x.Topic, x.Partition, RdKafka.Offset.Beginning))  
        let fb = new System.Collections.Generic.List<TopicPartitionOffset>(fromBeginning |> List.toSeq)
        consumer.Assign(fb);
    )

let processMessages (consumer:EventConsumer) n (flights:System.Collections.Generic.List<FlightSolution>) =
    //Always start from the beginning.
    readFromBeginning consumer
    let mutable x = 0

    consumer.OnMessage.Add(fun(msg) ->
        let messageString = System.Text.Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length)
        let flight = JsonConvert.DeserializeObject<Flight>(messageString)
        let fsol = {DestinationState = flight.DestinationState;
                    ArrDelay = match flight.ArrDelay.IsSome with
                                | true -> flight.ArrDelay.Value
                                | false -> 0 }
        flights.Add(fsol)

        //Every once in a while, spit out a message to let us know we're still working.
        x <- x + 1
        if x % n = 0 then
            printfn "Read in %i messages" x
    )

let delaysByState flightTuple =
    let totalFlights =
        flightTuple
        |> Seq.countBy fst
        |> Seq.sort
    let delayedFlights =
        flightTuple
        |> Seq.filter(fun(dest,delay) -> delay > 0)
        |> Seq.countBy fst
        |> Seq.sort
    let totalArrivalDelay =
        flightTuple
        |> Seq.groupBy(fun(dest,delay) -> dest)
        |> Seq.map(fun(dest,delay) -> (dest,delay |> Seq.sumBy snd))
        |> Seq.sort
    let results = 
        Seq.zip3 totalFlights delayedFlights totalArrivalDelay
        |> Seq.map(fun((dest,flights),(dest,delayed),(dest,delay)) -> dest,flights,delayed,delay)
        |> Seq.sort
    results

[<EntryPoint>]
let main argv = 
    let stopWatch = new System.Diagnostics.Stopwatch()
    stopWatch.Start()

    Console.BackgroundColor <- ConsoleColor.DarkCyan
    Console.ForegroundColor <- ConsoleColor.White
    Console.Clear()
    //Pull enriched queue items from the EnrichedFlights topic.
    let (config:RdKafka.Config) = new Config(GroupId = "Airplane Consumer")
    use consumer = new EventConsumer(config, "sandbox.hortonworks.com:6667")
    let topics = ["EnrichedFlights"]
    consumer.Subscribe(new System.Collections.Generic.List<string>(topics |> List.toSeq))

    let flights = new System.Collections.Generic.List<FlightSolution>()

    processMessages consumer 10000 flights
    consumer.Start()
    //Because consumer is an event, we leave it to the user to decide when to quit.
    //This allows for sampling the stream rather than needing to wait until its conclusion.
    printfn "Started reader. Press enter to finalize calculations and display results."
    System.Console.ReadLine() |> ignore
    consumer.Stop() |> ignore

    stopWatch.Stop()
    printfn "You've finished pulling enriched data.  You stopped after %A.  Now aggregating data..." stopWatch.Elapsed
    stopWatch.Reset()
    stopWatch.Start()

    let flightTuple =
        List.ofSeq flights
        |> Seq.map(fun f -> (f.DestinationState, f.ArrDelay))
    let results = delaysByState flightTuple
                    |> Seq.iter(fun(dest,flights,delayed,delay) ->
                            printfn "Dest: %A. Flights: %i.  Delayed: %i.  Total Delay (min): %i.  Avg When Delayed (min): %.3f" dest flights delayed delay (float(delay)/float(delayed)))

    stopWatch.Stop()
    printfn "It took %A to aggregate the data.  Press enter to quit." stopWatch.Elapsed
    System.Console.ReadLine() |> ignore

    0 // return an integer exit code
