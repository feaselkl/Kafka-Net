module AirplaneConsumer

open Confluent.Kafka
open Confluent.Kafka.Serialization
open System
open System.Text
open System.Collections.Generic
open Newtonsoft.Json
open FSharp.Data
open FSharp.Data.SqlClient

[<Literal>]
let connectionString =
    @"Data Source=LOCALHOST;Initial Catalog=Scratch;Integrated Security=True"

type UpdateDelayByState = SqlCommandProvider<"EXEC dbo.UpdateDelayByState @DelayByState", connectionString>
type TVP = UpdateDelayByState.DelayByStateType

//Step 3:  pull flight data from the cleansed queue and perform grouping.
//Flights are saved as JSON, so it should be easy to deserialize into a valid type.
//Group flights together by destination state
    //Total number of flights
    //Number of delayed flights
    //Average delay (decimal)
//Write to SQL Server every once in a while.
type Flight = { ArrDelay:int; Origin:string; Dest:string; DestST:string; }

type FlightAggregates = { TotalNumberOfFlights:int; NumberOfDelayedFlights:int; TotalArrivalDelay:int }

let processMessage (message:string) (flightAggregates:System.Collections.Generic.Dictionary<string, FlightAggregates>) =
    let flight = JsonConvert.DeserializeObject<Flight>(message)
    let currentResults = match flightAggregates.ContainsKey(flight.DestST) with
                            | true -> flightAggregates.[flight.DestST]
                            | false -> { TotalNumberOfFlights=0; NumberOfDelayedFlights=0; TotalArrivalDelay=0; }
            
    let newResults = {
        TotalNumberOfFlights=currentResults.TotalNumberOfFlights + 1;
        NumberOfDelayedFlights=match flight.ArrDelay > 0 with
                                | true -> currentResults.NumberOfDelayedFlights + 1
                                | false -> currentResults.NumberOfDelayedFlights;
        TotalArrivalDelay=match flight.ArrDelay > 0 with
                                | true -> currentResults.TotalArrivalDelay + flight.ArrDelay
                                | false -> currentResults.TotalArrivalDelay;
    }
    flightAggregates.[flight.DestST] <- newResults

let writeDelays (flightAggregates:System.Collections.Generic.Dictionary<string, FlightAggregates>) =
    let delaySP = new UpdateDelayByState(connectionString)
    let aggs = new List<TVP>()
    flightAggregates |>
        Seq.iter(fun f -> aggs.Add(new TVP(f.Key, f.Value.TotalNumberOfFlights, f.Value.NumberOfDelayedFlights, f.Value.TotalArrivalDelay)))
    delaySP.Execute(aggs) |> ignore

[<EntryPoint>]
let main argv = 
    let stopWatch = new System.Diagnostics.Stopwatch()
    stopWatch.Start()

    Console.BackgroundColor <- ConsoleColor.DarkCyan
    Console.ForegroundColor <- ConsoleColor.White
    Console.Clear()

    let mutable flightAggregates = new Dictionary<string, FlightAggregates>()

    //Pull enriched queue items from the EnrichedFlights topic.
    let config = new Dictionary<string, Object>()
    config.Add("bootstrap.servers", "clusterino:6667")
    config.Add("group.id", "airplane-consumer")
    //Settings for how frequently we update the consumer offset
    config.Add("enable.auto.commit", true)
    config.Add("auto.commit.interval.ms", 5000)
    //Increase throughput for consumer
    config.Add("fetch.wait.max.ms", 5000)
    config.Add("fetch.min.bytes", 1000)
    //I'm using Kafka 0.10.0 or later, so I want to set this to true.
    config.Add("api.version.request", true)
    //config.Add("statistics.interval.ms", 10000)

    //I want to default to starting at the smallest offset in the partition if we don't already have a valid offset.
    let topicConfig = new Dictionary<string, Object>()
    topicConfig.Add("auto.offset.reset", "earliest")
    config.Add("default.topic.config", topicConfig)

    let enrichedFlightsTopic = "EnrichedFlights"

    use consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8))

    let mutable x = 0
    consumer.OnMessage.Add(fun(msg) ->
        //Write our enriched flight into a set of aggregates.
        processMessage msg.Value flightAggregates

        //Every once in a while, spit out a message to let us know we're still working.
        x <- x + 1
        if x % 10000 = 0 then
            printfn "Read in %i messages" x
    )

    //Whenever we initially assing partitions, start from the beginning.
    //Because I changed auto.offset.reset above to "smallest," should I choose Offset.Stored,
    //it would default to the beginning of the partition, but would pick up the current offset
    //if I were to stop and restart the app.
    consumer.OnPartitionsAssigned.Add(fun(part) ->
        let fromLastPoint = List.ofSeq part
                               |> List.map(fun(x) -> new TopicPartitionOffset(x.Topic, x.Partition, Offset.Beginning))  
        let fb = new System.Collections.Generic.List<TopicPartitionOffset>(fromLastPoint |> List.toSeq)
        consumer.Assign(fb);
    )

    consumer.OnPartitionsRevoked.Add(fun(part) ->
        consumer.Unassign()
    )

    consumer.Subscribe(enrichedFlightsTopic)

    //Continuously poll for messages.
    let rec loop() =
        //Loop for a few seconds, collecting messages.
        consumer.Poll(TimeSpan.FromSeconds(10.))

        //Every once in a while, get the aggregates and write them to SQL Server.
        if x % 500000 = 0 then
            writeDelays flightAggregates
            //Now reset the aggregates and keep going if we didn't hit the Enter key in between.
            flightAggregates <- new Dictionary<string, FlightAggregates>()
            printfn "Wrote a batch to SQL Server.  Elapsed time: %A" stopWatch.Elapsed

        if Console.KeyAvailable then
            match Console.ReadKey().Key with
            | ConsoleKey.Enter -> ()
            | _ -> loop()
        else
            loop()
    
    //Because consumer is an event, we leave it to the user to decide when to quit.
    //This allows for sampling the stream rather than needing to wait until its conclusion.
    printfn "Started reader. Press enter to finalize calculations and display results."
    loop()
    //Write the last batch to SQL Server.
    writeDelays flightAggregates

    printfn "Elapsed time: %A" stopWatch.Elapsed
    stopWatch.Stop()
    printfn "You've finished pulling enriched data.  You stopped after %A.  Now check SQL Server for the aggregated data." stopWatch.Elapsed
    System.Console.ReadLine() |> ignore

    0 // return an integer exit code