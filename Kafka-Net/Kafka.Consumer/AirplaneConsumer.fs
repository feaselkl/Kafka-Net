module AirplaneConsumer

open Confluent.Kafka
open Confluent.Kafka.Serialization
open System
open System.Text
open System.Collections.Generic
open Newtonsoft.Json
open System.Collections.Generic

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

type FlightAggregates = { TotalNumberOfFlights:int; NumberOfDelayedFlights:int; TotalArrivalDelay:int }

let processMessage (message:string) (flightAggregates:System.Collections.Generic.Dictionary<string, FlightAggregates>) =
    let flight = JsonConvert.DeserializeObject<Flight>(message)
    let currentResults = match flightAggregates.ContainsKey(flight.DestinationState) with
                            | true -> flightAggregates.[flight.DestinationState]
                            | false -> { TotalNumberOfFlights=0; NumberOfDelayedFlights=0; TotalArrivalDelay=0; }
            
    let newResults = {
        TotalNumberOfFlights=currentResults.TotalNumberOfFlights + 1;
        NumberOfDelayedFlights=match flight.ArrDelay.IsSome with
                                | true -> if (flight.ArrDelay.Value > 0)
                                            then currentResults.NumberOfDelayedFlights + 1
                                            else currentResults.NumberOfDelayedFlights
                                | false -> currentResults.NumberOfDelayedFlights;
        TotalArrivalDelay=match flight.ArrDelay.IsSome with
                                | true -> if (flight.ArrDelay.Value > 0)
                                            then currentResults.TotalArrivalDelay + flight.ArrDelay.Value
                                            else currentResults.TotalArrivalDelay
                                | false -> currentResults.TotalArrivalDelay;
    }
    flightAggregates.[flight.DestinationState] <- newResults

[<EntryPoint>]
let main argv = 
    let stopWatch = new System.Diagnostics.Stopwatch()
    stopWatch.Start()

    Console.BackgroundColor <- ConsoleColor.DarkCyan
    Console.ForegroundColor <- ConsoleColor.White
    Console.Clear()

    let flightAggregates = new Dictionary<string, FlightAggregates>()

    //Pull enriched queue items from the EnrichedFlights topic.
    let config = new Dictionary<string, Object>()
    config.Add("bootstrap.servers", "clusterino:6667")
    config.Add("group.id", "airplane-consumer")
    config.Add("enable.auto.commit", true)
    config.Add("auto.commit.interval.ms", 5000)
    //config.Add("statistics.interval.ms", 10000)

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

    //Whenever we initially assing partitions, start from the beginning of the topic.
    consumer.OnPartitionsAssigned.Add(fun(part) ->
        let fromBeginning = List.ofSeq part
                               |> List.map(fun(x) -> new TopicPartitionOffset(x.Topic, x.Partition, Offset.Beginning))  
        let fb = new System.Collections.Generic.List<TopicPartitionOffset>(fromBeginning |> List.toSeq)
        consumer.Assign(fb);
    )

    consumer.OnPartitionsRevoked.Add(fun(part) ->
        consumer.Unassign()
    )

    consumer.Subscribe(enrichedFlightsTopic)

    //Continuously poll for messages.
    let rec loop() =
        consumer.Poll(TimeSpan.FromMilliseconds(7000.))

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
    stopWatch.Stop()
    printfn "You've finished pulling enriched data.  You stopped after %A.  Now aggregating data..." stopWatch.Elapsed
    stopWatch.Reset()
    stopWatch.Start()

    flightAggregates |>
        Seq.sortBy(fun f -> f.Key) |>
        Seq.iter(fun f -> printfn "Dest: %A. Flights: %i.  Delayed: %i.  Total Delay (min): %i.  Avg When Delayed (min): %.3f" f.Key f.Value.TotalNumberOfFlights f.Value.NumberOfDelayedFlights f.Value.TotalArrivalDelay (float(f.Value.TotalArrivalDelay)/(float(f.Value.NumberOfDelayedFlights))))

    stopWatch.Stop()
    printfn "It took %A to aggregate the data.  Press enter to quit." stopWatch.Elapsed
    System.Console.ReadLine() |> ignore

    0 // return an integer exit code
