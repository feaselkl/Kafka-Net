module AirplaneConsumer

open Confluent.Kafka
open Confluent.Kafka.Serialization
open System
open System.Text
open System.Collections.Generic
open Newtonsoft.Json
open System.Collections.Generic

//Step 2 bonus:  stream the EnrichedFlights2 topic and pick out delayed Raleigh-Durham flights.
//This shows that we can read the stream as the enricher adds rows.
type Flight = { ArrDelay:int; Origin:string; Dest:string; DestST:string; }

let processMessage (message:string) =
    let flight = JsonConvert.DeserializeObject<Flight>(message)
    if (flight.Dest = "RDU" && flight.ArrDelay >= 30) then
        printfn "%s to %s delayed %i minutes." flight.Origin flight.Dest flight.ArrDelay

[<EntryPoint>]
let main argv = 
    let stopWatch = new System.Diagnostics.Stopwatch()
    stopWatch.Start()

    Console.BackgroundColor <- ConsoleColor.DarkGreen
    Console.ForegroundColor <- ConsoleColor.White
    Console.Clear()

    //Pull enriched queue items from the EnrichedFlights topic.
    let config = new Dictionary<string, Object>()
    config.Add("bootstrap.servers", "clusterino:6667")
    config.Add("group.id", "airplane-rdu-streamer")
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

    consumer.OnMessage.Add(fun(msg) ->
        processMessage msg.Value
    )

    //Whenever we initially assing partitions, start from the stored value.
    //Because I changed auto.offset.reset above to "smallest," it will default to the beginning of the partition,
    //but will pick up the current offset if I stop and restart the app.
    consumer.OnPartitionsAssigned.Add(fun(part) ->
        let fromLastPoint = List.ofSeq part
                               |> List.map(fun(x) -> new TopicPartitionOffset(x.Topic, x.Partition, Offset.Stored))  
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
        consumer.Poll(TimeSpan.FromSeconds(5.))

        if Console.KeyAvailable then
            match Console.ReadKey().Key with
            | ConsoleKey.Enter -> ()
            | _ -> loop()
        else
            loop()
    
    //Because consumer is an event, we leave it to the user to decide when to quit.
    //This allows for sampling the stream rather than needing to wait until its conclusion.
    printfn "Started reader. Each long-delayed Raleigh flight will show up on the screen."
    loop()

    printfn "Elapsed time: %A" stopWatch.Elapsed
    stopWatch.Stop()
    printfn "You've finished pulling delayed Raleigh flights  This ran for %A." stopWatch.Elapsed
    System.Console.ReadLine() |> ignore

    0 // return an integer exit code