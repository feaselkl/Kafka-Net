module AirplaneEnricher

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

type AirportSql = 
    SqlCommandProvider<"SELECT IATA, Airport, City, State, Country, Lat, Long FROM dbo.Airports", connectionString>

//Step 2:  pull flight data from the initial queue, perform enrichment, and throw results onto a cleansed queue.
//Sample initial queue item:
//2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA
//Data cleansing operations include:
    //Dealing with potentially missing columns
    //Converting NA into valid data values (Option<Int>?)
    //Building a Flight type to handle results
//Enrichment operations include:
    //Hitting a SQL Server database for airport codes.
    //Performing airport lookups & adding to Flight type
//Convert enriched data into JSON object
//Write to enriched queue
//Step 3 will pull data from the enriched queue and perform filters & groups as desired.

type EnrichedFlight = { Date:DateTime; DepTime:Option<int>; ArrTime:Option<int>; UniqueCarrier:string; ArrDelay:Option<int>; DepDelay:Option<int>;
                Origin:string; Dest:string; OriginCity:string; OriginState:string; DestinationCity:string; DestinationState:string; }

let filterNA (str:string) =
    let x = match str with
            | "NA" -> None
            | _ -> Some(Convert.ToInt32(str))
    x

let convertNAint (str:string) =
    let x = match str with
            | "NA" -> 0
            | _ -> Convert.ToInt32(str)
    x

let convertNAbool (str:string) =
    let x = match str with
            | "NA" -> false
            | "0" -> false
            | "1" -> true
            | _ -> false
    x

let buildFlight (rawFlightMessage:string) (airports:System.Collections.Generic.IEnumerable<AirportSql.Record>) =
    //rawFlightMessage looks like:
    //2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA

    let flightSplit = rawFlightMessage.Split(',')
    let origin = airports |> Seq.filter(fun f -> f.IATA.Value.Equals(flightSplit.[16])) |> Seq.head
    let destination = airports |> Seq.filter(fun f -> f.IATA.Value.Equals(flightSplit.[17])) |> Seq.head

    let flight = {
        Date = new DateTime(Convert.ToInt32(flightSplit.[0]), Convert.ToInt32(flightSplit.[1]), Convert.ToInt32(flightSplit.[2]));
        DepTime = filterNA flightSplit.[4];
        ArrTime = filterNA flightSplit.[6];
        UniqueCarrier = flightSplit.[8];
        ArrDelay = filterNA flightSplit.[14];
        DepDelay = filterNA flightSplit.[15];
        Origin = flightSplit.[16];
        Dest = flightSplit.[17];
        OriginCity = origin.City.Value;
        OriginState = origin.State.Value;
        DestinationCity = destination.City.Value;
        DestinationState = destination.State.Value;
    }
    flight

let publish (producer:Producer<Null, string>) (topic:string) (text:string) =
    producer.ProduceAsync(topic, null, text) |> ignore

let processMessage (message:string) (airports:System.Collections.Generic.IEnumerable<AirportSql.Record>) =
    let flight = buildFlight message airports
    let jsonFlight = JsonConvert.SerializeObject(flight)
    jsonFlight

[<EntryPoint>]
let main argv = 
    let stopWatch = new System.Diagnostics.Stopwatch()
    stopWatch.Start()

    Console.BackgroundColor <- ConsoleColor.DarkMagenta
    Console.ForegroundColor <- ConsoleColor.White
    Console.Clear()

    //Build up airport metadata before we begin retrieving data from Kafka.
    let conn = new System.Data.SqlClient.SqlConnection(connectionString)
    conn.Open()
    let airports = AirportSql.Create(conn).Execute() |> Seq.toList
    conn.Close()

    //Pull unrefined queue items from the flights queue.
    let config = new Dictionary<string, Object>()
    config.Add("bootstrap.servers", "clusterino:6667")
    config.Add("group.id", "airplane-enricher")
    config.Add("enable.auto.commit", true)
    config.Add("auto.commit.interval.ms", 5000)
    //config.Add("statistics.interval.ms", 10000)

    let flightsTopic = "Flights"
    let enrichedFlightsTopic = "EnrichedFlights2"

    //We need a consumer to read from Flights and a producer to write to EnrichedFlights.
    use producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8))
    use consumer = new Consumer<Null, string>(config, null, new StringDeserializer(Encoding.UTF8))

    let mutable x = 0
    consumer.OnMessage.Add(fun(msg) ->
        //Convert our raw message into an enriched message
        let processedMessage = processMessage msg.Value airports
        //Write the enriched message out to the enriched flights topic
        publish producer enrichedFlightsTopic processedMessage

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

    consumer.Subscribe(flightsTopic)

    //Continuously poll for messages.
    let rec loop() =
        consumer.Poll(TimeSpan.FromMilliseconds(5000.))

        if Console.KeyAvailable then
            match Console.ReadKey().Key with
            | ConsoleKey.Enter -> ()
            | _ -> loop()
        else
            loop()
    
    printfn "Started enricher. Press enter to stop enriching."
    loop()
    stopWatch.Stop()
    printfn "You've finished enriching some data.  You stopped after %A.  Hit Enter to close this app." stopWatch.Elapsed

    System.Console.ReadLine() |> ignore

    0 // return an integer exit code
