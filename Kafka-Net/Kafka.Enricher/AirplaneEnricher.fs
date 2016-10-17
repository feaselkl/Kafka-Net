module AirplaneEnricher

open RdKafka
open System
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

type Flight = { Year:int; Month:int; DayOfMonth:int; DayOfWeek:int; DepTime:Option<int>; CRSDepTime:int; ArrTime:Option<int>; CRSArrTime:int; 
                UniqueCarrier:string; FlightNum:string; TailNum:string; ActualElapsedTime:Option<int>; CRSElapsedTime:Option<int>; AirTime:Option<int>;
                ArrDelay:Option<int>; DepDelay:Option<int>; Origin:string; Dest:string; Distance:int; TaxiIn:Option<int>; TaxiOut:Option<int>; 
                Cancelled:bool; CancellationCode:string; Diverted:bool; CarrierDelay:int; WeatherDelay:int; NASDelay:int; SecurityDelay:int; 
                LateAircraftDelay:int; OriginCity:string; OriginState:string; DestinationCity:string; DestinationState:string; }

let readFromBeginning (consumer:EventConsumer) =
    consumer.OnPartitionsAssigned.Add(fun(partitions) -> 
        printfn "Starting from the beginning..."
        let fromBeginning = List.ofSeq partitions
                               |> List.map(fun(x) -> new TopicPartitionOffset(x.Topic, x.Partition, RdKafka.Offset.Beginning))  
        let fb = new System.Collections.Generic.List<TopicPartitionOffset>(fromBeginning |> List.toSeq)
        consumer.Assign(fb);
    )

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
        Year = Convert.ToInt32(flightSplit.[0]);
        Month = Convert.ToInt32(flightSplit.[1]);
        DayOfMonth = Convert.ToInt32(flightSplit.[2]);
        DayOfWeek = Convert.ToInt32(flightSplit.[3]);
        DepTime = filterNA flightSplit.[4];
        CRSDepTime = Convert.ToInt32(flightSplit.[5]);
        ArrTime = filterNA flightSplit.[6];
        CRSArrTime = Convert.ToInt32(flightSplit.[7]);
        UniqueCarrier = flightSplit.[8];
        FlightNum = flightSplit.[9];
        TailNum = flightSplit.[10];
        ActualElapsedTime = filterNA flightSplit.[11];
        CRSElapsedTime = filterNA flightSplit.[12];
        AirTime = filterNA flightSplit.[13];
        ArrDelay = filterNA flightSplit.[14];
        DepDelay = filterNA flightSplit.[15];
        Origin = flightSplit.[16];
        Dest = flightSplit.[17];
        Distance = Convert.ToInt32(flightSplit.[18]);
        TaxiIn = filterNA flightSplit.[19];
        TaxiOut = filterNA flightSplit.[20];
        Cancelled = convertNAbool flightSplit.[21];
        CancellationCode = flightSplit.[22];
        Diverted = convertNAbool flightSplit.[23];
        CarrierDelay = convertNAint flightSplit.[24];
        WeatherDelay = convertNAint flightSplit.[25];
        NASDelay = convertNAint flightSplit.[26];
        SecurityDelay = convertNAint flightSplit.[27];
        LateAircraftDelay = convertNAint flightSplit.[28];
        OriginCity = origin.City.Value;
        OriginState = origin.State.Value;
        DestinationCity = destination.City.Value;
        DestinationState = destination.State.Value;
    }
    flight

let publish (topic:Topic) (text:string) =
    let data = System.Text.Encoding.UTF8.GetBytes(text)
    topic.Produce(data) |> ignore

let processMessages (consumer:EventConsumer) (topic:Topic) startFromBeginning n (airports:System.Collections.Generic.IEnumerable<AirportSql.Record>) =
    if startFromBeginning then readFromBeginning consumer
    let mutable x = 0

    consumer.OnMessage.Add(fun(msg) ->
        let rawFlightMessage = System.Text.Encoding.UTF8.GetString(msg.Payload, 0, msg.Payload.Length)
        try
            //build flight type -- enrich
            let flight = buildFlight rawFlightMessage airports

            //publish
            let jsonFlight = JsonConvert.SerializeObject(flight)
            publish topic jsonFlight

            //Every once in a while, spit out a message to let us know we're still working.
            x <- x + 1
            if x % n = 0 then
                let text = rawFlightMessage.Substring(0, 30)
                printfn "Pushed mesage %i (%A)" x text
        with
        | :? System.Exception as ex -> printfn "Error:  %s ...  Message:  %s" ex.Message rawFlightMessage
    )

[<EntryPoint>]
let main argv = 
    let stopWatch = new System.Diagnostics.Stopwatch()
    stopWatch.Start()

    Console.BackgroundColor <- ConsoleColor.DarkMagenta
    Console.ForegroundColor <- ConsoleColor.White
    Console.Clear()

    //Pull unrefined queue items from the flights queue.
    let (config:RdKafka.Config) = new Config(GroupId = "Airplane Enricher")
    use consumer = new EventConsumer(config, "sandbox.hortonworks.com:6667")
    let topics = ["Flights"]
    consumer.Subscribe(new System.Collections.Generic.List<string>(topics |> List.toSeq))

    //After enrichment, put the message onto the enriched flights queue
    use producer = new Producer("sandbox.hortonworks.com:6667")
    let metadata = producer.Metadata(allTopics=true)
    use topic = producer.Topic("EnrichedFlights")

    let conn = new System.Data.SqlClient.SqlConnection(connectionString)
    conn.Open()
    let airports = AirportSql.Create(conn).Execute() |> Seq.toList
    conn.Close()

    processMessages consumer topic true 10000 airports
    consumer.Start()
    printfn "Started enricher. Press enter to stop enriching."
    System.Console.ReadLine() |> ignore
    consumer.Stop() |> ignore

    stopWatch.Stop()
    printfn "You've finished enriching some data.  You stopped after %A.  Hit Enter to close this app." stopWatch.Elapsed
    System.Console.ReadLine() |> ignore

    0 // return an integer exit code
