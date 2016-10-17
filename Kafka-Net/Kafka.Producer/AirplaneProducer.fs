module AirplaneProducer

open RdKafka
open System
open System.IO
open FSharp.Collections.ParallelSeq

//Step 1:  pull flight data from a source and dump them onto an initial queue.
//In this phase, we don't want to do any data cleansing or special handling.
//We'll save that for a middle-tier client.
//Sample queue item:
//2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA

let publish (topic:Topic) (text:string) =
    let data = System.Text.Encoding.UTF8.GetBytes(text)
    topic.Produce(data) |> ignore

let loadEntries (topic:Topic) fileName =
    File.ReadLines(fileName)
        |> PSeq.filter(fun e -> e.Length > 0)
        |> PSeq.filter(fun e -> not (e.StartsWith("Year")))   //Filter out the header row
        |> PSeq.iter(fun e -> publish topic e)

[<EntryPoint>]
let main argv = 
    let stopWatch = System.Diagnostics.Stopwatch.StartNew()

    Console.BackgroundColor <- ConsoleColor.DarkRed
    Console.ForegroundColor <- ConsoleColor.White
    Console.Clear()

    let topicConfig = new TopicConfig()
    topicConfig.["request.required.acks"] = "0" |> ignore
    let config = new Config();
    config.DefaultTopicConfig = topicConfig |> ignore
    config.["socket.blocking.max.ms"] = "1" |> ignore
    config.["queue.buffering.max.messages"] = "1" |> ignore
    config.["queue.buffering.max.ms"] = "1" |> ignore

    use producer = new Producer(config, "sandbox.hortonworks.com:6667")
    let metadata = producer.Metadata(allTopics=true)
    
    use topic = producer.Topic("Flights")

    //Read and publish a list from a file
    loadEntries topic "C:/Temp/AirportData/2008.csv"

    stopWatch.Stop()
    printfn "Hey, we've finished loading all of the data!  It took us %A.  Hit Enter to close this app." stopWatch.Elapsed
    Console.ReadLine() |> ignore
    0 // return an integer exit code