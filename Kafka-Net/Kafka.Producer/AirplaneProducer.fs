module AirplaneProducer

open Confluent.Kafka
open Confluent.Kafka.Serialization
open System.Collections.Generic
open System
open System.Text
open System.IO
open FSharp.Collections.ParallelSeq

//Step 1:  pull flight data from a source and dump them onto an initial queue.
//In this phase, we don't want to do any data cleansing or special handling.
//We'll save that for a middle-tier client.
//Sample queue item:
//2008,1,3,4,2003,1955,2211,2225,WN,335,N712SW,128,150,116,-14,8,IAD,TPA,810,4,8,0,,0,NA,NA,NA,NA,NA

let publish (producer:Producer<Null, string>) (topic:string) (text:string) =
    producer.ProduceAsync(topic, null, text) |> ignore

let loadEntries (producer:Producer<Null, string>) (topic:string) fileName =
    File.ReadLines(fileName)
        |> PSeq.filter(fun e -> e.Length > 0)
        |> PSeq.filter(fun e -> not (e.StartsWith("Year")))   //Filter out the header row
        |> PSeq.iter(fun e -> publish producer topic e)

[<EntryPoint>]
let main argv = 
    let stopWatch = System.Diagnostics.Stopwatch.StartNew()

    Console.BackgroundColor <- ConsoleColor.DarkRed
    Console.ForegroundColor <- ConsoleColor.White
    Console.Clear()

    let config = new Dictionary<string, Object>()
    config.Add("bootstrap.servers", "clusterino:6667")
    config.Add("group.id", "airplane-producer")
    //Good settings for high throughput.  For low latency, setting queue.buffering.max.ms = 1
    //will push ASAP but send fewer messages in a block.
    config.Add("batch.num.messages", "50000")
    config.Add("queue.buffering.max.ms", "300")
    config.Add("compression.codec", "snappy")

    let topic = "Flights"
    use producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8))

    //Read and publish a list from a file
    loadEntries producer topic "C:/Temp/AirportData/2008.csv"

    stopWatch.Stop()
    printfn "Hey, we've finished loading all of the data!  It took us %A.  Hit Enter to close this app." stopWatch.Elapsed
    Console.ReadLine() |> ignore
    0 // return an integer exit code