type RawEvent = char

type Message = | IProcessOk | ICauseException | ICauseError
module Message =
    let deserialize = function
        | 'M' -> IProcessOk
        | 'T' -> ICauseException
        | 'E' -> ICauseError
        | other -> failwith $"Deserialization Failure. {other: char} is not a valid case of {nameof Message}"

type MessageHandler = Message -> Result<unit, string>

/// Simulate a basic stream subscription with call back
/// - This will be initialised at the current stream offset
/// - Invoke `Produce()` to simulate events
type Producer(onNext: int64 -> RawEvent -> unit) =
    member val pos = 0L with get, set
    member this.IncrementPosition () =
        this.pos <- this.pos + 1L
        this.pos

    member this.Produce (e: RawEvent) =
        let nextPosition = this.IncrementPosition()
        Logging.logInfo "Producer" $"Producing Event: {e} with Position: {nextPosition}"
        onNext nextPosition e

/// Simulate a DB Row for Offset
type InMemoryDbSim(?initial: int64) =
    member val Position = initial |> Option.defaultValue 0L with get, set

    member this.UpdatePosition(p: int64) =
        Logging.logInfo (nameof InMemoryDbSim) $"Updating position to {p}"
        this.Position <- p

module ErrorHandling =
    open Polly
    let logOnRetry (exn: System.Exception) (ts: System.TimeSpan) =
        Logging.logError "Polly Handler" $"Error: {exn.Message}"
        Logging.logInfo "Polly Handler" $"Retrying in {ts.TotalSeconds} seconds"

    let policy =
        Policy
            .Handle<System.Exception>()
            .WaitAndRetryForever((fun _ -> System.TimeSpan.FromSeconds(2.0)), onRetry=logOnRetry)

module EventProcessor =
    // It should be noted that f is allowed to throw exceptions
    let handleMessage (f: MessageHandler) (m: Message) =
        match f m with
        | Ok _ -> ()
        | Error e -> failwith $"{e}"

    let startProcessor (f: MessageHandler) =
        // This even takes an optional cancellation token!
        MailboxProcessor.Start (fun inbox ->
            async {
                while true do
                    let! (message, (replyChan: AsyncReplyChannel<_ Async>)) = inbox.Receive()

                    try
                        Logging.logInfo "Event Processor" $"Attempting to process {message}"
                        handleMessage f message |> ignore
                        Logging.logInfo "EventProcessor" "Event handled successfully"

                        () |> Ok |> async.Return |> replyChan.Reply // ACK

                    with | ex -> ex.Message |> Error |> async.Return |> replyChan.Reply // FAIL
            })

/// Defines functionality for:
///  - Deserialize EventStore event
///  - Queues from Event Store
///  - Updates event offset after each event is processed
module StreamListener =
    type HandlerResult =
        | Processed
        | Skipped

    let assertNext (db: InMemoryDbSim) p = (db.Position + 1L) = p

    let execute (eventProcessor: MailboxProcessor<_>) (positionDb: InMemoryDbSim) (position: int64) (event: Message) =
        // Check that the event position is next up otherwise skip it
        if assertNext positionDb position
        then
            // Send the event to the processor
            let result =
                eventProcessor.PostAndReply
                     (fun (replyChan: AsyncReplyChannel<_ Async>) -> event, replyChan)
                |> Async.RunSynchronously

            match result with
            | Ok () -> Processed
            | Error ex -> failwith $"{ex}"

        else Skipped

    let startListener (eventProcessor: MailboxProcessor<_>) (positionDb: InMemoryDbSim) =
        MailboxProcessor.Start (fun inbox ->
            async {
                let executor = execute eventProcessor positionDb

                while true do
                    // Get the event and position
                    let! event, position = inbox.Receive()
                    Logging.logInfo "Stream Listener" $"Received: {event}. Position: {position}"

                    match ErrorHandling.policy.Execute(fun _ -> executor position event) with
                    | Processed ->
                        Logging.logInfo "Stream Listener" "Event Handled. Updating Consumer Position"
                        positionDb.UpdatePosition(position)
                    | Skipped ->
                        Logging.logInfo "Stream Listener" $"Event number {position} skipped. Consumer Position is Ahead"

                    // Simulate a delay here to show that things can queue from the producer
                    do! Async.Sleep 100
            })

let eventHandlerFunc m =
    match m with
    | IProcessOk -> Logging.logInfo "Event Handler Func" "Doing a thing."; Ok ()
    | ICauseError -> Error "An error occurred when processing"
    | ICauseException -> failwithf "Exception during processing"

[<EntryPoint>]
let main _argv =
    let consumerOffset = InMemoryDbSim(8L)
    let eventProcessor = EventProcessor.startProcessor eventHandlerFunc
    let streamListener = StreamListener.startListener eventProcessor consumerOffset

    let producerOnEvent (position: int64) (rawEvent: RawEvent) =
        let message = Message.deserialize rawEvent
        streamListener.Post(message, position)

    // Note producer is ephermeral. They can die and come back and it won't break consumers
    let producer = Producer(onNext = producerOnEvent)

    // Seed 10 Events
    let i = [1..10] |> List.map (fun a -> producer.Produce('M'))

    // Produce an Erroneous Case
    producer.Produce('M')
    producer.Produce('E')

    producer.Produce('M')
    producer.Produce('M')
    producer.Produce('M')

    System.Threading.Thread.Sleep(5000)

    // Simulate a manual bump
    Logging.logBanner "Main" "Simulating manual offset bump!"
    consumerOffset.UpdatePosition(13L)

    System.Console.ReadLine() |> ignore

    0
