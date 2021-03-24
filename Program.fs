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
                    let! (message, (replyChan: AsyncReplyChannel<unit Async>)) = inbox.Receive()
                    // Define the root error boundary -- Polly Goes Here To Retry.
                    try
                        Logging.logInfo "Event Processor" $"Attempting to process {message}"
                        handleMessage f message |> ignore
                        Logging.logInfo "EventProcessor" "Event handled successfully"
                        replyChan.Reply (async { return () })
                    with | ex ->
                        Logging.logError "Event Processor" $"{ex.Message}"
                        Logging.logInfo "Event Processor" "Retrying Now"
            })

/// Defines functionality for:
///  - Deserialize EventStore event
///  - Queues from Event Store
///  - Updates event offset after each event is processed
module StreamListener =
    let startListener (eventProcessor: MailboxProcessor<_>) (positionDb: InMemoryDbSim) =
        MailboxProcessor.Start (fun inbox ->
            async {
                while true do
                    // Get the event and position
                    let! event, position = inbox.Receive()

                    Logging.logInfo "Stream Listener" $"Received: {event}. Position: {position}"

                    // We should check the offset in the DB here
                    // - Ensure position = offset + 1.

                    // Process and block for completion
                    do!
                        eventProcessor.PostAndReply
                            (fun (replyChan: AsyncReplyChannel<unit Async>) -> event, replyChan)

                    // Update Consumer Position()
                    Logging.logInfo "Stream Listener" "Event Handled. Updating Consumer Position"
                    positionDb.UpdatePosition(position)

                    // Simulate a delay here to show that things can queue from the producer
                    do! Async.Sleep(1000)
            })

let eventHandlerFunc m =
    match m with
    | IProcessOk -> Logging.logInfo "Event Handler Func" "Doing a thing."; Ok ()
    | ICauseError -> Error "An error occurred when processing"
    | ICauseException -> failwithf "Exception during processing"

[<EntryPoint>]
let main _argv =
    let consumerOffset = InMemoryDbSim()
    let eventProcessor = EventProcessor.startProcessor eventHandlerFunc
    let streamListener = StreamListener.startListener eventProcessor consumerOffset

    let producerOnEvent (position: int64) (rawEvent: RawEvent) =
        let message = Message.deserialize rawEvent
        streamListener.Post(message, position)

    // Note producer is ephermeral. They can die and come back and it won't break consumers
    let producer = Producer(onNext = producerOnEvent)

    producer.Produce('M')
    producer.Produce('M')
    producer.Produce('M')
    producer.Produce('M')
    producer.Produce('M')
    producer.Produce('E')
    producer.Produce('T')
    producer.Produce('M')
    producer.Produce('M')
    producer.Produce('M')
    producer.Produce('M')
    producer.Produce('M')

    System.Console.ReadLine() |> ignore
    0
