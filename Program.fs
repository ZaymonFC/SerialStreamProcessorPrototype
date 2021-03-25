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

    member val mutex = obj()

    member this.UpdatePosition () =
        lock this.mutex (fun _ ->
            let newPosition = this.Position + 1L
            Logging.logInfo (nameof InMemoryDbSim) $"Updating position to {newPosition}"
            this.Position <- newPosition)

module ErrorHandling =
    open Polly
    let logOnRetry (exn: System.Exception) (ts: System.TimeSpan) =
        Logging.logError "Polly Handler" $"Error: {exn.Message}"
        Logging.logInfo "Polly Handler" $"Retrying in {ts.TotalSeconds} seconds"

    let policy =
        Policy
            .Handle<System.Exception>()
            .WaitAndRetryForever((fun _ -> System.TimeSpan.FromSeconds(2.0)), onRetry=logOnRetry)

type EventProcessorReply = AsyncReplyChannel<Result<unit, string>>
type EventProcessor<'a> = MailboxProcessor<'a * EventProcessorReply>

module EventProcessor =
    // It should be noted that f is allowed to throw exceptions
    let handleMessage (f: MessageHandler) (m: Message) =
        match f m with
        | Ok _ -> ()
        | Error e -> failwith $"{e}"

    let startEventProcessor (f: MessageHandler) : EventProcessor<_> =
        MailboxProcessor.Start (fun inbox -> async {
            while true do
                let! (message, (chan: AsyncReplyChannel<Result<_,_>>)) = inbox.Receive()
                try
                    Logging.logInfo "Event Processor" $"Attempting to process: {message}"
                    handleMessage f message |> ignore
                    Logging.logInfo "EventProcessor" "Event handled successfully"

                    () |> Ok |> chan.Reply // ACK
                with | ex -> ex.Message |> Error |> chan.Reply // FAIL
        })

type ConsumerOffset = int64

type StreamListener<'a> = MailboxProcessor<'a * ConsumerOffset>
module StreamListener =
    type HandlerResult =
        | Processed
        | Skipped

    let executeWithFastForward (eventProcessor: EventProcessor<_>) (positionDb: InMemoryDbSim) (position: int64) (event: Message) =
        // Check that the event position is next up otherwise skip it
        if positionDb.Position = position
        then
            // Send the event to the processor
            let result = eventProcessor.PostAndReply (fun (chan: EventProcessorReply) -> event, chan)

            match result with
            | Ok () -> Processed
            | Error ex -> failwith $"{ex}"

        else Skipped

    let startListener (eventProcessor: EventProcessor<_>) (positionDb: InMemoryDbSim) : StreamListener<_> =
        MailboxProcessor.Start (fun inbox ->
            async {
                let executor = executeWithFastForward eventProcessor positionDb

                while true do
                    // Get the event and position
                    let! event, position = inbox.Receive()
                    Logging.logInfo "Stream Listener" $"Received: {event}. Position: {position}"

                    match ErrorHandling.policy.Execute(fun _ -> executor position event) with
                    | Processed ->
                        Logging.logInfo "Stream Listener" "Event Handled. Updating Consumer Position"
                        positionDb.UpdatePosition()
                    | Skipped ->
                        Logging.logInfo "Stream Listener" $"Event number {position} skipped. Consumer Position is Ahead"

                    // Simulate a delay here to show that things can queue from the producer
                    do! Async.Sleep 150
            })

let eventHandlerFunc m =
    match m with
    | IProcessOk -> Logging.logInfo "Event Handler Func" "Doing a thing."; Ok ()
    | ICauseError -> Error "An error occurred when processing"
    | ICauseException -> failwithf "Exception during processing"

[<EntryPoint>]
let main _argv =
    let consumerOffset = InMemoryDbSim(8L)
    let eventProcessor = EventProcessor.startEventProcessor eventHandlerFunc
    let streamListener = StreamListener.startListener eventProcessor consumerOffset

    let producerOnEvent (position: int64) (rawEvent: RawEvent) =
        let message = Message.deserialize rawEvent
        streamListener.Post(message, position)

    // Note producer is ephermeral. They can die and come back and it won't break consumers
    let producer = Producer(onNext = producerOnEvent)

    Logging.logBanner "Main" "Producing Events"
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
    consumerOffset.Position <- 13L

    System.Console.ReadLine() |> ignore

    0
