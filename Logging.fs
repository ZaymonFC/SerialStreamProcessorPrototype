module Logging
open System

let private blue = ConsoleColor.DarkBlue
let private white = ConsoleColor.White
let private black = ConsoleColor.Black
let private red = ConsoleColor.Red

let private lockObj = obj()

let private log (s: string) =
    lock lockObj (fun () -> Console.WriteLine(s))

let private logWithColor fc bg (sev: string) (logLine: string) =
    lock lockObj (fun () ->
        Console.ForegroundColor <- fc
        Console.BackgroundColor <- bg
        Console.Write(sev)
        Console.ResetColor()
        Console.WriteLine($" {logLine}"))

let private logLine componentName message = $"[{componentName}]: {message}"

let logInfo componentName message =
    let sev = "INF:"
    logWithColor white blue sev (logLine componentName message)

let logError componentName message =
    let sev = "ERR:"
    logWithColor red black sev (logLine componentName message)

let logBanner componentName message =
    let sev = "BANNER: "
    log "---------------"
    logWithColor white blue sev (logLine componentName message)
    log "---------------"
