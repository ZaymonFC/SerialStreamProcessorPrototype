module Logging
open System

let private blue = ConsoleColor.DarkBlue
let private white = ConsoleColor.White
let private black = ConsoleColor.Black
let private red = ConsoleColor.Red

let lockObj = obj()
let private logWithColor fc bg (sev: string) (logLine: string) =
    lock lockObj (fun () ->
        Console.ForegroundColor <- fc
        Console.BackgroundColor <- bg
        Console.Write(sev)
        Console.ResetColor()
        Console.WriteLine($" {logLine}"))

let logInfo componentName message =
    let logLine = $"[{componentName}]: {message}"
    let sev = "INF:"
    logWithColor white blue sev logLine

let logError componentName message =
    let logLine = $"[{componentName}]: {message}"
    let sev = "ERR:"
    logWithColor red black sev logLine
