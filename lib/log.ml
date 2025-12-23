let enabled = Atomic.make false
let enable () = Atomic.set enabled true
let disable () = Atomic.set enabled false
let mutex = Mutex.create ()

let log fmt =
  Printf.ksprintf
    (fun msg ->
       if Atomic.get enabled
       then Mutex.protect mutex (fun () -> Printf.eprintf "%s\n%!" msg))
    fmt
;;

let log_tid tid fmt = log ("[tid=%s] " ^^ fmt) (Transaction_id.show tid)
let log_page page fmt = log ("[page=%s] " ^^ fmt) (Page_key.show page)

let log_tid_page tid page fmt =
  log ("[tid=%s page=%s] " ^^ fmt) (Transaction_id.show tid) (Page_key.show page)
;;
