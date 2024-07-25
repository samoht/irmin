module Client : Irmin_client.S with type Schema.Branch.t = string

val run_server :
  [ `Websocket | `Tcp | `Unix_domain ] ->
  string * Uri.t * (unit -> (unit -> unit Lwt.t) Lwt.t)
