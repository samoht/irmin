(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt.Infix

let (/) = Filename.concat

let port name n =
  let p = ref (8189 + n) in
  String.iter (fun c ->
      p := Char.code c + !p
    ) name;
  !p

let uri name n = Fmt.kstrf Uri.of_string "http://127.0.0.1:%d" (port name n)
let pid_file n = Filename.get_temp_dir_name () / Fmt.strf "irmin-http.%d.pid" n

let http_store (module S: Irmin_test.S) =
  Irmin_test.store
    (module Irmin_http.Make(Cohttp_lwt_unix.Client)) (module S.Metadata)

(* See https://github.com/mirage/ocaml-cohttp/issues/511 *)
let () = Lwt.async_exception_hook := (fun e ->
    Fmt.pr "Async exception caught: %a" Fmt.exn e;
  )

let remove file = try Unix.unlink file with _ -> ()

let signal n pid =
  let pid_file = pid_file n in
  let oc = open_out pid_file in
  Logs.debug (fun l -> l "write PID %d in %s" pid pid_file);
  output_string oc (string_of_int pid);
  flush oc;
  close_out oc;
  Lwt.return_unit

let rec wait_for_the_server_to_start name n =
  let pid_file = pid_file n in
  if Sys.file_exists pid_file then (
    let ic = open_in pid_file in
    let line = input_line ic in
    close_in ic;
    let pid = int_of_string line in
    Logs.debug (fun l ->
        l "read PID %d fomr %s (port=%d)" pid pid_file (port name n));
    Unix.unlink pid_file;
    Lwt.return pid
  ) else (
    Logs.debug (fun l ->
        l "waiting for the server to start... (pid=%s)" pid_file);
    Lwt_unix.sleep 0.1 >>= fun () ->
    wait_for_the_server_to_start name n
  )

let servers = [
  `Quick, Test_mem.suite;
  `Quick, Test_git.suite;
]

let root c = Irmin.Private.Conf.(get c root)

let client_name n = Fmt.strf "HTTP.%s" n

let serve servers n =
  Logs.set_level ~all:true (Some Logs.Debug);
  Logs.debug (fun l -> l "pwd: %s" @@ Unix.getcwd ());
  let (_, server) = List.nth servers n in
  let name = server.Irmin_test.name in
  let port = port (client_name name) n in
  Logs.debug (fun l ->
      l "Got server: %s, port=%d root=%a"
        name port Fmt.(option string) (root server.config));
  let (module Server: Irmin_test.S) = server.store in
  let module HTTP = Irmin_http_server.Make(Cohttp_lwt_unix.Server)(Server) in
  let server () =
    server.init () >>= fun () ->
    Server.Repo.v server.config >>= fun repo ->
    signal n (Unix.getpid ()) >>= fun () ->
    let spec = HTTP.v repo ~strict:false in
    let mode = `TCP (`Port port) in
    Cohttp_lwt_unix.Server.create ~mode spec
  in
  Lwt_main.run (server ())

let () = Sys.catch_break false

let suite i server =
  let server_pid = ref 0 in
  let kill_server () =
    if !server_pid <> 0 then (
      Printf.printf "Killing %d\n%!" !server_pid;
      try Unix.kill !server_pid Sys.sigkill
      with Unix.Unix_error (Unix.ESRCH, _, _) -> ()
    ) in
  at_exit kill_server;
  let name = client_name server.Irmin_test.name in
  { Irmin_test.name;

    init = begin fun () ->
      remove (pid_file i);
      Lwt_io.flush_all () >>= fun () ->
      let _ = Sys.command @@ Fmt.strf "dune exec -- %s serve %d &" Sys.argv.(0) i in
      wait_for_the_server_to_start name i >|= fun pid ->
      server_pid := pid
    end;

    stats = None;
    clean = begin fun () ->
      kill_server ();
      let () =
        try ignore (Unix.waitpid [Unix.WUNTRACED] !server_pid)
        with _ -> ()
      in
      server.clean ()
    end;

    config = Irmin_http.config (uri name i);
    store = http_store server.store;
  }

let suites servers =
  if Sys.os_type = "Win32" then
    (* it's a bit hard to test client/server stuff on windows because
       we can't fork. Can work around that later if needed. *)
    []
  else
    List.mapi (fun i (s, server) ->
        s, suite i server
      ) servers

let with_server servers f =
  if Array.length Sys.argv = 3 && Sys.argv.(1) = "serve" then
    let n = int_of_string Sys.argv.(2) in
    Logs.set_reporter (Irmin_test.reporter ~prefix:"S" ());
    serve servers n
  else
    f ()
