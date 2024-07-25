(*
 * Copyright (c) 2018-2022 Tarides <contact@tarides.com>
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

open Lwt.Syntax
open Irmin_client_unix
open Util
module Info = Info (Client.Info)

let info = Info.v

module KV = Irmin_mem.KV.Make (Irmin.Contents.String)
module Store = Irmin_client_unix.Make (KV)

let suite kind =
  let kind, uri, serve = run_server kind in
  let config = Irmin_client_unix.config uri in
  let server = ref None in
  let init ~config:_ =
    let* () =
      match !server with
      | Some s ->
          Fmt.epr "The previous server has not stopped properly";
          s ()
      | None -> Lwt.return ()
    in
    let* serve = serve () in
    server := Some serve;
    let* client = Client.Repo.v config in
    let* _ = Client.Branch.remove client "main" in
    Client.close client
  in
  let clean ~config:_ =
    match !server with
    | None -> failwith "No server started!"
    | Some stop ->
        let+ () = stop () in
        server := None
  in
  Irmin_test.Suite.create_generic_key ~name:kind
    ~store:(module Store)
    ~init ~config ~clean ()

let error =
  Alcotest.testable (Fmt.using Error.to_string Fmt.string) (fun a b ->
      Error.to_string a = Error.to_string b)

let ty t =
  Alcotest.testable
    (Fmt.using (Irmin.Type.to_string t) Fmt.string)
    (fun a b -> Irmin.Type.(unstage (equal t)) a b)

let ping () =
  let _, uri, serve = run_server `Unix_domain in
  let* stop = serve () in
  let config = Irmin_client_unix.config uri in
  let* client = Client.Repo.v config in
  Logs.debug (fun l -> l "BEFORE PING");
  let* r = Client.ping client in
  Logs.debug (fun l -> l "AFTER PING");
  let* () = Client.close client in
  let+ () = stop () in
  Alcotest.(check (result unit error)) "ping" (Ok ()) r

let misc = [ ("ping", `Quick, ping) ]
let misc = [ ("misc", misc) ]

let tests =
  let tests =
    [
      (`Quick, suite `Unix_domain);
      (`Quick, suite `Tcp);
      (`Quick, suite `Websocket);
    ]
  in
  Lwt_main.run
  @@ Irmin_test.Store.run "irmin-server" ~sleep:Lwt_unix.sleep ~misc tests
