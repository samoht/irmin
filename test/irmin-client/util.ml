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
module Store = Irmin_mem.KV.Make (Irmin.Contents.String)
module Client = Irmin_client_unix.Make (Store)
module Server = Irmin_server_unix.Make (Store)

let rec echo uri =
  Lwt.catch
    (fun () ->
      let config = Irmin_client_unix.config uri in
      let* client = Client.Repo.v config in
      let* _ = Client.Repo.heads client in
      Client.close client)
    (function
      | Unix.Unix_error (Unix.ECONNREFUSED, "connect", "") ->
          let* () = Lwt_unix.sleep 0.1 in
          echo uri
      | e -> Lwt.reraise e)

let run_server s =
  let kind, uri =
    match s with
    | `Websocket -> ("Websocket", Uri.of_string "ws://localhost:90991")
    | `Unix_domain ->
        let dir = Unix.getcwd () in
        let sock = Filename.concat dir "test.sock" in
        ("Unix_domain", Uri.of_string ("unix://" ^ sock))
    | `Tcp -> ("Tcp", Uri.of_string "tcp://localhost:90992")
  in
  let serve () =
    let stop, u = Lwt.task () in
    let conf = Irmin_mem.config () in
    let* t = Server.v ~uri conf in
    let v = Server.serve ~stop t in
    Lwt.async (fun () -> v);
    let+ () = echo uri in

    fun () ->
      Lwt.wakeup u ();
      Lwt.cancel v;
      v
  in
  (kind, uri, serve)
