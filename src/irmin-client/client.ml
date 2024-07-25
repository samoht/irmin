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

open Import
open Irmin_server
open Lwt.Syntax
open Lwt.Infix
include Client_intf

module Conf = struct
  include Irmin.Backend.Conf

  let spec = Irmin.Backend.Conf.Spec.v "irmin-client"
  let uri = Irmin.Type.(map string) Uri.of_string Uri.to_string

  let uri =
    Irmin.Backend.Conf.key ~spec "uri" uri
      (Uri.of_string "tcp://127.0.0.1:9181")

  let tls = Irmin.Backend.Conf.key ~spec "tls" Irmin.Type.bool false

  let hostname =
    Irmin.Backend.Conf.key ~spec "hostname" Irmin.Type.string "127.0.0.1"
end

let config ?(tls = false) ?hostname uri =
  let default_host = Uri.host_with_default ~default:"127.0.0.1" uri in
  let config =
    Irmin.Backend.Conf.add (Irmin.Backend.Conf.empty Conf.spec) Conf.uri uri
  in
  let config =
    Irmin.Backend.Conf.add config Conf.hostname
      (Option.value ~default:default_host hostname)
  in
  Irmin.Backend.Conf.add config Conf.tls tls

module Client (I : IO) (Codec : Conn.Codec.S) (Store : Irmin.Generic_key.S) =
struct
  module C = Command.Make (I) (Codec) (Store)
  open C
  module IO = I

  type t = {
    ctx : IO.ctx;
    config : Conf.t;
    mutable conn : Conn.t;
    mutable closed : bool;
    lock : Lwt_mutex.t;
  }

  let pp ppf t = Conn.pp ppf t.conn

  let close t =
    t.closed <- true;
    Conn.close t.conn

  let mk_client conf =
    let uri = Conf.get conf Conf.uri in
    let hostname = Conf.get conf Conf.hostname in
    let tls = Conf.get conf Conf.tls in
    let scheme = Uri.scheme uri |> Option.value ~default:"tcp" in
    let addr = Uri.host_with_default ~default:"127.0.0.1" uri in
    let client =
      match String.lowercase_ascii scheme with
      | "unix" -> `Unix_domain_socket (`File (Uri.path uri))
      | "tcp" ->
          let port = Uri.port uri |> Option.value ~default:9181 in
          let ip = Ipaddr.of_string_exn addr in
          if not tls then `TCP (`IP ip, `Port port)
          else `TLS (`Hostname hostname, `IP ip, `Port port)
      | "ws" | "wss" -> (
          let port = Uri.port uri |> Option.value ~default:9181 in
          match Ipaddr.of_string addr with
          | Ok ip ->
              if not tls then `Ws (Some (`IP ip, `Port port), Uri.to_string uri)
              else `TLS (`Hostname hostname, `IP ip, `Port port)
          | _ -> `Ws (None, Uri.to_string uri))
      | x -> invalid_arg ("Unknown client scheme: " ^ x)
    in
    client

  let lock t f = Lwt_mutex.with_lock t.lock f [@@inline]

  let send_command_header t (module Cmd : C.CMD) =
    let header = Conn.Request.v_header ~command:Cmd.name in
    Conn.Request.write_header t.conn header

  let recv (t : t) name ty =
    let* res = Conn.Response.read_header t.conn in
    Conn.Response.get_error t.conn res >>= function
    | Some err ->
        [%log.err "[%a] Request error: command=%s, error=%s" pp t name err];
        Lwt.return_error (`Msg err)
    | None ->
        let+ x = Conn.read t.conn ty in
        [%log.debug "[%a] Completed request: command=%s" pp t name];
        x

  let request (t : t) (type x y)
      (module Cmd : C.CMD with type res = x and type req = y) (a : y) =
    if t.closed then raise Irmin.Closed
    else
      let name = Cmd.name in
      [%log.debug "[%a] Starting request: command=%s" Conn.pp t.conn name];
      lock t (fun () ->
          let* () = send_command_header t (module Cmd) in
          let* () = Conn.write t.conn Cmd.req_t a in
          recv t name Cmd.res_t)

  let recv_branch_diff (t : t) =
    let* _status = Conn.Response.read_header t.conn in
    Conn.read t.conn
      (Irmin.Type.pair Store.Branch.t (Irmin.Diff.t Store.commit_key_t))
    >|= Error.unwrap "recv_branch_diff"

  let recv_branch_key_diff (t : t) =
    let* _status = Conn.Response.read_header t.conn in
    Conn.read t.conn (Irmin.Diff.t Store.commit_key_t)
    >|= Error.unwrap "recv_branch_key_diff"
end

module Make (IO : IO) (Codec : Conn.Codec.S) (Store : Irmin.Generic_key.S) =
struct
  module Client = Client (IO) (Codec) (Store)
  module Command = Command.Make (IO) (Codec) (Store)
  module Conn = Command.Conn
  module Commands = Command.Commands

  module R = struct
    module Key = Store.Backend.Branch.Key
    module Val = Store.Backend.Branch.Val
    module W = Irmin.Backend.Watch.Make (Key) (Val)

    module Keys = Hashtbl.Make (struct
      type t = Key.t

      let hash = Hashtbl.hash
      let equal = Irmin.Type.(unstage (equal Key.t))
    end)

    (* cache the stream connections to the server: we open only one
       connection per stream kind. *)
    type cache = { mutable listeners : int; mutable stop : unit -> unit Lwt.t }

    let empty_cache () = { listeners = 0; stop = Lwt.return }

    type t = { client : Client.t; w : W.t; keys : cache Keys.t; global : cache }

    let pp ppf t =
      let x, y = W.stats t.w in
      Fmt.pf ppf "%a|%d,%d" Conn.pp t.client.conn x y

    let v client =
      { client; w = W.v (); keys = Keys.create 3; global = empty_cache () }

    let close t =
      [%log.debug "[%a] close" pp t];
      let* () = t.global.stop () in
      let acc =
        Keys.fold
          (fun _ c acc ->
            let* () = acc in
            c.stop ())
          t.keys Lwt.return_unit
      in
      let* () = acc in
      Client.close t.client
  end

  let request = Client.request

  let connect ?ctx config =
    let ctx = Option.value ~default:(Lazy.force IO.default_ctx) ctx in
    let client = Client.mk_client config in
    let* ic, oc = IO.connect ~ctx client in
    let conn = Conn.v ic oc in
    [%log.debug "[%a] send handshake (V1)" Conn.pp conn];
    let+ ok = Conn.Handshake.V1.send (module Store) conn in
    if not ok then Error.raise_error "invalid handshake (3)"
    else
      let t =
        Client.{ config; ctx; conn; closed = false; lock = Lwt_mutex.create () }
      in
      t

  let reconnect (t : R.t) =
    [%log.debug "[%a] reconnect" R.pp t];
    let* () = Lwt.catch (fun () -> R.close t) (fun _ -> Lwt.return_unit) in
    let+ conn = connect ~ctx:t.client.ctx t.client.config in
    t.client.conn <- conn.conn;
    t.client.closed <- false

  let dup (t : R.t) =
    [%log.debug "[%a] dup " R.pp t];
    let+ client = connect ~ctx:t.client.ctx t.client.config in
    let () = if t.client.closed then client.closed <- true in
    client

  let uri (t : R.t) = Conf.get t.client.config Conf.uri

  module X = struct
    open Lwt.Infix
    module Schema = Store.Schema
    module Hash = Store.Hash

    module Contents = struct
      type nonrec 'a t = Client.t

      open Commands.Contents
      module Key = Store.Backend.Contents.Key
      module Val = Store.Backend.Contents.Val
      module Hash = Store.Backend.Contents.Hash

      type key = Key.t
      type value = Val.t
      type hash = Hash.t

      let pp ppf t = Conn.pp ppf t.Client.conn
      let pp_key = Irmin.Type.pp Key.t

      let mem t key =
        [%log.debug "[%a] Contents.mem %a" pp t pp_key key];
        request t (module Mem) key >|= Error.unwrap "Contents.mem"

      let find t key =
        [%log.debug "[%a] Contents.find %a" pp t pp_key key];
        request t (module Find) key >|= Error.unwrap "Contents.find"

      let add t value =
        [%log.debug "[%a] Contents.add" pp t];
        request t (module Add) value >|= Error.unwrap "Contents.add"

      let unsafe_add t key value =
        [%log.debug "[%a] Contents.unsafe_add" pp t];
        request t (module Unsafe_add) (key, value)
        >|= Error.unwrap "Contents.unsafe_add"

      let index t hash =
        [%log.debug "[%a] Contents.index" pp t];
        request t (module Index) hash >|= Error.unwrap "Contents.index"

      let batch t f =
        [%log.debug "[%a] Contents.batch" pp t];
        f t

      let close t =
        [%log.debug "[%a] Contents.close" pp t];
        Client.close t

      let merge t =
        [%log.debug "[%a] Contents.merge" pp t];
        let f ~old a b =
          let* old = old () in
          match old with
          | Ok old ->
              request t (module Merge) (old, a, b)
              >|= Error.unwrap "Contents.merge"
          | Error e -> Lwt.return_error e
        in
        Irmin.Merge.v Irmin.Type.(option Key.t) f
    end

    module Node = struct
      type nonrec 'a t = Client.t

      open Commands.Node
      module Key = Store.Backend.Node.Key
      module Val = Store.Backend.Node.Val
      module Hash = Store.Backend.Node.Hash
      module Path = Store.Backend.Node.Path
      module Metadata = Store.Backend.Node.Metadata
      module Contents = Store.Backend.Node.Contents

      type key = Key.t
      type value = Val.t
      type hash = Hash.t

      let pp ppf t = Conn.pp ppf t.Client.conn
      let pp_key = Irmin.Type.pp Key.t

      let mem t key =
        [%log.debug "[%a] Node.close %a" pp t pp_key key];
        request t (module Mem) key >|= Error.unwrap "Node.mem"

      let find t key =
        [%log.debug "[%a] Node.find %a" pp t pp_key key];
        request t (module Find) key >|= Error.unwrap "Node.find"

      let add t value =
        [%log.debug "[%a] Node.add" pp t];
        request t (module Add) value >|= Error.unwrap "Node.add"

      let unsafe_add t key value =
        [%log.debug "[%a] Node.unsafe_add" pp t];
        request t (module Unsafe_add) (key, value)
        >|= Error.unwrap "Node.unsafe_add"

      let index t hash =
        [%log.debug "[%a] Node.index" pp t];
        request t (module Index) hash >|= Error.unwrap "Node.index"

      let batch t f =
        [%log.debug "[%a] Node.batch" pp t];
        f t

      let close t =
        [%log.debug "[%a] Node.close" pp t];
        Client.close t

      let merge t =
        [%log.debug "[%a] Node.merge" pp t];
        let f ~old a b =
          let* old = old () in
          match old with
          | Ok old ->
              request t (module Merge) (old, a, b) >|= Error.unwrap "Node.merge"
          | Error e -> Lwt.return_error e
        in
        Irmin.Merge.v Irmin.Type.(option Key.t) f
    end

    module Node_portable = Store.Backend.Node_portable

    module Commit = struct
      type nonrec 'a t = Client.t

      open Commands.Commit
      module Key = Store.Backend.Commit.Key
      module Val = Store.Backend.Commit.Val
      module Hash = Store.Backend.Commit.Hash
      module Info = Store.Backend.Commit.Info
      module Node = Node

      type key = Key.t
      type value = Val.t
      type hash = Hash.t

      let pp ppf t = Conn.pp ppf t.Client.conn
      let pp_key = Irmin.Type.pp Key.t

      let mem t key =
        [%log.debug "[%a] Commit.mem %a" pp t pp_key key];
        request t (module Mem) key >|= Error.unwrap "Commit.mem"

      let find t key =
        [%log.debug "[%a] Commit.find %a" pp t pp_key key];
        request t (module Find) key >|= Error.unwrap "Commit.find"

      let add t value =
        [%log.debug "[%a] Commit.add" pp t];
        request t (module Add) value >|= Error.unwrap "Commit.add"

      let unsafe_add t key value =
        [%log.debug "[%a] Commit.unsafe_add" pp t];
        request t (module Unsafe_add) (key, value)
        >|= Error.unwrap "Commit.unsafe_add"

      let index t hash =
        [%log.debug "[%a] Commit.index" pp t];
        request t (module Index) hash >|= Error.unwrap "Commit.index"

      let batch t f =
        [%log.debug "[%a] Commit.batch" pp t];
        f t

      let close t =
        [%log.debug "[%a] Commit.close" pp t];
        Client.close t

      let merge t ~info =
        [%log.debug "[%a] Commit.merge" pp t];
        let f ~old a b =
          let* old = old () in
          match old with
          | Ok old ->
              request t (module Merge) (info (), (old, a, b))
              >|= Error.unwrap "Node.merge"
          | Error e -> Lwt.return_error e
        in
        Irmin.Merge.v Irmin.Type.(option Key.t) f
    end

    module Commit_portable = Store.Backend.Commit_portable

    module Branch = struct
      open Commands.Branch
      include R

      type key = Key.t
      type value = Val.t
      type watch = Global of W.watch | Key of key * W.watch

      let pp_key = Irmin.Type.pp Key.t

      let mem t key =
        [%log.debug "[%a] Branch.merge %a" pp t pp_key key];
        request t.client (module Mem) key >|= Error.unwrap "Branch.mem"

      let find t key =
        [%log.debug "[%a] Branch.find %a" pp t pp_key key];
        request t.client (module Find) key >|= Error.unwrap "Branch.find"

      let set t key value =
        [%log.debug "[%a] Branch.set %a" pp t pp_key key];
        request t.client (module Set) (key, value) >|= Error.unwrap "Branch.set"

      let test_and_set t key ~test ~set =
        [%log.debug "[%a] Branch.test_and_set %a" pp t pp_key key];
        request t.client (module Test_and_set) (key, test, set)
        >|= Error.unwrap "Branch.test_and_set"

      let remove t key =
        [%log.debug "[%a] Branch.remove %a" pp t pp_key key];
        request t.client (module Remove) key >|= Error.unwrap "Branch.remove"

      let list t =
        [%log.debug "[%a] Branch.list" pp t];
        request t.client (module List) () >|= Error.unwrap "Branch.list"

      let seq f g =
        let* () = Lwt.catch f (fun _ -> Lwt.return_unit) in
        g ()

      let watch t ?init f =
        [%log.debug "[%a] Branch.watch" pp t];
        let init_stream () =
          [%log.debug "[%a] Branch.watch: init stream" pp t];
          assert (t.global.listeners = 0);
          let* client = dup t in
          let* () =
            request client (module Watch) init >|= Error.unwrap "Branch.watch"
          in
          let rec loop () =
            if client.closed || Conn.is_closed client.conn then Lwt.return_unit
            else
              seq
                (fun () ->
                  Client.recv_branch_diff client >>= fun (key, diff) ->
                  match diff with
                  | `Updated (_, v) | `Added v -> W.notify t.w key (Some v)
                  | `Removed _ -> W.notify t.w key None)
                loop
          in
          Lwt.async loop;
          t.global.listeners <- 1;
          t.global.stop <-
            (fun () ->
              let* () = Conn.write client.conn Unwatch.req_t () in
              Client.close client);
          Lwt.return_unit
        in
        let* () =
          match t.global.listeners with
          | 0 -> init_stream ()
          | n ->
              assert (n > 0);
              t.global.listeners <- 1 + t.global.listeners;
              Lwt.return_unit
        in
        let+ w = W.watch t.w ?init f in
        Global w

      let watch_key t key ?init f =
        [%log.debug "[%a] Branch.watch_key %a" pp t pp_key key];
        let init_stream cache =
          [%log.debug "[%a] Branch.watch_key %a: init stream" pp t pp_key key];
          assert (cache.listeners = 0);
          let* client = dup t in
          let* () =
            request client (module Watch_key) (init, key)
            >|= Error.unwrap "Branch.watch_key"
          in
          let rec loop () =
            if client.closed || Conn.is_closed client.conn then Lwt.return_unit
            else seq (fun () -> Client.recv_branch_key_diff client >>= f) loop
          in
          Lwt.async loop;
          cache.listeners <- 1;
          cache.stop <-
            (fun () ->
              let* () = Conn.write client.conn Unwatch.req_t () in
              Client.close client);
          Lwt.return_unit
        in
        let* () =
          match Keys.find_opt t.keys key with
          | None ->
              let cache = empty_cache () in
              Keys.add t.keys key cache;
              init_stream cache
          | Some cache ->
              assert (cache.listeners > 0);
              cache.listeners <- cache.listeners + 1;
              Lwt.return_unit
        in
        let+ w = W.watch_key t.w key ?init f in
        Key (key, w)

      let unwatch t w =
        [%log.debug "[%a] Branch.unwatch" pp t];
        match w with
        | Global w ->
            t.global.listeners <- t.global.listeners - 1;
            if t.global.listeners = 0 then (
              [%log.debug "[%a] Branch.unwatch: stop stream" pp t];
              let* () = W.unwatch t.w w in
              t.global.stop ())
            else Lwt.return_unit
        | Key (k, w) -> (
            match Keys.find_opt t.keys k with
            | None -> Lwt.return_unit
            | Some cache ->
                cache.listeners <- cache.listeners - 1;
                if cache.listeners = 0 then (
                  [%log.debug
                    "[%a] Branch.unwatch: stop stream key=%a" pp t pp_key k];
                  let* () = W.unwatch t.w w in
                  Keys.remove t.keys k;
                  cache.stop ())
                else Lwt.return_unit)

      let clear t =
        [%log.debug "[%a] Branch.clear" pp t];
        request t.client (module Clear) () >|= Error.unwrap "Branch.clear"
    end

    module Slice = Store.Backend.Slice

    module Repo = struct
      type t = Branch.t

      let v config =
        let+ client = connect config in
        Branch.v client

      let config (t : t) = t.client.config
      let close (t : t) = Client.close t.client
      let contents_t (t : t) = t.client
      let node_t (t : t) = t.client
      let commit_t (t : t) = t.client
      let branch_t (t : t) = t
      let batch (t : t) f = f t.client t.client t.client
    end

    module Remote = Irmin.Backend.Remote.None (Commit.Key) (Store.Branch)
  end

  include Irmin.Of_backend (X)

  let ping (t : repo) =
    [%log.debug "[%a] ping" R.pp t];
    request t.client (module Commands.Ping) ()

  let export ?depth (t : repo) =
    request t.client (module Commands.Export) depth >|= Error.unwrap "export"

  let import (t : repo) slice =
    request t.client (module Commands.Import) slice >|= Error.unwrap "import"

  let close (t : repo) = R.close t

  let connect ?tls ?hostname uri =
    let conf = config ?tls ?hostname uri in
    Repo.v conf

  let request_store store =
    match status store with
    | `Empty -> `Empty
    | `Branch b -> `Branch b
    | `Commit c -> `Commit (Commit.key c)

  module Batch = struct
    module Request_tree = Command.Tree

    type store = t

    type t =
      (Store.path
      * [ `Contents of
          [ `Hash of Store.Hash.t | `Value of Store.contents ]
          * Store.metadata option
        | `Tree of Request_tree.t
        | `Remove ])
      list
    [@@deriving irmin]

    let v () = []
    let remove k t = (k, `Remove) :: t

    let add_value path ?metadata value t =
      (path, `Contents (`Value value, metadata)) :: t

    let add_hash path ?metadata hash t =
      (path, `Contents (`Hash hash, metadata)) :: t

    let add_tree path tree t =
      let+ tree =
        match Tree.key tree with
        | None ->
            let+ concrete_tree = Tree.to_concrete tree in
            Request_tree.Concrete concrete_tree
        | Some key -> Request_tree.Key key |> Lwt.return
      in
      (path, `Tree tree) :: t

    let apply ~info ?(path = Store.Path.empty) store t =
      let repo = repo store in
      let store = request_store store in
      request repo.client
        (module Commands.Batch.Apply)
        ((store, path), info (), t)
      >|= Error.unwrap "Batch.apply"
  end

  (* Overrides *)

  module Commit = struct
    include Commit

    module Cache = struct
      module Key = Irmin.Backend.Lru.Make (struct
        type t = commit_key

        let hash = Hashtbl.hash
        let equal = Irmin.Type.(unstage (equal commit_key_t))
      end)

      module Hash = Irmin.Backend.Lru.Make (struct
        type t = hash

        let hash = Hashtbl.hash
        let equal = Irmin.Type.(unstage (equal hash_t))
      end)

      let key : commit Key.t = Key.create 32
      let hash : commit Hash.t = Hash.create 32
    end

    let of_key repo key =
      if Cache.Key.mem Cache.key key then
        Lwt.return_some (Cache.Key.find Cache.key key)
      else
        let+ x = of_key repo key in
        Option.iter (Cache.Key.add Cache.key key) x;
        x

    let of_hash repo hash =
      if Cache.Hash.mem Cache.hash hash then
        Lwt.return_some (Cache.Hash.find Cache.hash hash)
      else
        let+ x = of_hash repo hash in
        Option.iter (Cache.Hash.add Cache.hash hash) x;
        x
  end

  module Contents = struct
    include Contents

    module Cache = struct
      module Hash = Irmin.Backend.Lru.Make (struct
        type t = hash

        let hash = Hashtbl.hash
        let equal = Irmin.Type.(unstage (equal hash_t))
      end)

      let hash : contents Hash.t = Hash.create 32
    end

    let of_hash repo hash =
      if Cache.Hash.mem Cache.hash hash then
        Lwt.return_some (Cache.Hash.find Cache.hash hash)
      else
        let+ x = of_hash repo hash in
        Option.iter (Cache.Hash.add Cache.hash hash) x;
        x
  end

  let clone ~src ~dst =
    let repo = repo src in
    let* () =
      Head.find src >>= function
      | None -> Branch.remove repo dst
      | Some h -> Branch.set repo dst h
    in
    of_branch repo dst

  let request_store store =
    match status store with
    | `Empty -> `Empty
    | `Branch b -> `Branch b
    | `Commit c -> `Commit (Commit.key c)

  let mem store path =
    let repo = repo store in
    request repo.client (module Commands.Store.Mem) (request_store store, path)
    >|= Error.unwrap "mem"

  let mem_tree store path =
    let repo = repo store in
    request repo.client
      (module Commands.Store.Mem_tree)
      (request_store store, path)
    >|= Error.unwrap "mem_tree"

  let find store path =
    let repo = repo store in
    request repo.client (module Commands.Store.Find) (request_store store, path)
    >|= Error.unwrap "find"

  let remove_exn ?clear ?retries ?allow_empty ?parents ~info store path =
    let parents = Option.map (List.map (fun c -> Commit.hash c)) parents in
    let repo = repo store in
    request repo.client
      (module Commands.Store.Remove)
      ( ((clear, retries), (allow_empty, parents)),
        (request_store store, path),
        info () )
    >|= Error.unwrap "remove"

  let remove ?clear ?retries ?allow_empty ?parents ~info store path =
    let* x =
      remove_exn ?clear ?retries ?allow_empty ?parents ~info store path
    in
    Lwt.return_ok x

  let find_tree store path =
    let repo = repo store in
    let+ concrete =
      request repo.client
        (module Commands.Store.Find_tree)
        (request_store store, path)
      >|= Error.unwrap "find_tree"
    in
    Option.map Tree.of_concrete concrete
end
