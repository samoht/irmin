(*
 * Copyright (c) 2013-2019 Thomas Gazagnaire <thomas@gazagnaire.org>
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

let current_version = "00000001"

let src = Logs.Src.create "irmin.pack" ~doc:"Irmin in-memory store"

module Log = (val Logs.src_log src : Logs.LOG)

let fresh_key =
  Irmin.Private.Conf.key ~doc:"Start with a fresh disk." "fresh"
    Irmin.Private.Conf.bool false

let fresh config = Irmin.Private.Conf.get config fresh_key

let root_key = Irmin.Private.Conf.root

let root config =
  match Irmin.Private.Conf.get config root_key with
  | None -> failwith "no root set"
  | Some r -> r

let config ?(fresh = false) root =
  let config = Irmin.Private.Conf.empty in
  let config = Irmin.Private.Conf.add config fresh_key fresh in
  let config = Irmin.Private.Conf.add config root_key (Some root) in
  config

let ( // ) = Filename.concat

let ( ++ ) = Int64.add

let ( -- ) = Int64.sub

type all_stats = {
  mutable index_appends : int;
  mutable index_mems : int;
  mutable index_finds : int;
  mutable index_bloomf_misses : int;
  mutable index_bloomf_mems : int;
  mutable index_is : int;
  mutable index_is_steps : int;
  mutable pack_finds : int;
  mutable pack_cache_misses : int;
  mutable pack_page_read : int;
  mutable pack_page_miss : int;
  mutable index_page_read : int;
  mutable index_page_miss : int;
  mutable appended_hashes : int;
  mutable appended_offsets : int
}

let fresh_stats () =
  { index_appends = 0;
    index_mems = 0;
    index_finds = 0;
    index_bloomf_misses = 0;
    index_bloomf_mems = 0;
    index_is = 0;
    index_is_steps = 0;
    pack_finds = 0;
    pack_cache_misses = 0;
    pack_page_read = 0;
    pack_page_miss = 0;
    index_page_read = 0;
    index_page_miss = 0;
    appended_hashes = 0;
    appended_offsets = 0
  }

let stats = fresh_stats ()

let reset_stats () =
  stats.index_appends <- 0;
  stats.index_finds <- 0;
  stats.index_mems <- 0;
  stats.index_mems <- 0;
  stats.index_bloomf_misses <- 0;
  stats.index_bloomf_mems <- 0;
  stats.index_is <- 0;
  stats.index_is_steps <- 0;
  stats.pack_finds <- 0;
  stats.pack_cache_misses <- 0;
  stats.pack_page_read <- 0;
  stats.pack_page_miss <- 0;
  stats.index_page_read <- 0;
  stats.index_page_miss <- 0;
  stats.appended_hashes <- 0;
  stats.appended_offsets <- 0;
  ()

module type IO = sig
  type t

  val v : string -> t

  val clear : t -> unit

  val append : t -> string -> unit

  val set : t -> off:int64 -> string -> unit

  val read : t -> off:int64 -> bytes -> int

  val offset : t -> int64

  val version : t -> string

  val name : t -> string

  val sync : t -> unit
end

module IO : IO = struct
  module Raw = struct
    type t = { fd : Unix.file_descr; mutable cursor : int64 }

    let v fd = { fd; cursor = 0L }

    let really_write fd buf =
      let rec aux off len =
        let w = Unix.write fd buf off len in
        if w = 0 then () else (aux [@tailcall]) (off + w) (len - w)
      in
      (aux [@tailcall]) 0 (Bytes.length buf)

    let really_read fd len buf =
      let rec aux off len =
        let r = Unix.read fd buf off len in
        if r = 0 then off (* end of file *)
        else if r = len then off + r
        else (aux [@tailcall]) (off + r) (len - r)
      in
      (aux [@tailcall]) 0 len

    let lseek t off =
      if off = t.cursor then ()
      else
        let _ = Unix.LargeFile.lseek t.fd off Unix.SEEK_SET in
        t.cursor <- off

    let unsafe_write t ~off buf =
      lseek t off;
      let buf = Bytes.unsafe_of_string buf in
      really_write t.fd buf;
      t.cursor <- off ++ Int64.of_int (Bytes.length buf)

    let unsafe_read t ~off ~len buf =
      lseek t off;
      let n = really_read t.fd len buf in
      t.cursor <- off ++ Int64.of_int n;
      n

    let unsafe_set_offset t n =
      let buf = Irmin.Type.(to_bin_string int64) n in
      unsafe_write t ~off:0L buf

    let int64_buf = Bytes.create 8

    let unsafe_get_offset t =
      let n = unsafe_read t ~off:0L ~len:8 int64_buf in
      assert (n = 8);
      match
        Irmin.Type.(of_bin_string int64) (Bytes.unsafe_to_string int64_buf)
      with
      | Ok t -> t
      | Error (`Msg e) -> Fmt.failwith "get_offset: %s" e

    let version_buf = Bytes.create 8

    let unsafe_get_version t =
      let n = unsafe_read t ~off:8L ~len:8 version_buf in
      assert (n = 8);
      Bytes.to_string version_buf

    let unsafe_set_version t = unsafe_write t ~off:8L current_version
  end

  type t = {
    file : string;
    mutable raw : Raw.t;
    mutable offset : int64;
    mutable flushed : int64;
    version : string;
    buf : Buffer.t
  }

  let header = 16L (* offset + version *)

  let sync t =
    Log.debug (fun l -> l "IO sync %s" t.file);
    let buf = Buffer.contents t.buf in
    let offset = t.offset in
    Buffer.clear t.buf;
    if buf = "" then ()
    else (
      Raw.unsafe_write t.raw ~off:t.flushed buf;
      Raw.unsafe_set_offset t.raw offset;
      (* concurrent append might happen so here t.offset might differ
         from offset *)
      if not (t.flushed ++ Int64.of_int (String.length buf) = header ++ offset)
      then
        Fmt.failwith "sync error: %s flushed=%Ld offset+header=%Ld\n%!" t.file
          t.flushed (offset ++ header);
      t.flushed <- offset ++ header )

  let auto_flush_limit = 1_000_000L

  let append t buf =
    Buffer.add_string t.buf buf;
    let len = Int64.of_int (String.length buf) in
    t.offset <- t.offset ++ len;
    if t.offset -- t.flushed > auto_flush_limit then sync t

  let set t ~off buf =
    sync t;
    Raw.unsafe_write t.raw ~off:(header ++ off) buf;
    let len = Int64.of_int (String.length buf) in
    let off = header ++ off ++ len in
    assert (off <= t.flushed)

  let read t ~off buf =
    assert (header ++ off <= t.flushed);
    Raw.unsafe_read t.raw ~off:(header ++ off) ~len:(Bytes.length buf) buf

  let offset t = t.offset

  let version t = t.version

  let name t = t.file

  let protect_unix_exn = function
    | Unix.Unix_error _ as e -> failwith (Printexc.to_string e)
    | e -> raise e

  let ignore_enoent = function
    | Unix.Unix_error (Unix.ENOENT, _, _) -> ()
    | e -> raise e

  let protect f x = try f x with e -> protect_unix_exn e

  let safe f x = try f x with e -> ignore_enoent e

  let mkdir dirname =
    let rec aux dir k =
      if Sys.file_exists dir && Sys.is_directory dir then k ()
      else (
        if Sys.file_exists dir then safe Unix.unlink dir;
        (aux [@tailcall]) (Filename.dirname dir) @@ fun () ->
        protect (Unix.mkdir dir) 0o755;
        k () )
    in
    (aux [@tailcall]) dirname (fun () -> ())

  let clear t =
    t.offset <- 0L;
    t.flushed <- header;
    Buffer.clear t.buf

  let buffers = Hashtbl.create 256

  let buffer file =
    try
      let buf = Hashtbl.find buffers file in
      Buffer.clear buf;
      buf
    with Not_found ->
      let buf = Buffer.create (4 * 1024) in
      Hashtbl.add buffers file buf;
      buf

  let () = assert (String.length current_version = 8)

  let v file =
    let v ~offset ~version raw =
      { version;
        file;
        offset;
        raw;
        buf = buffer file;
        flushed = header ++ offset
      }
    in
    mkdir (Filename.dirname file);
    match Sys.file_exists file with
    | false ->
        let x = Unix.openfile file Unix.[ O_CREAT; O_RDWR ] 0o644 in
        let raw = Raw.v x in
        Raw.unsafe_set_offset raw 0L;
        Raw.unsafe_set_version raw;
        v ~offset:0L ~version:current_version raw
    | true ->
        let x = Unix.openfile file Unix.[ O_EXCL; O_RDWR ] 0o644 in
        let raw = Raw.v x in
        let offset = Raw.unsafe_get_offset raw in
        let version = Raw.unsafe_get_version raw in
        v ~offset ~version raw
end

module Lru (H : Hashtbl.HashedType) = struct
  (* Extracted from https://github.com/pqwy/lru
     Copyright (c) 2016 David Kaloper Meršinjak *)

  module HT = Hashtbl.Make (H)

  module Q = struct
    type 'a node = {
      value : 'a;
      mutable next : 'a node option;
      mutable prev : 'a node option
    }

    type 'a t = {
      mutable first : 'a node option;
      mutable last : 'a node option
    }

    let detach t n =
      let np = n.prev and nn = n.next in
      ( match np with
      | None -> t.first <- nn
      | Some x ->
          x.next <- nn;
          n.prev <- None );
      match nn with
      | None -> t.last <- np
      | Some x ->
          x.prev <- np;
          n.next <- None

    let append t n =
      let on = Some n in
      match t.last with
      | Some x as l ->
          x.next <- on;
          t.last <- on;
          n.prev <- l
      | None ->
          t.first <- on;
          t.last <- on

    let node x = { value = x; prev = None; next = None }

    let create () = { first = None; last = None }

    let iter f t =
      let rec go f = function
        | Some n ->
            f n.value;
            go f n.next
        | _ -> ()
      in
      go f t.first
  end

  type key = HT.key

  type 'a t = {
    ht : (key * 'a) Q.node HT.t;
    q : (key * 'a) Q.t;
    mutable cap : int;
    mutable w : int
  }

  let weight t = t.w

  let create cap = { cap; w = 0; ht = HT.create cap; q = Q.create () }

  let drop_lru t =
    match t.q.first with
    | None -> ()
    | Some ({ Q.value = k, _; _ } as n) ->
        t.w <- t.w - 1;
        HT.remove t.ht k;
        Q.detach t.q n

  let remove t k =
    try
      let n = HT.find t.ht k in
      t.w <- t.w - 1;
      HT.remove t.ht k;
      Q.detach t.q n
    with Not_found -> ()

  let add t k v =
    remove t k;
    let n = Q.node (k, v) in
    t.w <- t.w + 1;
    if weight t > t.cap then drop_lru t;
    HT.add t.ht k n;
    Q.append t.q n

  let promote t k =
    try
      let n = HT.find t.ht k in
      Q.(
        detach t.q n;
        append t.q n)
    with Not_found -> ()

  let find t k =
    let v = HT.find t.ht k in
    promote t k;
    snd v.value

  let mem t k =
    match HT.mem t.ht k with
    | false -> false
    | true ->
        promote t k;
        true

  let filter f t = Q.iter (fun (k, v) -> if not (f k v) then remove t k) t.q
end

module Table (K : Irmin.Type.S) = Hashtbl.Make (struct
  type t = K.t

  let hash (t : t) = Irmin.Type.short_hash K.t t

  let equal (x : t) (y : t) = Irmin.Type.equal K.t x y
end)

module Cache (K : Irmin.Type.S) = Lru (struct
  type t = K.t

  let hash (t : t) = Irmin.Type.short_hash K.t t

  let equal (x : t) (y : t) = Irmin.Type.equal K.t x y
end)

module Pool : sig
  type t

  val v : length:int -> lru_size:int -> IO.t -> t

  val read : t -> off:int64 -> len:int -> bytes * int

  val trim : off:int64 -> t -> unit
end = struct
  module Lru = Lru (struct
    include Int64

    let hash = Hashtbl.hash
  end)

  type t = { pages : bytes Lru.t; length : int; lru_size : int; io : IO.t }

  let v ~length ~lru_size io =
    let pages = Lru.create lru_size in
    { pages; length; io; lru_size }

  let rec read t ~off ~len =
    let name = IO.name t.io in
    if Filename.check_suffix name "pack" then
      stats.pack_page_read <- succ stats.pack_page_read
    else stats.index_page_read <- succ stats.index_page_read;
    let l = Int64.of_int t.length in
    let page_off = Int64.(mul (div off l) l) in
    let ioff = Int64.to_int (off -- page_off) in
    match Lru.find t.pages page_off with
    | buf ->
        if t.length - ioff < len then (
          Lru.remove t.pages page_off;
          (read [@tailcall]) t ~off ~len )
        else (buf, ioff)
    | exception Not_found ->
        let length = max t.length (ioff + len) in
        let length =
          if page_off ++ Int64.of_int length > IO.offset t.io then
            Int64.to_int (IO.offset t.io -- page_off)
          else length
        in
        let buf = Bytes.create length in
        if Filename.check_suffix name "pack" then
          stats.pack_page_miss <- succ stats.pack_page_miss
        else stats.index_page_miss <- succ stats.index_page_miss;
        let n = IO.read t.io ~off:page_off buf in
        assert (n = length);
        Lru.add t.pages page_off buf;
        (buf, ioff)

  let trim ~off t =
    let max = off -- Int64.of_int t.length in
    Lru.filter (fun h _ -> h <= max) t.pages
end

module Dict = struct
  type t = {
    cache : (string, int) Hashtbl.t;
    index : (int, string) Hashtbl.t;
    block : IO.t
  }

  let append_string t v =
    let len = Int32.of_int (String.length v) in
    let buf = Irmin.Type.(to_bin_string int32 len) ^ v in
    IO.append t.block buf

  let index t v =
    Log.debug (fun l -> l "[dict] index %S" v);
    try Hashtbl.find t.cache v
    with Not_found ->
      let id = Hashtbl.length t.cache in
      append_string t v;
      Hashtbl.add t.cache v id;
      Hashtbl.add t.index id v;
      id

  let find t id =
    Log.debug (fun l -> l "[dict] find %d" id);
    let v = try Some (Hashtbl.find t.index id) with Not_found -> None in
    v

  let clear t =
    IO.clear t.block;
    Hashtbl.clear t.cache;
    Hashtbl.clear t.index

  let files = Hashtbl.create 10

  let v ?(fresh = false) root =
    let root = root // "store.dict" in
    Log.debug (fun l -> l "[dict] v fresh=%b root=%s" fresh root);
    try
      let t = Hashtbl.find files root in
      if fresh then clear t;
      t
    with Not_found ->
      let block = IO.v root in
      if fresh then IO.clear block;
      let cache = Hashtbl.create 997 in
      let index = Hashtbl.create 997 in
      let len = Int64.to_int (IO.offset block) in
      let raw = Bytes.create len in
      let n = IO.read block ~off:0L raw in
      assert (n = len);
      let raw = Bytes.unsafe_to_string raw in
      let rec aux n offset k =
        if offset >= len then k ()
        else
          let _, v = Irmin.Type.(decode_bin int32) raw offset in
          let len = Int32.to_int v in
          let v = String.sub raw (offset + 4) len in
          Hashtbl.add cache v n;
          Hashtbl.add index n v;
          (aux [@tailcall]) (n + 1) (offset + 4 + len) k
      in
      (aux [@tailcall]) 0 0 @@ fun () ->
      let t = { index; cache; block } in
      Hashtbl.add files root t;
      t
end

type ('k, 'v) item = { magic : char; hash : 'k; v : 'v }

let item ~magic ~hash v = { magic; hash; v }

let item_t key value =
  let open Irmin.Type in
  record "item" (fun magic hash v -> { magic; hash; v })
  |+ field "magic" char (fun v -> v.magic)
  |+ field "hash" key (fun v -> v.hash)
  |+ field "v" value (fun v -> v.v)
  |> sealr

module type S = sig
  include Irmin.Type.S

  type hash

  val magic : char

  val hash : t -> hash

  val encode_bin :
    dict:(string -> int) ->
    offset:(hash -> int64 option) ->
    (hash, t) item ->
    (string -> unit) ->
    unit

  val decode_bin :
    dict:(int -> string option) ->
    hash:(int64 -> hash) ->
    string ->
    int ->
    (hash, t) item
end

open Lwt.Infix

module Pack (K : Irmin.Hash.S) = struct
  module Index =
    Index_unix.Make (struct
        type t = K.t

        let hash = K.short_hash

        let equal = Irmin.Type.equal K.t

        let encode = Irmin.Type.to_bin_string K.t

        let decode s off = snd (Irmin.Type.decode_bin K.t s off)

        let encoded_size = K.hash_size
      end)
      (struct
        type t = int64 * int

        let encode (off, len) =
          Irmin.Type.(to_bin_string (pair int64 int32)) (off, Int32.of_int len)

        let decode s off =
          let off, len =
            snd (Irmin.Type.(decode_bin (pair int64 int32)) s off)
          in
          (off, Int32.to_int len)

        let encoded_size = (64 / 8) + (32 / 8)
      end)

  module Tbl = Table (K)

  type 'a t = {
    block : IO.t;
    index : Index.t;
    dict : Dict.t;
    lock : Lwt_mutex.t
  }

  let unsafe_clear t =
    IO.clear t.block;
    Index.clear t.index;
    Dict.clear t.dict

  let clear t =
    Lwt_mutex.with_lock t.lock (fun () ->
        unsafe_clear t;
        Lwt.return () )

  let files = Hashtbl.create 10

  let create = Lwt_mutex.create ()

  let unsafe_v ?(fresh = false) root =
    Log.debug (fun l -> l "[state] v fresh=%b root=%s" fresh root);
    let root_f = root // "store.pack" in
    try
      let t = Hashtbl.find files root_f in
      if fresh then unsafe_clear t;
      t
    with Not_found ->
      let lock = Lwt_mutex.create () in
      let index = Index.v ~fresh ~log_size:10_000_000 ~fan_out_size:256 root in
      let dict = Dict.v ~fresh root in
      let block = IO.v root_f in
      if fresh then IO.clear block;
      if IO.version block <> current_version then
        Fmt.failwith "invalid version: got %S, expecting %S" (IO.version block)
          current_version;
      let t = { block; index; lock; dict } in
      Hashtbl.add files root_f t;
      t

  let v ?fresh root =
    Lwt_mutex.with_lock create (fun () ->
        let t = unsafe_v ?fresh root in
        Lwt.return t )

  module Make (V : S with type hash := K.t) : sig
    include
      Irmin.CONTENT_ADDRESSABLE_STORE with type key = K.t and type value = V.t

    val v : ?fresh:bool -> string -> [ `Read ] t Lwt.t

    val batch : [ `Read ] t -> ([ `Read | `Write ] t -> 'a Lwt.t) -> 'a Lwt.t

    val unsafe_append : 'a t -> K.t -> V.t -> unit

    val unsafe_find : 'a t -> K.t -> V.t option
  end = struct
    module Tbl = Table (K)
    module Lru = Cache (K)

    let lru_size = 200

    let page_size = 4 * 1024

    type nonrec 'a t = {
      pack : 'a t;
      lru : V.t Lru.t;
      staging : V.t Tbl.t;
      pages : Pool.t
    }

    type key = K.t

    type value = V.t

    let clear t = clear t.pack >|= fun () -> Tbl.clear t.staging

    let files : (string, [ `Read ] t) Hashtbl.t = Hashtbl.create 10

    let create = Lwt_mutex.create ()

    let unsafe_v ?(fresh = false) root =
      Log.debug (fun l -> l "[pack] v fresh=%b root=%s" fresh root);
      try
        let t = Hashtbl.find files root in
        (if fresh then clear t else Lwt.return ()) >|= fun () -> t
      with Not_found ->
        v ~fresh root >>= fun pack ->
        let staging = Tbl.create 127 in
        let lru = Lru.create 10_000 in
        let t =
          { staging;
            lru;
            pack;
            pages = Pool.v ~lru_size ~length:page_size pack.block
          }
        in
        (if fresh then clear t else Lwt.return ()) >|= fun () ->
        Hashtbl.add files root t;
        t

    let v ?fresh root =
      Lwt_mutex.with_lock create (fun () -> unsafe_v ?fresh root)

    let pp_hash = Irmin.Type.pp K.t

    let unsafe_mem t k =
      Log.debug (fun l -> l "[pack] mem %a" pp_hash k);
      if Tbl.mem t.staging k then true
      else if Lru.mem t.lru k then true
      else Index.mem t.pack.index k

    let mem t k =
      Lwt_mutex.with_lock create (fun () ->
          let b = unsafe_mem t k in
          Lwt.return b )

    let check_key k v =
      let k' = v.hash in
      if Irmin.Type.equal K.t k k' then ()
      else
        Fmt.failwith "corrupted value: got %a, expecting %a." pp_hash k'
          pp_hash k

    let unsafe_find t k =
      Log.debug (fun l -> l "[pack] find %a" pp_hash k);
      stats.pack_finds <- succ stats.pack_finds;
      match Tbl.find t.staging k with
      | v ->
          Lru.add t.lru k v;
          Some v
      | exception Not_found -> (
        match Lru.find t.lru k with
        | v -> Some v
        | exception Not_found -> (
          match Index.find t.pack.index k with
          | None -> None
          | Some (off, len) ->
              let buf, pos = Pool.read t.pages ~off ~len in
              let hash off =
                (* match Hashtbl.find t.pack.index.offsets off with
                | e -> e.hash
                | exception Not_found -> *)
                let buf, pos = Pool.read t.pages ~off ~len:K.hash_size in
                let _, v =
                  Irmin.Type.decode_bin ~headers:false K.t
                    (Bytes.unsafe_to_string buf)
                    pos
                in
                v
              in
              let dict = Dict.find t.pack.dict in
              let v =
                V.decode_bin ~hash ~dict (Bytes.unsafe_to_string buf) pos
              in
              check_key k v;
              Tbl.add t.staging k v.v;
              Lru.add t.lru k v.v;
              stats.pack_cache_misses <- succ stats.pack_cache_misses;
              Some v.v ) )

    let find t k =
      Lwt_mutex.with_lock t.pack.lock (fun () ->
          let v = unsafe_find t k in
          Lwt.return v )

    let cast t = (t :> [ `Read | `Write ] t)

    let flush t =
      IO.sync t.pack.dict.block;
      Index.flush t.pack.index;
      let off = IO.offset t.pack.block in
      Pool.trim ~off t.pages;
      IO.sync t.pack.block;
      Tbl.clear t.staging

    let batch t f =
      f (cast t) >>= fun r ->
      if Tbl.length t.staging = 0 then Lwt.return r
      else (
        flush t;
        Lwt.return r )

    let auto_flush = 1024

    let unsafe_append t k v =
      match unsafe_mem t k with
      | true -> ()
      | false ->
          Log.debug (fun l -> l "[pack] append %a" pp_hash k);
          let offset k =
            match Index.find t.pack.index k with
            | Some (off, _) ->
                stats.appended_offsets <- stats.appended_offsets + 1;
                Some off
            | None ->
                stats.appended_hashes <- stats.appended_hashes + 1;
                None
          in
          let dict = Dict.index t.pack.dict in
          let off = IO.offset t.pack.block in
          let item = item ~magic:V.magic ~hash:k v in
          V.encode_bin ~offset ~dict item (IO.append t.pack.block);
          let len = Int64.to_int (IO.offset t.pack.block -- off) in
          Index.replace t.pack.index k (off, len);
          if Tbl.length t.staging >= auto_flush then flush t
          else Tbl.add t.staging k v;
          Lru.add t.lru k v

    let append t k v =
      Lwt_mutex.with_lock t.pack.lock (fun () ->
          unsafe_append t k v;
          Lwt.return () )

    let add t v =
      let k = V.hash v in
      append t k v >|= fun () -> k

    let unsafe_add t k v = append t k v
  end
end

module Atomic_write (K : Irmin.Type.S) (V : Irmin.Hash.S) = struct
  module Tbl = Table (K)
  module W = Irmin.Private.Watch.Make (K) (V)

  type key = K.t

  type value = V.t

  type watch = W.watch

  type t = {
    index : int64 Tbl.t;
    cache : V.t Tbl.t;
    block : IO.t;
    lock : Lwt_mutex.t;
    w : W.t
  }

  let page = Bytes.create 4

  let read_length32 ~off block =
    let n = IO.read block ~off page in
    assert (n = 4);
    let n, v = Irmin.Type.(decode_bin int32) (Bytes.unsafe_to_string page) 0 in
    assert (n = 4);
    Int32.to_int v

  let entry = Irmin.Type.(pair (string_of `Int32) V.t)

  let set_entry t ?off k v =
    let k = Irmin.Type.to_bin_string K.t k in
    let buf = Irmin.Type.to_bin_string entry (k, v) in
    match off with
    | None -> IO.append t.block buf
    | Some off -> IO.set t.block buf ~off

  let pp_branch = Irmin.Type.pp K.t

  let unsafe_find t k =
    Log.debug (fun l -> l "[branches] find %a" pp_branch k);
    try Lwt.return (Some (Tbl.find t.cache k))
    with Not_found -> Lwt.return None

  let find t k = Lwt_mutex.with_lock t.lock (fun () -> unsafe_find t k)

  let unsafe_mem t k =
    Log.debug (fun l -> l "[branches] mem %a" pp_branch k);
    try Lwt.return (Tbl.mem t.cache k) with Not_found -> Lwt.return false

  let mem t v = Lwt_mutex.with_lock t.lock (fun () -> unsafe_mem t v)

  let zero =
    match Irmin.Type.of_bin_string V.t (String.make V.hash_size '\000') with
    | Ok x -> x
    | Error _ -> assert false

  let unsafe_remove t k =
    Tbl.remove t.cache k;
    try
      let off = Tbl.find t.index k in
      set_entry t ~off k zero
    with Not_found -> ()

  let remove t k =
    Log.debug (fun l -> l "[branches] remove %a" pp_branch k);
    Lwt_mutex.with_lock t.lock (fun () ->
        unsafe_remove t k;
        Lwt.return () )
    >>= fun () -> W.notify t.w k None

  let clear t =
    W.clear t.w >|= fun () ->
    IO.clear t.block;
    Tbl.clear t.cache;
    Tbl.clear t.index

  let files = Hashtbl.create 10

  let create = Lwt_mutex.create ()

  let watches = W.v ()

  let unsafe_v ?(fresh = false) root =
    let root = root // "store.branches" in
    Log.debug (fun l -> l "[branches] v fresh=%b root=%s" fresh root);
    try
      let t = Hashtbl.find files root in
      (if fresh then clear t else Lwt.return ()) >|= fun () -> t
    with Not_found ->
      let block = IO.v root in
      if fresh then IO.clear block;
      let cache = Tbl.create 997 in
      let index = Tbl.create 997 in
      let len = IO.offset block in
      let rec aux offset k =
        if offset >= len then k ()
        else
          let len = read_length32 ~off:offset block in
          let buf = Bytes.create (len + V.hash_size) in
          let off = offset ++ 4L in
          let n = IO.read block ~off buf in
          assert (n = Bytes.length buf);
          let buf = Bytes.unsafe_to_string buf in
          let h =
            let h = String.sub buf 0 len in
            match Irmin.Type.of_bin_string K.t h with
            | Ok k -> k
            | Error (`Msg e) -> failwith e
          in
          let n, v = Irmin.Type.decode_bin V.t buf len in
          assert (n = String.length buf);
          if not (Irmin.Type.equal V.t v zero) then Tbl.add cache h v;
          Tbl.add index h offset;
          (aux [@tailcall]) (off ++ Int64.(of_int @@ (len + V.hash_size))) k
      in
      (aux [@tailcall]) 0L @@ fun () ->
      let t =
        { cache; index; block; w = watches; lock = Lwt_mutex.create () }
      in
      Hashtbl.add files root t;
      Lwt.return t

  let v ?fresh root =
    Lwt_mutex.with_lock create (fun () -> unsafe_v ?fresh root)

  let unsafe_set t k v =
    try
      let off = Tbl.find t.index k in
      Tbl.replace t.cache k v;
      set_entry t ~off k v
    with Not_found ->
      let offset = IO.offset t.block in
      set_entry t k v;
      Tbl.add t.cache k v;
      Tbl.add t.index k offset

  let set t k v =
    Log.debug (fun l -> l "[branches] set %a" pp_branch k);
    Lwt_mutex.with_lock t.lock (fun () ->
        unsafe_set t k v;
        Lwt.return () )
    >>= fun () -> W.notify t.w k (Some v)

  let unsafe_test_and_set t k ~test ~set =
    let v = try Some (Tbl.find t.cache k) with Not_found -> None in
    if not (Irmin.Type.(equal (option V.t)) v test) then Lwt.return false
    else
      let return () = Lwt.return true in
      match set with
      | None -> unsafe_remove t k |> return
      | Some v -> unsafe_set t k v |> return

  let test_and_set t k ~test ~set =
    Log.debug (fun l -> l "[branches] test-and-set %a" pp_branch k);
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_test_and_set t k ~test ~set)
    >>= function
    | true -> W.notify t.w k set >|= fun () -> true
    | false -> Lwt.return false

  let list t =
    Log.debug (fun l -> l "[branches] list");
    let keys = Tbl.fold (fun k _ acc -> k :: acc) t.cache [] in
    Lwt.return keys

  let watch_key t = W.watch_key t.w

  let watch t = W.watch t.w

  let unwatch t = W.unwatch t.w
end

module type CONFIG = sig
  val entries_per_level : int

  val stable_hash_limit : int
end

module Make_ext
    (Conf : CONFIG)
    (M : Irmin.Metadata.S)
    (C : Irmin.Contents.S)
    (P : Irmin.Path.S)
    (B : Irmin.Branch.S)
    (H : Irmin.Hash.S)
    (Node : Irmin.Private.Node.S
            with type metadata = M.t
             and type hash = H.t
             and type step = P.step)
    (Commit : Irmin.Private.Commit.S with type hash = H.t) =
struct
  module Pack = Pack (H)

  module X = struct
    module Hash = H

    module Contents = struct
      module CA = struct
        module Key = H
        module Val = C

        include Pack.Make (struct
          include Val
          module H = Irmin.Hash.Typed (H) (Val)

          let magic = 'B'

          let hash v = H.hash v

          let item_t = item_t H.t Val.t

          let encode_bin ~dict:_ ~offset:_ v = Irmin.Type.encode_bin item_t v

          let decode_bin ~dict:_ ~hash:_ s off =
            let _, t = Irmin.Type.decode_bin ~headers:false item_t s off in
            t
        end)
      end

      include Irmin.Contents.Store (CA)
    end

    module Inode = struct
      module Val = struct
        type hash = H.t

        let hash_t = H.t

        type step = P.step

        let step_t = P.step_t

        type metadata = M.t

        let metadata_t = M.t

        let default = M.default

        type value = Node.value

        let value_t = Node.value_t

        type v =
          | Node of { name : P.step; node : H.t }
          | Contents of { metadata : M.t; name : P.step; node : H.t }

        type inode = {
          index : int;
          length : int;
          mutable hash : H.t option;
          mutable tree : t option
        }

        and entry = Empty | Inode of inode | Values of v list

        and t = { cardinal : int; entries : entry array }

        let v_t : v Irmin.Type.t =
          let open Irmin.Type in
          variant "Node.v" (fun node contents contents_x -> function
            | Node n -> node (n.name, n.node)
            | Contents c ->
                if Irmin.Type.equal M.t M.default c.metadata then
                  contents (c.name, c.node)
                else contents_x (c.metadata, c.name, c.node) )
          |~ case1 "Node" (pair P.step_t H.t) (fun (name, node) ->
                 Node { name; node } )
          |~ case1 "Contents" (pair P.step_t H.t) (fun (name, node) ->
                 Contents { metadata = M.default; name; node } )
          |~ case1 "Contents-x" (triple M.t P.step_t H.t)
               (fun (metadata, name, node) -> Contents { metadata; name; node }
             )
          |> sealv

        let inode_t t : inode Irmin.Type.t =
          let open Irmin.Type in
          record "Node.inode" (fun index length hash tree ->
              { index; length; hash; tree } )
          |+ field "index" int (fun t -> t.index)
          |+ field "length" int (fun t -> t.length)
          |+ field "hash" (option H.t) (fun t -> t.hash)
          |+ field "tree" (option t) (fun t -> t.tree)
          |> sealr

        let entry_t inode : entry Irmin.Type.t =
          let open Irmin.Type in
          variant "Node.entry" (fun empty inode values -> function
            | Empty -> empty | Inode i -> inode i | Values v -> values v )
          |~ case0 "Empty" Empty
          |~ case1 "Inode" inode (fun i -> Inode i)
          |~ case1 "Values" (list v_t) (fun v -> Values v)
          |> sealv

        let t entry : t Irmin.Type.t =
          let open Irmin.Type in
          record "Node.t" (fun cardinal entries -> { cardinal; entries })
          |+ field "cardinal" int (fun t -> t.cardinal)
          |+ field "entries" (array entry) (fun t -> t.entries)
          |> sealr

        let t = Irmin.Type.mu (fun x -> t (entry_t (inode_t x)))

        module H_node = Irmin.Hash.Typed (H) (Node)

        module H_t =
          Irmin.Hash.Typed
            (H)
            (struct
              type nonrec t = t

              let t = t
            end)

        let compare_step a b = Irmin.Type.compare P.step_t a b

        let compare_v a b =
          match (a, b) with
          | Contents a, Contents b -> compare_step a.name b.name
          | Node a, Node b -> compare_step a.name b.name
          | Contents _, _ -> 1
          | _, Contents _ -> -1

        module Bin = struct
          type inode = { index : int; length : int; hash : H.t }

          type entry = Value of v | Inode of inode

          let inode : inode Irmin.Type.t =
            let open Irmin.Type in
            record "Bin.inode" (fun index length hash ->
                { index; length; hash } )
            |+ field "index" int (fun t -> t.index)
            |+ field "length" int (fun t -> t.length)
            |+ field "hash" H.t (fun t -> t.hash)
            |> sealr

          let entry : entry Irmin.Type.t =
            let open Irmin.Type in
            variant "Bin.elt" (fun value inode -> function
              | Value v -> value v | Inode i -> inode i )
            |~ case1 "Value" v_t (fun v -> Value v)
            |~ case1 "Inode" inode (fun i -> Inode i)
            |> sealv

          type t = entry list

          let t = Irmin.Type.list entry
        end

        module H_bin = Irmin.Hash.Typed (H) (Bin)

        let empty () =
          { cardinal = 0; entries = Array.make Conf.entries_per_level Empty }

        let is_empty t = t.cardinal = 0

        let v_of_value name (v : Node.value) =
          match v with
          | `Node node -> Node { name; node }
          | `Contents (node, metadata) -> Contents { metadata; name; node }

        let value_of_v : v -> P.step * Node.value = function
          | Node { node; name } -> (name, `Node node)
          | Contents { metadata; node; name } ->
              (name, `Contents (node, metadata))

        let index ~seed k =
          abs (Irmin.Type.short_hash P.step_t ~seed k)
          mod Conf.entries_per_level

        let inode ?tree ?hash ~index ~length () = { index; length; hash; tree }

        let rec of_values : type a. seed:int -> _ -> (t -> a) -> a =
         fun ~seed l k ->
          let cardinal = List.length l in
          let entries = Array.make Conf.entries_per_level Empty in
          List.iter
            (fun (s, v) ->
              let e = v_of_value s v in
              let i = index ~seed s in
              let values =
                Fmt.epr "XXX a\n%!";
                match entries.(i) with
                | Empty -> Values [ e ]
                | Values vs -> Values (e :: vs)
                | Inode _ -> assert false
              in
              entries.(i) <- values )
            l;
          Array.iteri
            (fun i -> function
              | Values vs ->
                  let length = List.length vs in
                  if length > Conf.entries_per_level then
                    of_values ~seed:(seed + 1) (List.map value_of_v vs)
                    @@ fun tree ->
                    entries.(i) <- Inode (inode ~tree ~length ~index:i ())
              | Empty -> () | Inode _ -> assert false )
            entries;
          k { cardinal; entries }

        let of_values ~seed l = of_values ~seed l (fun x -> x)

        let get_tree ~find i =
          match i.tree with
          | Some n -> n
          | None -> (
              let k = match i.hash with None -> assert false | Some k -> k in
              match find k with
              | None -> failwith "unknown key"
              | Some x ->
                  i.tree <- Some x;
                  x )

        let rec list_entry ~find acc = function
          | Empty -> acc
          | Values vs -> List.map value_of_v vs @ acc
          | Inode i -> list_values ~find acc (get_tree ~find i)

        and list_values ~find acc t =
          Array.fold_left (list_entry ~find) acc t.entries

        let list ~find t =
          let elts = list_values ~find [] t in
          assert (List.length elts = t.cardinal);
          elts

        let find_value ~seed ~find t s =
          let rec aux ~seed t =
            let i = index ~seed s in
            Fmt.epr "XXX b\n%!";
            let x = t.entries.(i) in
            match x with
            | Empty -> None
            | Values elts -> entries ~seed elts
            | Inode i -> aux ~seed:(seed + 1) (get_tree ~find i)
          and entries ~seed = function
            | [] -> None
            | (Node e as v) :: rest ->
                if Irmin.Type.equal P.step_t s e.name then
                  Some (snd (value_of_v v))
                else entries ~seed rest
            | (Contents e as v) :: rest ->
                if Irmin.Type.equal P.step_t s e.name then
                  Some (snd (value_of_v v))
                else entries ~seed rest
          in
          aux ~seed t

        let find ~find t s = find_value ~seed:0 ~find t s

        let name = function Contents { name; _ } | Node { name; _ } -> name

        let pp_step = Irmin.Type.pp P.step_t

        let rec add ~seed ~find ~copy t s v k =
          Log.debug (fun l ->
              l "Inode.add seed:%d copy:%b %a" seed copy pp_step s );
          match find_value ~seed ~find t s with
          | Some v' when Irmin.Type.equal Node.value_t v v' -> k t
          | r -> (
              let cardinal =
                match r with None -> t.cardinal + 1 | Some _ -> t.cardinal
              in
              let entries = if copy then Array.copy t.entries else t.entries in
              let i = index ~seed s in
              Fmt.epr "XXX c\n%!";
              match entries.(i) with
              | Empty ->
                  entries.(i) <- Values [ v_of_value s v ];
                  { cardinal; entries }
              | Inode n ->
                  let t = get_tree ~find n in
                  add ~seed:(seed + 1) ~find ~copy t s v @@ fun t ->
                  let inode = inode ~tree:t ~index:i ~length:t.cardinal () in
                  let entry = Inode inode in
                  entries.(i) <- entry;
                  k { cardinal; entries }
              | Values vs ->
                  let same_name e = Irmin.Type.equal P.step_t (name e) s in
                  let vs = List.filter (fun e -> not (same_name e)) vs in
                  let vs = List.fast_sort compare_v (v_of_value s v :: vs) in
                  let length = List.length vs in
                  if length > Conf.entries_per_level then
                    let vs = List.map value_of_v vs in
                    let tree = of_values ~seed:(seed + 1) vs in
                    let inode = inode ~tree ~index:i ~length () in
                    entries.(i) <- Inode inode
                  else entries.(i) <- Values vs;
                  k { cardinal; entries } )

        let rec remove ~seed ~find t s k =
          match find_value ~seed ~find t s with
          | None -> k t
          | Some _ -> (
              let cardinal = t.cardinal - 1 in
              let entries = Array.copy t.entries in
              let i = index ~seed s in
              Fmt.epr "XXX d\n%!";
              match entries.(i) with
              | Empty -> assert false
              | Values vs ->
                  let same_name e = Irmin.Type.equal P.step_t (name e) s in
                  let e =
                    match List.filter (fun e -> not (same_name e)) vs with
                    | [] -> Empty
                    | vs -> Values vs
                  in
                  entries.(i) <- e;
                  k { entries; cardinal }
              | Inode t ->
                  let t = get_tree ~find t in
                  remove ~seed:(seed + 1) ~find t s @@ fun tree ->
                  let length = tree.cardinal - 1 in
                  if length <= Conf.entries_per_level then (
                    let vs = list ~find tree in
                    let same_name e = Irmin.Type.equal P.step_t (fst e) s in
                    let vs = List.filter (fun e -> not (same_name e)) vs in
                    let vs = List.map (fun (s, v) -> v_of_value s v) vs in
                    entries.(i) <- Values vs;
                    k { entries; cardinal } )
                  else
                    let inode = inode ~tree ~index:i ~length () in
                    entries.(i) <- Inode inode;
                    k { entries; cardinal } )

        let remove ~find t s = remove ~find ~seed:0 t s (fun x -> x)

        let v l : t =
          if List.length l < Conf.entries_per_level then of_values ~seed:0 l
          else
            let aux acc (s, v) =
              add ~seed:0
                ~find:(fun _ -> assert false)
                ~copy:false acc s v
                (fun x -> x)
            in
            List.fold_left aux (empty ()) l

        let add ~find t s v = add ~seed:0 ~find ~copy:true t s v (fun x -> x)

        let to_bin t =
          assert (Array.length t.entries <= Conf.entries_per_level);
          let rec aux t k =
            let entries =
              Array.fold_left
                (fun acc -> function Empty -> acc
                  | Values vs -> List.map (fun v -> Bin.Value v) vs @ acc
                  | Inode { index; length; hash = Some hash; _ } ->
                      Inode { Bin.index; hash; length } :: acc
                  | Inode ({ index; tree = Some t; length; _ } as inode) ->
                      aux t @@ fun bin ->
                      let hash = H_bin.hash bin in
                      inode.hash <- Some hash;
                      Bin.Inode { Bin.index; hash; length } :: acc
                  | Inode { tree = None; hash = None; _ } -> assert false )
                [] t.entries
            in
            k entries
          in
          aux t (fun x -> x)

        let of_bin t =
          assert (List.length t <= Conf.entries_per_level);
          let entries = Array.make Conf.entries_per_level Empty in
          let n = ref 0 in
          List.iteri
            (fun i -> function
              | Bin.Value v -> (
                  Fmt.epr "XXX e\n%!";
                  match entries.(i) with
                  | Empty ->
                      n := !n + 1;
                      entries.(i) <- Values [ v ]
                  | Values vs ->
                      n := !n + List.length vs;
                      entries.(i) <- Values (v :: vs)
                  | Inode _ -> assert false )
              | Inode { index; length; hash } ->
                  n := !n + length;
                  let inode =
                    { index; hash = Some hash; tree = None; length }
                  in
                  entries.(i) <- Inode inode )
            t;
          { cardinal = !n; entries }

        let ignore_hash (_ : H.t) = ()

        let save ?hash ~add ~find t =
          let rec aux : type a. seed:_ -> _ -> (_ -> a) -> a =
           fun ~seed t k ->
            Log.debug (fun l -> l "save lenght:%d seed:%d" t.cardinal seed);
            if t.cardinal <= Conf.entries_per_level then k (to_bin t)
            else (
              Array.iteri
                (fun i -> function
                  | Empty | Inode { tree = None; _ } | Values [ _ ] -> ()
                  | Inode ({ tree = Some t; _ } as i) ->
                      aux ~seed:(seed + 1) t @@ fun bin ->
                      let t = of_bin bin in
                      let hash = H_t.hash t in
                      i.hash <- Some hash;
                      ignore_hash (add (H_bin.hash bin) bin)
                  | Values vs ->
                      let seed = seed + 1 in
                      let t = of_values ~seed (List.map value_of_v vs) in
                      aux ~seed t @@ fun bin ->
                      let t = of_bin bin in
                      let hash = H_t.hash t in
                      let n =
                        inode ~hash ~tree:t ~length:t.cardinal ~index:i ()
                      in
                      Fmt.epr "XXX f\n%!";
                      t.entries.(i) <- Inode n;
                      ignore_hash (add hash bin) )
                t.entries;
              k (to_bin t) )
          in
          aux ~seed:0 t (fun bin ->
              match hash with
              | Some h -> add h bin
              | None ->
                  if t.cardinal <= Conf.stable_hash_limit then
                    let values = list ~find t in
                    let hash = H_node.hash (Node.v values) in
                    add hash bin
                  else
                    let hash = H_bin.hash bin in
                    add hash bin )
      end

      module Compress = struct
        type name = Indirect of int | Direct of P.step

        type address = Indirect of int64 | Direct of H.t

        type inode = { index : int; length : int; hash : address }

        type entry =
          | Contents of name * address * M.t
          | Node of name * address
          | Inode of inode

        let entry : entry Irmin.Type.t =
          let open Irmin.Type in
          variant "Compress.entry"
            (fun contents_ii
            node_ii
            inode_i
            contents_id
            node_id
            inode_d
            contents_di
            node_di
            contents_dd
            node_dd
            -> function
            | Contents (Indirect n, Indirect h, m) -> contents_ii (n, h, m)
            | Node (Indirect n, Indirect h) -> node_ii (n, h)
            | Inode { index; length; hash = Indirect h } ->
                inode_i (index, length, h)
            | Contents (Indirect n, Direct h, m) -> contents_id (n, h, m)
            | Node (Indirect n, Direct h) -> node_id (n, h)
            | Inode { index; length; hash = Direct h } ->
                inode_d (index, length, h)
            | Contents (Direct n, Indirect h, m) -> contents_di (n, h, m)
            | Node (Direct n, Indirect h) -> node_di (n, h)
            | Contents (Direct n, Direct h, m) -> contents_dd (n, h, m)
            | Node (Direct n, Direct h) -> node_dd (n, h) )
          |~ case1 "contents-ii" (triple int int64 M.t) (fun (n, i, m) ->
                 Contents (Indirect n, Indirect i, m) )
          |~ case1 "node-ii" (pair int int64) (fun (n, i) ->
                 Node (Indirect n, Indirect i) )
          |~ case1 "inode-i" (triple int int int64) (fun (index, length, i) ->
                 Inode { index; length; hash = Indirect i } )
          |~ case1 "contents-id" (triple int H.t M.t) (fun (n, h, m) ->
                 Contents (Indirect n, Direct h, m) )
          |~ case1 "node-id" (pair int H.t) (fun (n, h) ->
                 Node (Indirect n, Direct h) )
          |~ case1 "inode-d" (triple int int H.t) (fun (index, length, h) ->
                 Inode { index; length; hash = Direct h } )
          |~ case1 "contents-di" (triple P.step_t int64 M.t) (fun (n, i, m) ->
                 Contents (Direct n, Indirect i, m) )
          |~ case1 "node-di" (pair P.step_t int64) (fun (n, i) ->
                 Node (Direct n, Indirect i) )
          |~ case1 "contents-dd" (triple P.step_t H.t M.t) (fun (n, i, m) ->
                 Contents (Direct n, Direct i, m) )
          |~ case1 "node-dd" (pair P.step_t H.t) (fun (n, i) ->
                 Node (Direct n, Direct i) )
          |> sealv

        let t = item_t H.t (Irmin.Type.list entry)
      end

      include Pack.Make (struct
        type t = H.t * Val.Bin.t

        let t = Irmin.Type.pair H.t Val.Bin.t

        let magic = 'N' (* nodes *)

        let hash t = fst t

        let encode_bin ~dict ~offset (t : (H.t, t) item) =
          let step s : Compress.name =
            let str = Irmin.Type.to_bin_string P.step_t s in
            if String.length str <= 4 then Direct s
            else
              let s = dict str in
              Indirect s
          in
          let hash h : Compress.address =
            match offset h with
            | None -> Compress.Direct h
            | Some off -> Compress.Indirect off
          in
          let inode : Val.Bin.entry -> Compress.entry = function
            | Value (Contents c) ->
                let s = step c.name in
                let v = hash c.node in
                Compress.Contents (s, v, c.metadata)
            | Value (Node n) ->
                let s = step n.name in
                let v = hash n.node in
                Compress.Node (s, v)
            | Inode { index; length; hash = h; _ } ->
                let hash = hash h in
                Compress.Inode { index; length; hash }
          in
          (* List.map is fine here as the number of entries is small *)
          let v = List.map inode (snd t.v) in
          Irmin.Type.encode_bin Compress.t
            { magic = t.magic; hash = t.hash; v }

        exception Exit of [ `Msg of string ]

        let decode_bin ~dict ~hash t off =
          let _, i = Irmin.Type.decode_bin ~headers:false Compress.t t off in
          let step : Compress.name -> P.step = function
            | Direct n -> n
            | Indirect s -> (
              match dict s with
              | None -> raise_notrace (Exit (`Msg "dict"))
              | Some s -> (
                match Irmin.Type.of_bin_string P.step_t s with
                | Error e -> raise_notrace (Exit e)
                | Ok v -> v ) )
          in
          let hash : Compress.address -> H.t = function
            | Indirect off -> hash off
            | Direct n -> n
          in
          let inode : Compress.entry -> Val.Bin.entry = function
            | Contents (n, h, metadata) ->
                let name = step n in
                let node = hash h in
                Value (Contents { name; node; metadata })
            | Node (n, h) ->
                let name = step n in
                let node = hash h in
                Value (Node { name; node })
            | Inode { index; hash = h; length } ->
                let hash = hash h in
                Inode { index; hash; length }
          in
          let v = (i.hash, List.map inode i.v) in
          { magic; hash = i.hash; v }
      end)
    end

    module Node = struct
      module CA = struct
        module Val = struct
          module I = Inode.Val

          type t = { mutable find : H.t -> I.t option; v : I.t }

          type metadata = I.metadata

          type hash = I.hash

          type step = I.step

          type value = I.value

          let niet _ = assert false

          let v l = { find = niet; v = I.v l }

          let list t = I.list ~find:t.find t.v

          let empty = { find = niet; v = Inode.Val.empty () }

          let is_empty t = I.is_empty t.v

          let find t s = I.find ~find:t.find t.v s

          let add t s v =
            let v = I.add ~find:t.find t.v s v in
            if v == t.v then t else { find = t.find; v }

          let remove t s =
            let v = I.remove ~find:t.find t.v s in
            if v == t.v then t else { find = t.find; v }

          let hash_t = I.hash_t

          let value_t = I.value_t

          let step_t = I.step_t

          let metadata_t = I.metadata_t

          let default = I.default

          let pre_hash t =
            if t.v.cardinal <= Conf.stable_hash_limit then (
              Fmt.epr "XXX 0\n%!";
              Irmin.Type.pre_hash Node.t (Node.v (I.list ~find:t.find t.v)) )
            else (
              Fmt.epr "XXX 1\n%!";
              Irmin.Type.pre_hash I.t t.v )

          let t : t Irmin.Type.t =
            Irmin.Type.map ~pre_hash I.t
              (fun v -> { find = niet; v })
              (fun t -> t.v)
        end

        module Key = H

        type 'a t = 'a Inode.t

        type key = Key.t

        type value = Val.t

        let mem t k = Inode.mem t k

        let unsafe_find t k =
          match Inode.unsafe_find t k with
          | None -> None
          | Some (_, v) -> Some (Inode.Val.of_bin v)

        let find t k =
          Inode.find t k >|= function
          | None -> None
          | Some (_, v) ->
              let v = Inode.Val.of_bin v in
              let find = unsafe_find t in
              Some { Val.find; v }

        let save ?hash t v =
          let add k v =
            Inode.unsafe_append t k (k, v);
            k
          in
          Inode.Val.save ?hash ~add ~find:(unsafe_find t) v

        let add t v =
          Fmt.epr "XXX add\n%!";
          Lwt.return (save t v.Val.v)

        let unsafe_add t k v =
          Inode.Val.ignore_hash (save ~hash:k t v.Val.v);
          Lwt.return ()

        let batch = Inode.batch

        let v = Inode.v
      end

      include Irmin.Private.Node.Store (Contents) (P) (M) (CA)
    end

    module Commit = struct
      module CA = struct
        module Key = H
        module Val = Commit

        include Pack.Make (struct
          include Val
          module H = Irmin.Hash.Typed (H) (Val)

          let hash = H.hash

          let item = item_t H.t Val.t

          let magic = 'C'

          let encode_bin ~dict:_ ~offset:_ v = Irmin.Type.encode_bin item v

          let decode_bin ~dict:_ ~hash:_ s off =
            let _, v = Irmin.Type.decode_bin ~headers:false item s off in
            v
        end)
      end

      include Irmin.Private.Commit.Store (Node) (CA)
    end

    module Branch = struct
      module Key = B
      module Val = H
      include Atomic_write (Key) (Val)
    end

    module Slice = Irmin.Private.Slice.Make (Contents) (Node) (Commit)
    module Sync = Irmin.Private.Sync.None (H) (B)

    module Repo = struct
      type t = {
        config : Irmin.Private.Conf.t;
        contents : [ `Read ] Contents.CA.t;
        node : [ `Read ] Node.CA.t;
        commit : [ `Read ] Commit.CA.t;
        branch : Branch.t
      }

      let contents_t t : 'a Contents.t = t.contents

      let node_t t : 'a Node.t = (contents_t t, t.node)

      let commit_t t : 'a Commit.t = (node_t t, t.commit)

      let branch_t t = t.branch

      let batch t f =
        Commit.CA.batch t.commit (fun commit ->
            Node.CA.batch t.node (fun node ->
                Contents.CA.batch t.contents (fun contents ->
                    let contents : 'a Contents.t = contents in
                    let node : 'a Node.t = (contents, node) in
                    let commit : 'a Commit.t = (node, commit) in
                    f contents node commit ) ) )

      let v config =
        let root = root config in
        let fresh = fresh config in
        Contents.CA.v ~fresh root >>= fun contents ->
        Node.CA.v ~fresh root >>= fun node ->
        Commit.CA.v ~fresh root >>= fun commit ->
        Branch.v ~fresh root >|= fun branch ->
        { contents; node; commit; branch; config }
    end
  end

  include Irmin.Of_private (X)
end

module Hash = Irmin.Hash.SHA1
module Path = Irmin.Path.String_list
module Metadata = Irmin.Metadata.None

module Make
    (Conf : CONFIG)
    (M : Irmin.Metadata.S)
    (C : Irmin.Contents.S)
    (P : Irmin.Path.S)
    (B : Irmin.Branch.S)
    (H : Irmin.Hash.S) =
struct
  module XNode = Irmin.Private.Node.Make (H) (P) (M)
  module XCommit = Irmin.Private.Commit.Make (H)
  include Make_ext (Conf) (M) (C) (P) (B) (H) (XNode) (XCommit)
end

module KV (Conf : CONFIG) (C : Irmin.Contents.S) =
  Make (Conf) (Metadata) (C) (Path) (Irmin.Branch.String) (Hash)

let div_or_zero a b = if b = 0 then 0. else float_of_int a /. float_of_int b

type stats = {
  bf_misses : float;
  pack_page_faults : float;
  index_page_faults : float;
  pack_cache_misses : float;
  search_steps : float;
  offset_ratio : float;
  offset_significance : int
}

let stats () =
  { bf_misses = div_or_zero stats.index_bloomf_misses stats.index_bloomf_mems;
    pack_page_faults = div_or_zero stats.pack_page_miss stats.pack_page_read;
    index_page_faults = div_or_zero stats.index_page_miss stats.index_page_read;
    pack_cache_misses = div_or_zero stats.pack_cache_misses stats.pack_finds;
    search_steps = div_or_zero stats.index_is_steps stats.index_is;
    offset_ratio =
      div_or_zero stats.appended_offsets
        (stats.appended_offsets + stats.appended_hashes);
    offset_significance = stats.appended_offsets + stats.appended_hashes
  }
