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

open Lwt.Infix

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

module type IO = sig
  type t

  val v : string -> t Lwt.t

  val rename : src:t -> dst:t -> unit Lwt.t

  val clear : t -> unit Lwt.t

  val append : t -> string -> unit Lwt.t

  val set : t -> off:int64 -> string -> unit Lwt.t

  val read : t -> off:int64 -> bytes -> unit Lwt.t

  val offset : t -> int64

  (*  val file : t -> string *)

  val sync : t -> unit Lwt.t
end

module IO : IO = struct
  module Raw = struct
    type t = { fd : Lwt_unix.file_descr; mutable cursor : int64 }

    let v fd = { fd; cursor = 0L }

    let really_write fd buf =
      let rec aux off len =
        Lwt_unix.write fd buf off len >>= fun w ->
        if w = 0 then Lwt.return () else aux (off + w) (len - w)
      in
      aux 0 (Bytes.length buf)

    let really_read fd buf =
      let rec aux off len =
        Lwt_unix.read fd buf off len >>= fun r ->
        if r = 0 || r = len then Lwt.return (off + r)
        else aux (off + r) (len - r)
      in
      aux 0 (Bytes.length buf)

    let lseek t off =
      if off = t.cursor then Lwt.return ()
      else
        Lwt_unix.LargeFile.lseek t.fd off Unix.SEEK_SET >|= fun _ ->
        t.cursor <- off

    let unsafe_write t ~off buf =
      lseek t off >>= fun () ->
      let buf = Bytes.unsafe_of_string buf in
      really_write t.fd buf >|= fun () ->
      t.cursor <- off ++ Int64.of_int (Bytes.length buf)

    let unsafe_read t ~off buf =
      lseek t off >>= fun () ->
      really_read t.fd buf >|= fun n -> t.cursor <- off ++ Int64.of_int n

    let unsafe_set_offset fd n =
      let buf = Irmin.Type.(to_bin_string int64) n in
      unsafe_write fd ~off:0L buf

    let unsafe_get_offset fd =
      let buf = Bytes.create 8 in
      unsafe_read fd ~off:0L buf >|= fun () ->
      match Irmin.Type.(of_bin_string int64) (Bytes.unsafe_to_string buf) with
      | Ok t -> t
      | Error (`Msg e) -> Fmt.failwith "get_offset: %s" e
  end

  type t = {
    lock : Lwt_mutex.t;
    file : string;
    mutable raw : Raw.t;
    mutable offset : int64;
    mutable flushed : int64;
    buf : Buffer.t
  }

  let header = 8L

  let unsafe_sync t =
    Log.debug (fun l -> l "IO sync %s" t.file);
    let buf = Buffer.contents t.buf in
    let offset = t.offset in
    Buffer.clear t.buf;
    if buf = "" then Lwt.return ()
    else
      Raw.unsafe_write t.raw ~off:t.flushed buf >>= fun () ->
      Raw.unsafe_set_offset t.raw offset >|= fun () ->
      (* concurrent append might happen so here t.offset might differ
         from offset *)
      if not (t.flushed ++ Int64.of_int (String.length buf) = header ++ offset)
      then
        Fmt.failwith "sync error: %s flushed=%Ld offset+header=%Ld\n%!" t.file
          t.flushed (offset ++ header);
      t.flushed <- offset ++ header

  let sync t = Lwt_mutex.with_lock t.lock (fun () -> unsafe_sync t)

  let unsafe_rename ~src ~dst =
    unsafe_sync src >>= fun () ->
    Lwt_unix.close dst.raw.fd >>= fun () ->
    Log.debug (fun l -> l "IO rename %s => %s" src.file dst.file);
    Lwt_unix.rename src.file dst.file >|= fun () ->
    dst.offset <- src.offset;
    dst.flushed <- src.flushed;
    dst.raw <- src.raw

  let rename ~src ~dst =
    Lwt_mutex.with_lock src.lock (fun () ->
        Lwt_mutex.with_lock dst.lock (fun () -> unsafe_rename ~src ~dst) )

  let auto_flush_limit = 10_000_000L

  let append t buf =
    Buffer.add_string t.buf buf;
    let len = Int64.of_int (String.length buf) in
    t.offset <- t.offset ++ len;
    if t.offset -- t.flushed > auto_flush_limit then sync t else Lwt.return ()

  let unsafe_set t ~off buf =
    unsafe_sync t >>= fun () ->
    Raw.unsafe_write t.raw ~off:(header ++ off) buf >|= fun () ->
    let len = Int64.of_int (String.length buf) in
    let off = header ++ off ++ len in
    assert (off <= t.flushed)

  let set t ~off buf =
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_set t ~off buf)

  let read t ~off buf =
    Lwt_mutex.with_lock t.lock (fun () ->
        assert (header ++ off <= t.flushed);
        Raw.unsafe_read t.raw ~off:(header ++ off) buf )

  let offset t = t.offset

  (*  let file t = t.file *)

  let protect_unix_exn = function
    | Unix.Unix_error _ as e -> Lwt.fail (Failure (Printexc.to_string e))
    | e -> Lwt.fail e

  let ignore_enoent = function
    | Unix.Unix_error (Unix.ENOENT, _, _) -> Lwt.return_unit
    | e -> Lwt.fail e

  let protect f x = Lwt.catch (fun () -> f x) protect_unix_exn

  let safe f x = Lwt.catch (fun () -> f x) ignore_enoent

  let mkdir dirname =
    let rec aux dir =
      if Sys.file_exists dir && Sys.is_directory dir then Lwt.return_unit
      else
        let clear =
          if Sys.file_exists dir then safe Lwt_unix.unlink dir
          else Lwt.return_unit
        in
        clear >>= fun () ->
        aux (Filename.dirname dir) >>= fun () ->
        protect (Lwt_unix.mkdir dir) 0o755
    in
    aux dirname

  let clear t =
    t.offset <- 0L;
    t.flushed <- header;
    Buffer.clear t.buf;
    Lwt.return ()

  let v file =
    let v ~offset raw =
      let buf = Buffer.create (1024 * 1024) in
      { file;
        lock = Lwt_mutex.create ();
        offset;
        raw;
        buf;
        flushed = header ++ offset
      }
    in
    mkdir (Filename.dirname file) >>= fun () ->
    Lwt_unix.file_exists file >>= function
    | false ->
        Lwt_unix.openfile file Unix.[ O_CREAT; O_RDWR ] 0o644 >>= fun x ->
        let raw = Raw.v x in
        Raw.unsafe_set_offset raw 0L >|= fun () -> v ~offset:0L raw
    | true ->
        Lwt_unix.openfile file Unix.[ O_EXCL; O_RDWR ] 0o644 >>= fun x ->
        let raw = Raw.v x in
        Raw.unsafe_get_offset raw >|= fun offset -> v ~offset raw
end

module Pool : sig
  type t

  val v : length:int -> lru_size:int -> IO.t -> t

  val read : t -> off:int64 -> len:int -> (bytes * int) Lwt.t

  val clear : t -> unit
end = struct
  module Lru =
    Lru.M.Make (struct
        include Int64

        let hash = Hashtbl.hash
      end)
      (struct
        type t = Bytes.t

        let weight _ = 1
      end)

  type t = { mutable pages : Lru.t; length : int; lru_size : int; io : IO.t }

  let v ~length ~lru_size io =
    let pages = Lru.create lru_size in
    { pages; length; io; lru_size }

  let rec read t ~off ~len =
    let l = Int64.of_int t.length in
    let page_off = Int64.(mul (div off l) l) in
    let ioff = Int64.to_int (off -- page_off) in
    match Lru.find page_off t.pages with
    | Some buf ->
        if t.length - ioff < len then (
          Lru.remove page_off t.pages;
          read t ~off ~len )
        else (
          Lru.promote page_off t.pages;
          Lru.trim t.pages;
          Lwt.return (buf, ioff) )
    | None ->
        let length = max t.length (ioff + len) in
        let length =
          if page_off ++ Int64.of_int length > IO.offset t.io then
            Int64.to_int (IO.offset t.io -- page_off)
          else length
        in
        let buf = Bytes.create length in
        IO.read t.io ~off:page_off buf >|= fun () ->
        Lru.add page_off buf t.pages;
        Lru.trim t.pages;
        (buf, ioff)

  let clear t = t.pages <- Lru.create t.lru_size
end

module Dict = struct
  type t = {
    cache : (string, int) Hashtbl.t;
    index : (int, string) Hashtbl.t;
    block : IO.t;
    lock : Lwt_mutex.t
  }

  let read_length32 ~off block =
    let page = Bytes.create 4 in
    IO.read block ~off page >|= fun () ->
    let n, v = Irmin.Type.(decode_bin int32) (Bytes.unsafe_to_string page) 0 in
    assert (n = 4);
    Int32.to_int v

  let append_string t v =
    let len = Int32.of_int (String.length v) in
    let buf = Irmin.Type.(to_bin_string int32 len) ^ v in
    IO.append t.block buf

  let unsafe_index t v =
    Log.debug (fun l -> l "[dict] index %S" v);
    try Lwt.return (Hashtbl.find t.cache v)
    with Not_found ->
      let id = Hashtbl.length t.cache in
      append_string t v >|= fun () ->
      Hashtbl.add t.cache v id;
      Hashtbl.add t.index id v;
      id

  let index t v = Lwt_mutex.with_lock t.lock (fun () -> unsafe_index t v)

  let find t id =
    Log.debug (fun l -> l "[dict] find %d" id);
    let v = try Some (Hashtbl.find t.index id) with Not_found -> None in
    Lwt.return v

  let clear t =
    IO.clear t.block >|= fun () ->
    Hashtbl.clear t.cache;
    Hashtbl.clear t.index

  let files = Hashtbl.create 10

  let create = Lwt_mutex.create ()

  let unsafe_v ?(fresh = false) root =
    let root = root // "store.dict" in
    Log.debug (fun l -> l "[dict] v fresh=%b root=%s" fresh root);
    try
      let t = Hashtbl.find files root in
      (if fresh then clear t else Lwt.return ()) >|= fun () -> t
    with Not_found ->
      IO.v root >>= fun block ->
      (if fresh then IO.clear block else Lwt.return ()) >>= fun () ->
      let cache = Hashtbl.create 997 in
      let index = Hashtbl.create 997 in
      let len = IO.offset block in
      let rec aux n offset =
        if offset >= len then Lwt.return ()
        else
          read_length32 ~off:offset block >>= fun len ->
          let v = Bytes.create len in
          let off = offset ++ 4L in
          IO.read block ~off v >>= fun () ->
          let v = Bytes.unsafe_to_string v in
          Hashtbl.add cache v n;
          Hashtbl.add index n v;
          let off = off ++ Int64.of_int (String.length v) in
          aux (n + 1) off
      in
      aux 0 0L >|= fun () ->
      let t = { index; cache; block; lock = Lwt_mutex.create () } in
      Hashtbl.add files root t;
      t

  let v ?fresh root =
    Lwt_mutex.with_lock create (fun () -> unsafe_v ?fresh root)
end

module Index (H : Irmin.Hash.S) = struct
  type entry = { hash : H.t; offset : int64; len : int }

  let offset_size = 64 / 8

  let length_size = 32 / 8

  let pp_hash = Irmin.Type.pp H.t

  let pad = H.digest_size + offset_size + length_size

  (* last allowed offset *)
  let log_size = 300 * pad

  let log_sizeL = Int64.of_int log_size

  let entry = Irmin.Type.(triple H.t int64 int32)

  let decode_entry buf off =
    let _, (hash, offset, len) =
      Irmin.Type.decode_bin entry (Bytes.unsafe_to_string buf) off
    in
    { hash; offset; len = Int32.to_int len }

  let encode_entry { hash; offset; len } =
    let len = Int32.of_int len in
    Irmin.Type.to_bin_string entry (hash, offset, len)

  module Tbl = Hashtbl.Make (struct
    include H

    let equal x y = Irmin.Type.equal H.t x y
  end)

  module BF = Bloomf.Make (H)

  let lru_size = 1000

  let page_size = 1000 * pad

  type t = {
    cache : entry Tbl.t;
    pages : Pool.t;
    offsets : (int64, entry) Hashtbl.t;
    log : IO.t;
    index : IO.t;
    entries : BF.t;
    root : string;
    lock : Lwt_mutex.t
  }

  let unsafe_clear t =
    IO.clear t.log >>= fun () ->
    IO.clear t.index >|= fun () ->
    BF.clear t.entries;
    Pool.clear t.pages;
    Tbl.clear t.cache;
    Hashtbl.clear t.offsets

  let clear t = Lwt_mutex.with_lock t.lock (fun () -> unsafe_clear t)

  let files = Hashtbl.create 10

  let create = Lwt_mutex.create ()

  let log_path root = root // "store.log"

  let index_path root = root // "store.index"

  let unsafe_v ?(fresh = false) root =
    let log_path = log_path root in
    let index_path = index_path root in
    Log.debug (fun l ->
        l "[index] v fresh=%b log=%s index=%s" fresh log_path index_path );
    try
      let t = Hashtbl.find files root in
      (if fresh then clear t else Lwt.return ()) >|= fun () -> t
    with Not_found ->
      IO.v log_path >>= fun log ->
      IO.v index_path >>= fun index ->
      ( if fresh then IO.clear log >>= fun () -> IO.clear index
      else Lwt.return () )
      >|= fun () ->
      let t =
        { cache = Tbl.create 997;
          root;
          offsets = Hashtbl.create 997;
          pages = Pool.v ~length:page_size ~lru_size index;
          log;
          index;
          lock = Lwt_mutex.create ();
          entries = BF.create ~error_rate:0.1 500_000
        }
      in
      Hashtbl.add files root t;
      t

  let v ?fresh root =
    Lwt_mutex.with_lock create (fun () -> unsafe_v ?fresh root)

  let get_entry t off =
    Pool.read t.pages ~off ~len:pad >|= fun (page, ioff) ->
    decode_entry page ioff

  let padf = float_of_int pad

  let interpolation_search t key =
    let hashed_key = H.hash key in
    Log.debug (fun l -> l "interpolation_search %a (%d)" pp_hash key hashed_key);
    let hashed_key = float_of_int hashed_key in
    let low = 0. in
    let high = Int64.to_float (IO.offset t.index) -. padf in
    let rec search low high =
      get_entry t (Int64.of_float low) >>= fun lowest_entry ->
      let lowest_hash = float_of_int (H.hash lowest_entry.hash) in
      if high = low then
        if Irmin.Type.equal H.t lowest_entry.hash key then
          Lwt.return_some lowest_entry
        else Lwt.return_none
      else
        get_entry t (Int64.of_float high) >>= fun highest_entry ->
        let highest_hash = float_of_int (H.hash highest_entry.hash) in
        if high < low || lowest_hash > hashed_key || highest_hash < hashed_key
        then Lwt.return_none
        else
          let doff =
            floor
              ( (high -. low)
              *. (hashed_key -. lowest_hash)
              /. (highest_hash -. lowest_hash) )
          in
          let off = low +. doff -. mod_float doff padf in
          let offL = Int64.of_float off in
          get_entry t offL >>= fun e ->
          if Irmin.Type.equal H.t e.hash key then Lwt.return (Some e)
          else if float_of_int (H.hash e.hash) < hashed_key then
            search (off +. padf) high
          else search low (max 0. (off -. padf))
    in
    if high < 0. then Lwt.return_none else search low high

  (*  let dump_entry ppf e = Fmt.pf ppf "[offset:%Ld len:%d]" e.offset e.len *)

  let unsafe_find t key =
    Log.debug (fun l -> l "[index] find %a" pp_hash key);
    if not (BF.mem t.entries key) then Lwt.return None
    else
      match Tbl.find t.cache key with
      | e -> Lwt.return (Some e)
      | exception Not_found -> interpolation_search t key

  let find t key = Lwt_mutex.with_lock t.lock (fun () -> unsafe_find t key)

  let unsafe_mem t key =
    if not (BF.mem t.entries key) then Lwt.return false
    else unsafe_find t key >|= function None -> false | Some _ -> true

  let mem t key = Lwt_mutex.with_lock t.lock (fun () -> unsafe_mem t key)

  let append_entry t e = IO.append t (encode_entry e)

  module HashMap = Map.Make (struct
    type t = H.t

    let compare a b = compare (H.hash a) (H.hash b)
  end)

  let merge_with t tmp =
    let log_list =
      Tbl.fold (fun h k acc -> HashMap.add h k acc) t.cache HashMap.empty
      |> HashMap.bindings
    in
    let offset = ref 0L in
    let get_index_entry = function
      | Some e -> Lwt.return (Some e)
      | None ->
          if !offset >= IO.offset t.index then Lwt.return None
          else
            get_entry t !offset >|= fun e ->
            offset := !offset ++ Int64.of_int pad;
            Some e
    in
    let rec go last_read l =
      get_index_entry last_read >>= function
      | None -> Lwt_list.iter_s (fun (_, e) -> append_entry tmp e) l
      | Some e -> (
        match l with
        | (k, v) :: r ->
            ( if Irmin.Type.equal H.t e.hash k then
              append_entry tmp e >|= fun () -> (None, r)
            else
              let hashed_e = H.hash e.hash in
              let hashed_k = H.hash k in
              if hashed_e = hashed_k then
                append_entry tmp e >>= fun () ->
                append_entry tmp v >|= fun () -> (None, r)
              else if hashed_e < hashed_k then
                append_entry tmp e >|= fun () -> (None, l)
              else append_entry tmp v >|= fun () -> (Some e, r) )
            >>= fun (last, rst) ->
            if !offset >= IO.offset t.index && last = None then
              Lwt_list.iter_s (fun (_, e) -> append_entry tmp e) rst
            else go last rst
        | [] ->
            append_entry tmp e >>= fun () ->
            if !offset >= IO.offset t.index then Lwt.return_unit
            else
              let len = IO.offset t.index -- !offset in
              let buf = Bytes.create (Int64.to_int len) in
              IO.read t.index ~off:!offset buf >>= fun () ->
              IO.append tmp (Bytes.unsafe_to_string buf) )
    in
    go None log_list

  let merge t =
    IO.sync t.log >>= fun () ->
    let tmp_path = t.root // "store.index.tmp" in
    IO.v tmp_path >>= fun tmp ->
    merge_with t tmp >>= fun () ->
    IO.rename ~src:tmp ~dst:t.index >>= fun () ->
    (* reset the log *)
    IO.clear t.log >|= fun () ->
    Tbl.clear t.cache;
    Pool.clear t.pages;
    Hashtbl.clear t.offsets

  (* do not check for duplicates *)
  let unsafe_append t key ~off ~len =
    Log.debug (fun l ->
        l "[index] append %a off=%Ld len=%d" pp_hash key off len );
    let entry = { hash = key; offset = off; len } in
    append_entry t.log entry >>= fun () ->
    Tbl.add t.cache key entry;
    Hashtbl.add t.offsets entry.offset entry;
    BF.add t.entries key;
    if Int64.compare (IO.offset t.log) log_sizeL > 0 then merge t
    else Lwt.return_unit

  let append t key ~off ~len =
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_append t key ~off ~len)
end

module type S = sig
  include Irmin.Type.S

  type hash

  val to_bin :
    dict:(string -> int Lwt.t) ->
    offset:(hash -> int64 Lwt.t) ->
    t ->
    hash ->
    string Lwt.t

  val decode_bin :
    dict:(int -> string option Lwt.t) ->
    hash:(int64 -> hash Lwt.t) ->
    string ->
    int ->
    t Lwt.t
end

module Pack (K : Irmin.Hash.S) = struct
  module Index = Index (K)

  module Tbl = Hashtbl.Make (struct
    include K

    let equal x y = Irmin.Type.equal K.t x y
  end)

  type 'a t = {
    block : IO.t;
    index : Index.t;
    dict : Dict.t;
    lock : Lwt_mutex.t
  }

  let unsafe_clear t =
    IO.clear t.block >>= fun () ->
    Index.clear t.index >>= fun () -> Dict.clear t.dict

  let clear t = Lwt_mutex.with_lock t.lock (fun () -> unsafe_clear t)

  let files = Hashtbl.create 10

  let create = Lwt_mutex.create ()

  let unsafe_v ?(fresh = false) root =
    Log.debug (fun l -> l "[state] v fresh=%b root=%s" fresh root);
    let root_f = root // "store.pack" in
    try
      let t = Hashtbl.find files root_f in
      (if fresh then clear t else Lwt.return ()) >|= fun () -> t
    with Not_found ->
      let lock = Lwt_mutex.create () in
      Index.v ~fresh root >>= fun index ->
      Dict.v ~fresh root >>= fun dict ->
      IO.v root_f >>= fun block ->
      (if fresh then IO.clear block else Lwt.return ()) >|= fun () ->
      let t = { block; index; lock; dict } in
      Hashtbl.add files root_f t;
      t

  let v ?fresh root =
    Lwt_mutex.with_lock create (fun () -> unsafe_v ?fresh root)

  module Make (V : S with type hash = K.t) = struct
    module Tbl = Hashtbl.Make (struct
      type t = K.t

      let equal = Irmin.Type.equal K.t

      let hash = Irmin.Type.hash K.t
    end)

    let lru_size = 10_000

    let page_size = 1024

    type nonrec 'a t = { pack : 'a t; cache : V.t Tbl.t; pages : Pool.t }

    type key = K.t

    type value = V.t

    let clear t = clear t.pack >|= fun () -> Tbl.clear t.cache

    let files = Hashtbl.create 10

    let create = Lwt_mutex.create ()

    let unsafe_v ?(fresh = false) root =
      Log.debug (fun l -> l "[pack] v fresh=%b root=%s" fresh root);
      try
        let t = Hashtbl.find files root in
        (if fresh then clear t else Lwt.return ()) >|= fun () -> t
      with Not_found ->
        v ~fresh root >>= fun pack ->
        let cache = Tbl.create (1024 * 1024) in
        let t =
          { cache;
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

    let mem t k =
      Log.debug (fun l -> l "[pack] mem %a" pp_hash k);
      Index.mem t.pack.index k

    let digest v = K.digest (Irmin.Type.pre_digest V.t v)

    let check_key k v =
      let k' = digest v in
      if Irmin.Type.equal K.t k k' then Lwt.return ()
      else
        Fmt.kstrf Lwt.fail_invalid_arg "corrupted value: got %a, expecting %a."
          pp_hash k' pp_hash k

    let unsafe_find t k =
      Log.debug (fun l -> l "[pack] find %a" pp_hash k);
      match Tbl.find t.cache k with
      | v -> Lwt.return (Some v)
      | exception Not_found -> (
          Index.find t.pack.index k >>= function
          | None -> Lwt.return None
          | Some e ->
              Pool.read t.pages ~off:e.offset ~len:e.len >>= fun (buf, pos) ->
              let hash off =
                match Hashtbl.find t.pack.index.offsets off with
                | e -> Lwt.return e.hash
                | exception Not_found ->
                    Pool.read t.pages ~off ~len:K.digest_size
                    >|= fun (buf, pos) ->
                    let _, v =
                      Irmin.Type.decode_bin ~headers:false K.t
                        (Bytes.unsafe_to_string buf)
                        pos
                    in
                    v
              in
              let dict = Dict.find t.pack.dict in
              V.decode_bin ~hash ~dict (Bytes.unsafe_to_string buf) pos
              >>= fun v ->
              check_key k v >|= fun () ->
              Tbl.add t.cache k v;
              Some v )

    let find t k = Lwt_mutex.with_lock t.pack.lock (fun () -> unsafe_find t k)

    let cast t = (t :> [ `Read | `Write ] t)

    let batch t f =
      f (cast t) >>= fun r ->
      if Tbl.length t.cache = 0 then Lwt.return r
      else
        IO.sync t.pack.dict.block >>= fun () ->
        IO.sync t.pack.index.log >>= fun () ->
        IO.sync t.pack.block >|= fun () ->
        Tbl.clear t.cache;
        (* would probably be better to just clear the last page *)
        Pool.clear t.pages;
        r

    let unsafe_append t k v =
      Index.mem t.pack.index k >>= function
      | true -> Lwt.return ()
      | false ->
          Log.debug (fun l -> l "[pack] append %a" pp_hash k);
          let offset k =
            Index.find t.pack.index k >|= function
            | Some e -> e.offset
            | None -> Fmt.failwith "cannot find %a" pp_hash k
          in
          let dict = Dict.index t.pack.dict in
          V.to_bin ~offset ~dict v k >>= fun buf ->
          let off = IO.offset t.pack.block in
          IO.append t.pack.block buf >>= fun () ->
          Index.append t.pack.index k ~off ~len:(String.length buf)
          >|= fun () -> Tbl.add t.cache k v

    let append t k v =
      Lwt_mutex.with_lock t.pack.lock (fun () -> unsafe_append t k v)

    let add t v =
      let k = digest v in
      append t k v >|= fun () -> k
  end
end

module Atomic_write (K : Irmin.Type.S) (V : Irmin.Hash.S) = struct
  module Tbl = Hashtbl.Make (struct
    type t = K.t

    let hash = Irmin.Type.hash K.t

    let equal = Irmin.Type.equal K.t
  end)

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

  let read_length32 ~off block =
    let page = Bytes.create 4 in
    IO.read block ~off page >|= fun () ->
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
    match Irmin.Type.of_bin_string V.t (String.make V.digest_size '\000') with
    | Ok x -> x
    | Error _ -> assert false

  let unsafe_remove t k =
    Tbl.remove t.cache k;
    try
      let off = Tbl.find t.index k in
      set_entry t ~off k zero
    with Not_found -> Lwt.return ()

  let remove t k =
    Log.debug (fun l -> l "[branches] remove %a" pp_branch k);
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_remove t k) >>= fun () ->
    W.notify t.w k None

  let clear t =
    W.clear t.w >>= fun () ->
    IO.clear t.block >|= fun () ->
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
      IO.v root >>= fun block ->
      (if fresh then IO.clear block else Lwt.return ()) >>= fun () ->
      let cache = Tbl.create 997 in
      let index = Tbl.create 997 in
      let len = IO.offset block in
      let rec aux offset =
        if offset >= len then Lwt.return ()
        else
          read_length32 ~off:offset block >>= fun len ->
          let buf = Bytes.create (len + V.digest_size) in
          let off = offset ++ 4L in
          IO.read block ~off buf >>= fun () ->
          let buf = Bytes.unsafe_to_string buf in
          let k = String.sub buf 0 len in
          let k =
            match Irmin.Type.of_bin_string K.t k with
            | Ok k -> k
            | Error (`Msg e) -> failwith e
          in
          let n, v = Irmin.Type.decode_bin V.t buf len in
          assert (n = len + V.digest_size);
          if not (Irmin.Type.equal V.t v zero) then Tbl.add cache k v;
          Tbl.add index k offset;
          aux (off ++ Int64.(of_int @@ (len + V.digest_size)))
      in
      aux 0L >|= fun () ->
      let t =
        { cache; index; block; w = watches; lock = Lwt_mutex.create () }
      in
      Hashtbl.add files root t;
      t

  let v ?fresh root =
    Lwt_mutex.with_lock create (fun () -> unsafe_v ?fresh root)

  let unsafe_set t k v =
    try
      let off = Tbl.find t.index k in
      Tbl.replace t.cache k v;
      set_entry t ~off k v
    with Not_found ->
      let offset = IO.offset t.block in
      set_entry t k v >|= fun () ->
      Tbl.add t.cache k v;
      Tbl.add t.index k offset

  let set t k v =
    Log.debug (fun l -> l "[branches] set %a" pp_branch k);
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_set t k v) >>= fun () ->
    W.notify t.w k (Some v)

  let unsafe_test_and_set t k ~test ~set =
    let v = try Some (Tbl.find t.cache k) with Not_found -> None in
    if not (Irmin.Type.(equal (option V.t)) v test) then Lwt.return false
    else
      let return () = true in
      match set with
      | None -> unsafe_remove t k >|= return
      | Some v -> unsafe_set t k v >|= return

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

(* (too??) similar to Irmin.Make_ext *)
module Make_ext
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

          type hash = H.t

          let with_hash_t = Irmin.Type.(pair H.t Val.t)

          let to_bin ~dict:_ ~offset:_ t k =
            let s = Irmin.Type.to_bin_string with_hash_t (k, t) in
            Lwt.return s

          let decode_bin ~dict:_ ~hash:_ s off =
            let _, (_, t) =
              Irmin.Type.decode_bin ~headers:false with_hash_t s off
            in
            Lwt.return t
        end)
      end

      include Irmin.Contents.Store (CA)
    end

    module Node = struct
      module CA = struct
        module Key = H
        module Val = Node

        module Int = struct
          type t = int64

          type step = int64

          let step_t = Irmin.Type.int64

          let t = Irmin.Type.int64
        end

        module Val_int = Irmin.Private.Node.Make (Int) (Int) (M)

        include Pack.Make (struct
          include Val

          let entries_t =
            Irmin.Type.(pair H.t (list (pair int Val_int.value_t)))

          let to_bin ~dict ~offset t k =
            let entries = Val.list t in
            let step s = dict (Irmin.Type.to_bin_string P.step_t s) in
            let value = function
              | `Contents (v, m) -> offset v >|= fun off -> `Contents (off, m)
              | `Node v -> offset v >|= fun off -> `Node off
            in
            Lwt_list.map_p
              (fun (s, v) -> step s >>= fun s -> value v >|= fun v -> (s, v))
              entries
            >|= fun entries ->
            let res = Irmin.Type.to_bin_string entries_t (k, entries) in
            res

          exception Exit of [ `Msg of string ]

          let decode_bin ~dict ~hash t off =
            let _, (_, entries) =
              Irmin.Type.decode_bin ~headers:false entries_t t off
            in
            let step s =
              dict s >|= function
              | None -> raise_notrace (Exit (`Msg "dict"))
              | Some s -> (
                match Irmin.Type.of_bin_string P.step_t s with
                | Error e -> raise_notrace (Exit e)
                | Ok v -> v )
            in
            let value = function
              | `Contents (off, m) -> hash off >|= fun v -> `Contents (v, m)
              | `Node off -> hash off >|= fun v -> `Node v
            in
            Lwt.catch
              (fun () ->
                Lwt_list.map_p
                  (fun (s, v) ->
                    step s >>= fun s -> value v >|= fun v -> (s, v) )
                  entries
                >|= fun entries -> Val.v entries )
              (function Exit (`Msg e) -> Lwt.fail_with e | e -> Lwt.fail e)
        end)
      end

      include Irmin.Private.Node.Store (Contents) (P) (M) (CA)
    end

    module Commit = struct
      module CA = struct
        module Key = H
        module Val = Commit

        include Pack.Make (struct
          include Val

          let with_hash_t = Irmin.Type.(pair H.t Val.t)

          let to_bin ~dict:_ ~offset:_ t k =
            Lwt.return (Irmin.Type.to_bin_string with_hash_t (k, t))

          let decode_bin ~dict:_ ~hash:_ s off =
            let _, (_, v) =
              Irmin.Type.decode_bin ~headers:false with_hash_t s off
            in
            Lwt.return v
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
    (M : Irmin.Metadata.S)
    (C : Irmin.Contents.S)
    (P : Irmin.Path.S)
    (B : Irmin.Branch.S)
    (H : Irmin.Hash.S) =
struct
  module XNode = Irmin.Private.Node.Make (H) (P) (M)
  module XCommit = Irmin.Private.Commit.Make (H)
  include Make_ext (M) (C) (P) (B) (H) (XNode) (XCommit)
end

module KV (C : Irmin.Contents.S) =
  Make (Metadata) (C) (Path) (Irmin.Branch.String) (Hash)
