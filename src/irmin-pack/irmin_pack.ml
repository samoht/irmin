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

module type IO = sig
  type t

  val v : string -> t Lwt.t

  val clear : t -> unit Lwt.t

  val append : t -> string -> unit

  val set : t -> off:int64 -> string -> unit Lwt.t

  val read : t -> off:int64 -> bytes -> unit Lwt.t

  val offset : t -> int64

  val sync : t -> unit Lwt.t
end

module IO : IO = struct
  type fd = {
    file : string;
    fd : Lwt_unix.file_descr;
    mutable cursor : int64;
    lock : Lwt_mutex.t
  }

  type t = {
    mutable offset : int64;
    mutable flushed : int64;
    fd : fd;
    buf : Buffer.t
  }

  let header = 8L

  let ( ++ ) = Int64.add

  module Raw = struct
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

    let read t ~off buf =
      Lwt_mutex.with_lock t.lock (fun () -> unsafe_read t ~off buf)

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

  let unsafe_sync t =
    let buf = Buffer.contents t.buf in
    Buffer.clear t.buf;
    if buf = "" then Lwt.return ()
    else
      Raw.unsafe_write t.fd ~off:t.flushed buf >>= fun () ->
      Raw.unsafe_set_offset t.fd t.offset >|= fun () ->
      t.flushed <- t.flushed ++ Int64.of_int (String.length buf);
      assert (t.flushed <= t.offset ++ header)

  let sync t = Lwt_mutex.with_lock t.fd.lock (fun () -> unsafe_sync t)

  let append t buf =
    Buffer.add_string t.buf buf;
    let len = Int64.of_int (String.length buf) in
    t.offset <- t.offset ++ len

  let unsafe_set t ~off buf =
    unsafe_sync t >>= fun () ->
    Raw.unsafe_write t.fd ~off:(header ++ off) buf >|= fun () ->
    let len = Int64.of_int (String.length buf) in
    let off = header ++ off ++ len in
    assert (off <= t.flushed)

  let set t ~off buf =
    Lwt_mutex.with_lock t.fd.lock (fun () -> unsafe_set t ~off buf)

  let read t ~off buf = Raw.read t.fd ~off:(header ++ off) buf

  (* Lwt_unix.fsync t.write.fd *)

  let offset t = t.offset

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

  let fd file fd = { file; lock = Lwt_mutex.create (); fd; cursor = 0L }

  let v file =
    let v ~offset ~fd =
      let buf = Buffer.create (1024 * 1024) in
      { offset; fd; buf; flushed = header ++ offset }
    in
    mkdir (Filename.dirname file) >>= fun () ->
    Lwt_unix.file_exists file >>= function
    | false ->
        Lwt_unix.openfile file Unix.[ O_CREAT; O_RDWR ] 0o644 >>= fun x ->
        let fd = fd file x in
        Raw.unsafe_set_offset fd 0L >|= fun () -> v ~offset:0L ~fd
    | true ->
        Lwt_unix.openfile file Unix.[ O_EXCL; O_RDWR ] 0o644 >>= fun x ->
        let fd = fd file x in
        Raw.unsafe_get_offset fd >|= fun offset -> v ~offset ~fd
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
      append_string t v;
      Hashtbl.add t.cache v id;
      Hashtbl.add t.index id v;
      Lwt.return id

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
  let invalid_bounds off len =
    Fmt.invalid_arg "Invalid bounds (off: %d, len: %d)" off len

  type entry = { id : int; hash : H.t; offset : int64; len : int }

  let offset_size = 64 / 8

  let length_size = 32 / 8

  let pp_hash = Irmin.Type.pp H.t

  let pad = H.digest_size + offset_size + length_size

  let padL = Int64.of_int pad

  module Decoder = struct
    type decode = [ `Await | `End | `Entry of entry | `Malformed of string ]

    type src = [ `Manual ]

    let unexpected_end_of_input = `Malformed "Unexpected end of input"

    type decoder = {
      src : src;
      id : int;
      mutable i_off : int;
      mutable i_len : int;
      mutable i_pos : int;
      mutable i : Bytes.t;
      mutable h_len : int;
      mutable h_need : int;
      h : Bytes.t;
      mutable hash : H.t;
      mutable k : decoder -> decode
    }

    let end_of_input decoder =
      decoder.i <- Bytes.empty;
      decoder.i_off <- 0;
      decoder.i_pos <- 0;
      decoder.i_len <- min_int

    let unsafe_blit src src_off dst dst_off len =
      Bytes.unsafe_blit src src_off dst dst_off len

    let src decoder buffer off len =
      if off < 0 || len < 0 || off + len > Bytes.length buffer then
        invalid_bounds off len;
      if len = 0 then end_of_input decoder
      else (
        decoder.i <- buffer;
        decoder.i_off <- off;
        decoder.i_pos <- 0;
        decoder.i_len <- len - 1 )

    let refill k decoder =
      match decoder.src with
      | `Manual ->
          decoder.k <- k;
          `Await

    let ret k value decoder =
      decoder.k <- k;
      (value :> decode)

    (* XXX(dinosaure): post-processing is only (:>). *)

    let i_rem decoder = decoder.i_len - decoder.i_pos + 1

    let t_need decoder need =
      decoder.h_len <- 0;
      decoder.h_need <- need

    let rec t_fill k decoder =
      let blit decoder len =
        unsafe_blit decoder.i
          (decoder.i_off + decoder.i_pos)
          decoder.h decoder.h_len len;
        decoder.i_pos <- decoder.i_pos + len;
        decoder.h_len <- decoder.h_len + len
      in
      let rem = i_rem decoder in
      if rem < 0 then k decoder
      else
        let need = decoder.h_need - decoder.h_len in
        if rem < need then (
          blit decoder rem;
          refill (t_fill k) decoder )
        else (
          blit decoder need;
          k decoder )

    let r_hash buffer off len =
      let buffer = Bytes.to_string buffer in
      assert (len = H.digest_size);
      let n, v = Irmin.Type.(decode_bin H.t) buffer off in
      assert (n - off = len);
      v

    let get_int64 ~off buf =
      let n, v = Irmin.Type.(decode_bin int64) buf off in
      assert (n - off = 8);
      v

    let get_int32 ~off buf =
      let n, v = Irmin.Type.(decode_bin int32) buf off in
      assert (n - off = 4);
      Int32.to_int v

    let r_entry hash buffer off len =
      assert (len = 12);
      let buffer = Bytes.to_string buffer in
      let offset = get_int64 ~off buffer in
      let len = get_int32 ~off:(off + 8) buffer in
      `Entry { id = 0; hash; offset; len }

    let rec t_decode_hash decoder =
      if decoder.h_len < decoder.h_need then
        ret decode_entry unexpected_end_of_input decoder
      else
        let hash = r_hash decoder.h 0 decoder.h_len in
        decode_offset hash decoder

    and t_decode_offset decoder =
      if decoder.h_len < decoder.h_need then
        ret decode_entry unexpected_end_of_input decoder
      else
        let e = r_entry decoder.hash decoder.h 0 decoder.h_len in
        ret decode_entry e decoder

    and decode_offset hash decoder =
      let rem = i_rem decoder in
      if rem <= 0 then if rem < 0 then `End else refill decode_entry decoder
      else (
        t_need decoder (offset_size + length_size);
        decoder.hash <- hash;
        t_fill t_decode_offset decoder )

    and decode_entry decoder =
      let rem = i_rem decoder in
      if rem <= 0 then if rem < 0 then `End else refill decode_entry decoder
      else (
        t_need decoder H.digest_size;
        t_fill t_decode_hash decoder )

    let decode decoder = decoder.k decoder

    let decoder src id =
      let k = decode_entry in
      let i, i_off, i_pos, i_len =
        match src with `Manual -> (Bytes.empty, 0, 1, 0)
      in
      { src;
        id;
        i_off;
        i_pos;
        i_len;
        i;
        hash = H.digest "";
        h = Bytes.create (H.digest_size + 12);
        h_need = 0;
        h_len = 0;
        k
      }
  end

  module Encoder = struct end

  module Tbl = Hashtbl.Make (struct
    include H

    let equal x y = Irmin.Type.equal H.t x y
  end)

  type t = {
    mutable decoder : Decoder.decoder;
    cache : entry Tbl.t;
    entries : (int, entry) Hashtbl.t;
    block : IO.t;
    lock : Lwt_mutex.t;
    mutable i_off : int64;
    mutable pos : int64;
    mutable max : int64;
    root : string;
    i : Bytes.t;
    o : Bytes.t
  }

  let unsafe_clear t =
    IO.clear t.block >|= fun () ->
    Tbl.clear t.cache;
    Hashtbl.clear t.entries;
    t.decoder <- Decoder.decoder `Manual 0;
    t.i_off <- 0L;
    t.pos <- 0L;
    t.max <- 0L

  let clear t = Lwt_mutex.with_lock t.lock (fun () -> unsafe_clear t)

  let files = Hashtbl.create 10

  let create = Lwt_mutex.create ()

  let unsafe_v ?(fresh = false) root =
    let root = root // "store.index" in
    Log.debug (fun l -> l "[index] v fresh=%b root=%s" fresh root);
    try
      let t = Hashtbl.find files root in
      (if fresh then clear t else Lwt.return ()) >|= fun () -> t
    with Not_found ->
      IO.v root >>= fun block ->
      (if fresh then IO.clear block else Lwt.return ()) >|= fun () ->
      let o = Bytes.create 4096 in
      let t =
        { decoder = Decoder.decoder `Manual 0;
          cache = Tbl.create 997;
          entries = Hashtbl.create 997;
          root;
          lock = Lwt_mutex.create ();
          block;
          i_off = 0L;
          pos = 0L;
          max = IO.offset block;
          i = Bytes.create 4096;
          o
        }
      in
      Hashtbl.add files root t;
      t

  (* XXX(dinosaure): pos & max is like a queue... *)

  let v ?fresh root =
    Lwt_mutex.with_lock create (fun () -> unsafe_v ?fresh root)

  let get_id t = Int64.to_int (Int64.div (IO.offset t.block) padL)

  let unsafe_find t key =
    Log.debug (fun l -> l "[index] find %a" pp_hash key);
    match Tbl.find t.cache key with
    | e -> Lwt.return (Some e)
    | exception Not_found ->
        let rec go () =
          match Decoder.decode t.decoder with
          | `Await ->
              IO.read t.block ~off:t.i_off t.i >>= fun () ->
              t.i_off <- t.i_off ++ 4096L;
              Decoder.src t.decoder t.i 0 4096;
              go ()
          | `Entry e ->
              let id = get_id t in
              let e = { e with id } in
              Tbl.add t.cache e.hash e;
              Hashtbl.add t.entries e.id e;
              t.pos <- t.pos ++ padL;
              if Irmin.Type.equal H.t e.hash key then Lwt.return (Some e)
              else if t.pos >= t.max then Lwt.return None
              else go ()
          | `Malformed _ -> assert false
          | `End -> assert false
        in
        if t.pos < t.max then go () else Lwt.return None

  let find t key = Lwt_mutex.with_lock t.lock (fun () -> unsafe_find t key)

  let mem t key =
    if Tbl.mem t.cache key then Lwt.return true
    else find t key >|= function None -> false | Some _ -> true

  let unsafe_read t id =
    Log.debug (fun l -> l "[index] read %d" id);
    try Lwt.return (Some (Hashtbl.find t.entries id))
    with Not_found ->
      if get_id t < id then Lwt.return None
      else
        let buf = Bytes.create pad in
        let off = Int64.(mul (of_int id) padL) in
        IO.read t.block ~off buf >|= fun () ->
        let h =
          Decoder.r_hash (Bytes.sub buf 0 H.digest_size) 0 H.digest_size
        in
        let (`Entry e) = Decoder.r_entry h buf H.digest_size 12 in
        let e = { e with id } in
        Hashtbl.add t.entries id e;
        Tbl.add t.cache e.hash e;
        Some e

  let read t id = Lwt_mutex.with_lock t.lock (fun () -> unsafe_read t id)

  (* do not check for duplicates *)
  let unsafe_append t key ~off ~len =
    Log.debug (fun l ->
        l "[index] append %a off=%Ld len=%d" pp_hash key off len );
    let entry = { hash = key; offset = off; len; id = get_id t } in
    let buf = Buffer.create pad in
    let open Irmin.Type in
    encode_bin H.t buf key;
    encode_bin int64 buf off;
    encode_bin int32 buf (Int32.of_int len);
    IO.append t.block (Buffer.contents buf);
    Tbl.add t.cache key entry;
    Hashtbl.add t.entries entry.id entry;
    Lwt.return ()

  let append t key ~off ~len =
    Lwt_mutex.with_lock t.lock (fun () -> unsafe_append t key ~off ~len)
end

module type S = sig
  include Irmin.Type.S

  type hash

  val to_bin :
    dict:(string -> int Lwt.t) ->
    index:(hash -> int Lwt.t) ->
    t ->
    string Lwt.t

  val of_bin :
    dict:(int -> string option Lwt.t) ->
    index:(int -> hash option Lwt.t) ->
    string ->
    (t, [ `Msg of string ]) result Lwt.t
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

    type nonrec 'a t = { pack : 'a t; cache : V.t Tbl.t }

    type key = K.t

    type value = V.t

    let clear t = clear t.pack >|= fun () -> Tbl.clear t.cache

    let files = Hashtbl.create 10

    let create = Lwt_mutex.create ()

    let unsafe_v ?(fresh = false) root =
      try
        let t = Hashtbl.find files root in
        (if fresh then clear t else Lwt.return ()) >|= fun () -> t
      with Not_found ->
        v ~fresh root >>= fun pack ->
        let cache = Tbl.create (1024 * 1024) in
        let t = { cache; pack } in
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
          | Some e -> (
              let buf = Bytes.create e.len in
              IO.read t.pack.block ~off:e.offset buf >>= fun () ->
              let index id =
                Index.read t.pack.index id >|= function
                | Some e -> Some e.hash
                | None -> None
              in
              let dict = Dict.find t.pack.dict in
              V.of_bin ~index ~dict (Bytes.unsafe_to_string buf) >>= function
              | Error (`Msg e) -> Lwt.fail_with e
              | Ok v ->
                  check_key k v >|= fun () ->
                  Tbl.add t.cache k v;
                  Some v ) )

    let find t k = Lwt_mutex.with_lock t.pack.lock (fun () -> unsafe_find t k)

    let cast t = (t :> [ `Read | `Write ] t)

    let batch t f =
      f (cast t) >>= fun r ->
      if Tbl.length t.cache = 0 then Lwt.return r
      else
        IO.sync t.pack.dict.block >>= fun () ->
        IO.sync t.pack.index.block >>= fun () ->
        IO.sync t.pack.block >|= fun () ->
        Tbl.clear t.cache;
        r

    let unsafe_append t k v =
      Index.mem t.pack.index k >>= function
      | true -> Lwt.return ()
      | false ->
          Log.debug (fun l -> l "[pack] append %a" pp_hash k);
          let index k =
            Index.find t.pack.index k >|= function
            | Some e -> e.id
            | None -> assert false
          in
          let dict = Dict.index t.pack.dict in
          V.to_bin ~index ~dict v >>= fun buf ->
          let off = IO.offset t.pack.block in
          IO.append t.pack.block buf;
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
    | None ->
        IO.append t.block buf;
        Lwt.return ()
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
    let root = root // "store.dict" in
    Log.debug (fun l -> l "[dict] v fresh=%b root=%s" fresh root);
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

          let to_bin ~dict:_ ~index:_ t =
            Lwt.return (Irmin.Type.to_bin_string Val.t t)

          let of_bin ~dict:_ ~index:_ s =
            Lwt.return (Irmin.Type.of_bin_string Val.t s)
        end)
      end

      include Irmin.Contents.Store (CA)
    end

    module Node = struct
      module CA = struct
        module Key = H
        module Val = Node

        module Int = struct
          type t = int

          type step = int

          let step_t = Irmin.Type.int

          let t = Irmin.Type.int
        end

        module Val_int = Irmin.Private.Node.Make (Int) (Int) (M)

        include Pack.Make (struct
          include Val

          let entries_t = Irmin.Type.(list (pair int Val_int.value_t))

          let to_bin ~dict ~index t =
            let entries = Val.list t in
            let step s = dict (Irmin.Type.to_bin_string P.step_t s) in
            let value : Val.value -> Val_int.value Lwt.t = function
              | `Contents (v, m) -> index v >|= fun v -> `Contents (v, m)
              | `Node v -> index v >|= fun v -> `Node v
            in
            Lwt_list.map_p
              (fun (s, v) -> step s >>= fun s -> value v >|= fun v -> (s, v))
              entries
            >|= fun entries -> Irmin.Type.to_bin_string entries_t entries

          exception Exit of [ `Msg of string ]

          let of_bin ~dict ~index (t : string) =
            match Irmin.Type.of_bin_string entries_t t with
            | Error _ as e -> Lwt.return e
            | Ok entries ->
                let step s =
                  dict s >|= function
                  | None -> raise_notrace (Exit (`Msg "dict"))
                  | Some s -> (
                    match Irmin.Type.of_bin_string P.step_t s with
                    | Error e -> raise_notrace (Exit e)
                    | Ok v -> v )
                in
                let value : Val_int.value -> Val.value Lwt.t = function
                  | `Contents (v, m) -> (
                      index v >|= function
                      | Some v -> `Contents (v, m)
                      | None -> raise_notrace (Exit (`Msg "no contents")) )
                  | `Node v -> (
                      index v >|= function
                      | Some v -> `Node v
                      | None -> raise_notrace (Exit (`Msg "no node")) )
                in
                Lwt.catch
                  (fun () ->
                    Lwt_list.map_p
                      (fun (s, v) ->
                        step s >>= fun s -> value v >|= fun v -> (s, v) )
                      entries
                    >|= fun entries -> Ok (Val.v entries) )
                  (function Exit e -> Lwt.return (Error e) | e -> Lwt.fail e)
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

          let to_bin ~dict:_ ~index:_ t =
            Lwt.return (Irmin.Type.to_bin_string Val.t t)

          let of_bin ~dict:_ ~index:_ s =
            Lwt.return (Irmin.Type.of_bin_string Val.t s)
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
