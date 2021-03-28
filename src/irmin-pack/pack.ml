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

include Pack_intf

let src = Logs.Src.create "irmin.pack" ~doc:"irmin-pack backend"

module Log = (val Logs.src_log src : Logs.LOG)

let ( -- ) = Int64.sub

open! Import

module HashedType (H : Irmin.Hash.S) = struct
  type t = H.t

  let hash t = H.short_hash t
  let equal = Irmin.Type.(unstage (equal H.t))
end

module File
    (Index : Pack_index.S)
    (H : Irmin.Hash.S with type t = Index.key)
    (Key : KEY with type hash = H.t)
    (IO_version : IO.VERSION) =
struct
  module IO_cache = IO.Cache
  module IO = IO.Unix
  module H' = HashedType (H)
  module Tbl = Hashtbl.Make (H')
  module Lru = Irmin.Private.Lru.Make (H')
  module Dict = Pack_dict.Make (IO_version)

  type index = Index.t

  let current_version = IO_version.io_version

  type 'a t = {
    mutable block : IO.t;
    index : Index.t;
    dict : Dict.t;
    mutable open_instances : int;
  }

  let clear ?keep_generation t =
    Index.clear t.index;
    match current_version with
    | `V1 -> IO.truncate t.block
    | `V2 ->
        IO.clear ?keep_generation t.block;
        Dict.clear t.dict

  let valid t =
    if t.open_instances <> 0 then (
      t.open_instances <- t.open_instances + 1;
      true)
    else false

  let unsafe_v ~index ~fresh ~readonly file =
    let root = Filename.dirname file in
    let dict = Dict.v ~fresh ~readonly root in
    let block = IO.v ~version:(Some current_version) ~fresh ~readonly file in
    { block; index; dict; open_instances = 1 }

  let IO_cache.{ v } =
    IO_cache.memoize ~clear ~valid ~v:(fun index -> unsafe_v ~index) Layout.pack

  type key = Key.t

  let close t =
    t.open_instances <- t.open_instances - 1;
    if t.open_instances = 0 then (
      if not (IO.readonly t.block) then IO.flush t.block;
      IO.close t.block;
      Dict.close t.dict)

  module Make (V : ELT with type hash := H.t) = struct
    type nonrec 'a t = {
      pack : 'a t;
      lru : V.t Lru.t;
      staging : V.t Tbl.t;
      mutable open_instances : int;
      readonly : bool;
    }

    type key = Key.t

    let equal_key x y = H'.equal (Key.hash x) (Key.hash y)
    let equal_hash = Irmin.Type.(unstage (equal H.t))

    type value = V.t
    type index = Index.t

    let unsafe_clear ?keep_generation t =
      clear ?keep_generation t.pack;
      Tbl.clear t.staging;
      Lru.clear t.lru

    (* we need another cache here, as we want to share the LRU and
       staging caches too. *)

    let roots = Hashtbl.create 10

    let valid t =
      if t.open_instances <> 0 then (
        t.open_instances <- t.open_instances + 1;
        true)
      else false

    let flush ?(index = true) ?(index_merge = false) t =
      if index_merge then Index.try_merge t.pack.index;
      Dict.flush t.pack.dict;
      IO.flush t.pack.block;
      if index then Index.flush ~no_callback:() t.pack.index;
      Tbl.clear t.staging

    let unsafe_v_no_cache ~fresh ~readonly ~lru_size ~index root =
      let pack = v index ~fresh ~readonly root in
      let staging = Tbl.create 127 in
      let lru = Lru.create lru_size in
      { staging; lru; pack; open_instances = 1; readonly }

    let unsafe_v ?(fresh = false) ?(readonly = false) ?(lru_size = 10_000)
        ~index root =
      try
        let t = Hashtbl.find roots (root, readonly) in
        if valid t then (
          if fresh then unsafe_clear t;
          t)
        else (
          Hashtbl.remove roots (root, readonly);
          raise Not_found)
      with Not_found ->
        let t = unsafe_v_no_cache ~fresh ~readonly ~lru_size ~index root in
        if fresh then unsafe_clear t;
        Hashtbl.add roots (root, readonly) t;
        t

    let v ?fresh ?readonly ?lru_size ~index root =
      let t = unsafe_v ?fresh ?readonly ?lru_size ~index root in
      Lwt.return t

    let pp_hash = Irmin.Type.pp H.t

    let pp_key ppf t =
      Fmt.pf ppf "%a:%a" pp_hash (Key.hash t) Fmt.(option int64) (Key.offset t)

    let decode_hash = Irmin.Type.(unstage (decode_bin H.t))

    let decode_len =
      Irmin.Type.(unstage (decode_bin (map int32 Int32.to_int Int32.of_int)))

    (* FIXME(samoht): too much copies *)
    (* |hash|len(value)|magic|value| *)
    let io_read_and_decode_hash ~off t =
      let buf = Bytes.create H.hash_size in
      let n = IO.read t.pack.block ~off buf in
      assert (n = H.hash_size);
      let buf = Bytes.unsafe_to_string buf in
      let _, v = decode_hash buf 0 in
      Key.v ~offset:off v

    let io_read_and_decode_len ~off t =
      let buf = Bytes.create 4 in
      let off = Int64.add off (Int64.of_int H.hash_size) in
      let n = IO.read t.pack.block ~off buf in
      assert (n = 4);
      let buf = Bytes.unsafe_to_string buf in
      let _, v = decode_len buf 0 in
      v

    exception Invalid_read

    let io_read_and_decode ~off ~len t =
      if (not (IO.readonly t.pack.block)) && off > IO.offset t.pack.block then
        raise Invalid_read;
      let len =
        match len with None -> io_read_and_decode_len ~off t | Some n -> n
      in
      let buf = Bytes.create len in
      let n = IO.read t.pack.block ~off buf in
      if n <> len then raise Invalid_read;
      let hash off =
        let k = io_read_and_decode_hash ~off t in
        Key.hash k
      in
      let dict = Dict.find t.pack.dict in
      V.decode_bin ~hash ~dict (Bytes.unsafe_to_string buf) 0

    let unsafe_mem t h =
      Log.debug (fun l -> l "[pack] mem %a" pp_hash h);
      Tbl.mem t.staging h || Lru.mem t.lru h || Index.mem t.pack.index h

    let mem t k =
      let b = unsafe_mem t (Key.hash k) in
      Lwt.return b

    let check_key k v =
      let h = V.hash v in
      if equal_hash (Key.hash k) h then Ok () else Error (k, h)

    let pp_io ppf t =
      let name = Filename.basename (Filename.dirname (IO.name t.pack.block)) in
      let mode = if t.readonly then ":RO" else "" in
      Fmt.pf ppf "%s%s" name mode

    let unsafe_find_offset ~check_integrity ~off ~len t k =
      let v = io_read_and_decode ~off ~len t in
      (if check_integrity then
       check_key k v |> function
       | Ok () -> ()
       | Error (expected, got) ->
           Fmt.failwith "corrupted value: got %a, expecting %a." pp_hash got
             pp_key expected);
      Lru.add t.lru (Key.hash k) v;
      Some v

    let unsafe_find ~check_integrity t k =
      Log.debug (fun l -> l "[pack:%a] find %a" pp_io t pp_key k);
      Stats.incr_finds ();
      let h = Key.hash k in
      match Tbl.find t.staging h with
      | v ->
          Lru.add t.lru h v;
          Some v
      | exception Not_found -> (
          match Lru.find t.lru h with
          | v -> Some v
          | exception Not_found -> (
              Stats.incr_cache_misses ();
              match Index.find t.pack.index (Key.hash k) with
              | None -> None
              | Some (off, len, _) ->
                  unsafe_find_offset ~check_integrity ~off ~len:(Some len) t k))

    let find t k =
      match Key.offset k with
      | None ->
          let v = unsafe_find ~check_integrity:true t k in
          Lwt.return v
      | Some off ->
          let v = unsafe_find_offset ~check_integrity:true ~off ~len:None t k in
          Lwt.return v

    let cast t = (t :> read_write t)

    let integrity_check ~offset ~length k t =
      try
        let value = io_read_and_decode ~off:offset ~len:length t in
        match check_key k value with
        | Ok () -> Ok ()
        | Error _ -> Error `Wrong_hash
      with Invalid_read -> Error `Absent_value

    let batch t f =
      let* r = f (cast t) in
      if Tbl.length t.staging = 0 then Lwt.return r
      else (
        flush ~index_merge:true t;
        Lwt.return r)

    let auto_flush = 1024

    let unsafe_append ~ensure_unique ~overcommit ~add_to_index t h v =
      if ensure_unique && unsafe_mem t h then
        (* FIXME(samoht): use the correct offset *) Key.v h
      else (
        Log.debug (fun l -> l "[pack] append %a" pp_hash h);
        let offset k =
          match Index.find t.pack.index k with
          | None ->
              Stats.incr_appended_hashes ();
              None
          | Some (off, _, _) ->
              Stats.incr_appended_offsets ();
              Some off
        in
        let dict = Dict.index t.pack.dict in
        let off = IO.offset t.pack.block in
        V.encode_bin ~offset ~dict v h (IO.append t.pack.block);
        let len = Int64.to_int (IO.offset t.pack.block -- off) in
        if add_to_index then
          Index.add ~overcommit t.pack.index h (off, len, V.magic v);
        if Tbl.length t.staging >= auto_flush then flush t
        else Tbl.add t.staging h v;
        Lru.add t.lru h v;
        Key.v ~offset:off h)

    let add t v =
      let h = V.hash v in
      let k = unsafe_append ~ensure_unique:true ~overcommit:true t h v in
      Lwt.return k

    let unsafe_add t h v =
      let _k = unsafe_append ~ensure_unique:true ~overcommit:true t h v in
      Lwt.return ()

    let unsafe_close t =
      t.open_instances <- t.open_instances - 1;
      if t.open_instances = 0 then (
        Log.debug (fun l -> l "[pack] close %s" (IO.name t.pack.block));
        Tbl.clear t.staging;
        Lru.clear t.lru;
        close t.pack)

    let close t =
      unsafe_close t;
      Lwt.return_unit

    let clear t =
      unsafe_clear t;
      Lwt.return_unit

    let clear_keep_generation t =
      unsafe_clear ~keep_generation:() t;
      Lwt.return_unit

    let clear_caches t =
      Tbl.clear t.staging;
      Lru.clear t.lru

    let sync ?(on_generation_change = Fun.id) t =
      let former_generation = IO.generation t.pack.block in
      let generation = IO.force_generation t.pack.block in
      if former_generation <> generation then (
        Log.debug (fun l -> l "[pack] generation changed, refill buffers");
        clear_caches t;
        on_generation_change ();
        IO.close t.pack.block;
        let block =
          IO.v ~fresh:false ~version:(Some current_version) ~readonly:true
            (IO.name t.pack.block)
        in
        t.pack.block <- block;
        Dict.sync t.pack.dict)
      else Dict.sync t.pack.dict;
      Index.sync t.pack.index

    let version t = IO.version t.pack.block
    let generation t = IO.generation t.pack.block
    let offset t = IO.offset t.pack.block
  end
end
