(*
 * Copyright (c) 2018-2021 Tarides <contact@tarides.com>
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

include Content_addressable_intf
open! Import

module Table (K : Irmin.Hash.S) = Hashtbl.Make (struct
  type t = K.t

  let hash = K.short_hash
  let equal = Irmin.Type.(unstage (equal K.t))
end)

module type Key = Irmin.Key.S with type metadata = int63

module Maker (V : Version.S) (Index : Pack_index.S) = struct
  module IO_cache = IO.Cache
  module IO = IO.Unix
  module Dict = Pack_dict.Make (V)

  type index = Index.t
  type hash = Index.key

  type 'a t = {
    mutable block : IO.t;
    index : Index.t;
    dict : Dict.t;
    mutable open_instances : int;
  }

  let clear ?keep_generation t =
    Index.clear t.index;
    match V.version with
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
    let block = IO.v ~version:(Some V.version) ~fresh ~readonly file in
    { block; index; dict; open_instances = 1 }

  let IO_cache.{ v } =
    IO_cache.memoize ~clear ~valid ~v:(fun index -> unsafe_v ~index) Layout.pack

  let close t =
    t.open_instances <- t.open_instances - 1;
    if t.open_instances = 0 then (
      if not (IO.readonly t.block) then IO.flush t.block;
      IO.close t.block;
      Dict.close t.dict)

  module Make
      (K : S.Key with type hash = hash)
      (Val : Value with type key := K.t and type hash := hash) =
  struct
    module Tbl = Table (K.Hash)

    module Lru = Irmin.Private.Lru.Make (struct
      include K.Hash

      let hash = K.Hash.short_hash
      let equal = Irmin.Type.(unstage (equal K.Hash.t))
    end)

    type kind = [ `Commit | `Node | `Contents ]

    type nonrec 'a t = {
      kind : kind;
      pack : 'a t;
      lru : (K.t * Val.t) Lru.t;
      staging : (K.t * Val.t) Tbl.t;
      mutable open_instances : int;
      readonly : bool;
    }

    type key = K.t
    type hash = K.hash
    type value = Val.t
    type index = Index.t

    let equal_hash = Irmin.Type.(unstage (equal K.Hash.t))

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

    let unsafe_v_no_cache ~fresh ~readonly ~lru_size ~index ~kind root =
      let pack = v index ~fresh ~readonly root in
      let staging = Tbl.create 127 in
      let lru = Lru.create lru_size in
      { kind; staging; lru; pack; open_instances = 1; readonly }

    let unsafe_v ?(fresh = false) ?(readonly = false) ?(lru_size = 10_000)
        ~index ~kind root =
      try
        let t = Hashtbl.find roots (root, readonly) in
        if valid t then (
          if fresh then unsafe_clear t;
          t)
        else (
          Hashtbl.remove roots (root, readonly);
          raise Not_found)
      with Not_found ->
        let t =
          unsafe_v_no_cache ~fresh ~readonly ~lru_size ~index ~kind root
        in
        if fresh then unsafe_clear t;
        Hashtbl.add roots (root, readonly) t;
        t

    let v ?fresh ?readonly ?lru_size ~index ~kind root =
      let t = unsafe_v ?fresh ?readonly ?lru_size ~index ~kind root in
      Lwt.return t

    let pp_key = Irmin.Type.pp K.t
    let pp_hash = Irmin.Type.pp K.Hash.t
    let decode_hash = Irmin.Type.(unstage (decode_bin K.Hash.t))

    let io_read_and_decode_hash ~off t =
      let buf = Bytes.create K.Hash.hash_size in
      let n = IO.read t.pack.block ~off buf in
      assert (n = K.Hash.hash_size);
      let _, h = decode_hash (Bytes.unsafe_to_string buf) 0 in
      K.v ~metadata:off h

    let mem_hash t h =
      Tbl.mem t.staging h || Lru.mem t.lru h || Index.mem t.pack.index h

    let unsafe_mem t k =
      Log.debug (fun l -> l "[pack] mem %a" pp_key k);
      mem_hash t (K.hash k)

    let mem t k = Lwt.return (unsafe_mem t k)

    let check_key k v =
      let h = Val.hash v in
      if equal_hash (K.hash k) h then Ok () else Error (k, h)

    exception Invalid_read

    let io_read_and_decode ~off ~len t =
      if (not (IO.readonly t.pack.block)) && off > IO.offset t.pack.block then
        raise Invalid_read;
      let buf = Bytes.create len in
      let n = IO.read t.pack.block ~off buf in
      if n <> len then raise Invalid_read;
      let key off = io_read_and_decode_hash ~off t in
      let dict = Dict.find t.pack.dict in
      Val.decode_bin ~key ~dict (Bytes.unsafe_to_string buf) 0

    let pp_io ppf t =
      let name = Filename.basename (Filename.dirname (IO.name t.pack.block)) in
      let mode = if t.readonly then ":RO" else "" in
      Fmt.pf ppf "%s%s" name mode

    let find_hash ~not_in_caches t h =
      Log.debug (fun l -> l "[pack:%a] find %a" pp_io t pp_hash h);
      Stats.incr_finds ();
      match Tbl.find t.staging h with
      | v ->
          Lru.add t.lru h v;
          Some v
      | exception Not_found -> (
          match Lru.find t.lru h with
          | v -> Some v
          | exception Not_found ->
              Stats.incr_cache_misses ();
              not_in_caches ())

    let unsafe_find ~check_integrity t k =
      let h = K.hash k in
      let not_in_caches () =
        match Index.find t.pack.index h with
        | None -> None
        | Some (off, len, _) ->
            let v = io_read_and_decode ~off ~len t in
            (if check_integrity then
             check_key k v |> function
             | Ok () -> ()
             | Error (expected, got) ->
                 Fmt.failwith "corrupted value: got %a, expecting %a." pp_hash
                   got pp_key expected);
            Lru.add t.lru h (k, v);
            Some (k, v)
      in
      find_hash ~not_in_caches t h |> Option.map snd

    let find t k = Lwt.return (unsafe_find ~check_integrity:true t k)
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

    let unsafe_append ~ensure_unique ~overcommit t h v =
      let append () =
        Log.debug (fun l -> l "[pack] append %a" pp_hash h);
        let offset k =
          match Index.find t.pack.index (K.hash k) with
          | None ->
              Stats.incr_appended_hashes ();
              None
          | Some (off, _, _) ->
              Stats.incr_appended_offsets ();
              Some off
        in
        let dict = Dict.index t.pack.dict in
        let off = IO.offset t.pack.block in
        let k = K.v ~metadata:off h in
        Val.encode_bin ~offset ~dict v k (IO.append t.pack.block);
        let len = Int63.to_int (IO.offset t.pack.block -- off) in
        if t.kind = `Commit then
          Index.add ~overcommit t.pack.index h (off, len, Val.magic v);
        if Tbl.length t.staging >= auto_flush then flush t
        else Tbl.add t.staging h (k, v);
        Lru.add t.lru h (k, v);
        k
      in
      if not ensure_unique then append ()
      else
        match find_hash t h ~not_in_caches:(fun () -> None) with
        | None -> append ()
        | Some (k, _) -> k

    let add t v =
      let h = Val.hash v in
      let k = unsafe_append ~ensure_unique:true ~overcommit:true t h v in
      Lwt.return k

    let unsafe_add t h v =
      let k = unsafe_append ~ensure_unique:true ~overcommit:true t h v in
      Lwt.return k

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
      let former_offset = IO.offset t.pack.block in
      let former_generation = IO.generation t.pack.block in
      let h = IO.force_headers t.pack.block in
      if former_generation <> h.generation then (
        Log.debug (fun l -> l "[pack] generation changed, refill buffers");
        clear_caches t;
        on_generation_change ();
        IO.close t.pack.block;
        let block =
          IO.v ~fresh:false ~version:(Some V.version) ~readonly:true
            (IO.name t.pack.block)
        in
        t.pack.block <- block;
        Dict.sync t.pack.dict;
        Index.sync t.pack.index)
      else if h.offset > former_offset then (
        Dict.sync t.pack.dict;
        Index.sync t.pack.index)

    let version t = IO.version t.pack.block
    let generation t = IO.generation t.pack.block
    let offset t = IO.offset t.pack.block
  end
end

(* FIXME: remove code duplication with irmin/content_addressable *)
module Closeable (S : S) = struct
  type 'a t = { closed : bool ref; t : 'a S.t }
  type key = S.key
  type hash = S.hash
  type value = S.value
  type index = S.index

  let check_not_closed t = if !(t.closed) then raise Irmin.Closed

  let mem t k =
    check_not_closed t;
    S.mem t.t k

  let find t k =
    check_not_closed t;
    S.find t.t k

  let add t v =
    check_not_closed t;
    S.add t.t v

  let unsafe_add t k v =
    check_not_closed t;
    S.unsafe_add t.t k v

  let batch t f =
    check_not_closed t;
    S.batch t.t (fun w -> f { t = w; closed = t.closed })

  let v ?fresh ?readonly ?lru_size ~index ~kind root =
    let+ t = S.v ?fresh ?readonly ?lru_size ~index ~kind root in
    { closed = ref false; t }

  let close t =
    if !(t.closed) then Lwt.return_unit
    else (
      t.closed := true;
      S.close t.t)

  let unsafe_append ~ensure_unique ~overcommit t k v =
    check_not_closed t;
    S.unsafe_append ~ensure_unique ~overcommit t.t k v

  let unsafe_mem t k =
    check_not_closed t;
    S.unsafe_mem t.t k

  let unsafe_find ~check_integrity t k =
    check_not_closed t;
    S.unsafe_find ~check_integrity t.t k

  let flush ?index ?index_merge t =
    check_not_closed t;
    S.flush ?index ?index_merge t.t

  let sync ?on_generation_change t =
    check_not_closed t;
    S.sync ?on_generation_change t.t

  let clear t =
    check_not_closed t;
    S.clear t.t

  let integrity_check ~offset ~length k t =
    check_not_closed t;
    S.integrity_check ~offset ~length k t.t

  let clear_caches t =
    check_not_closed t;
    S.clear_caches t.t

  let version t =
    check_not_closed t;
    S.version t.t

  let generation t =
    check_not_closed t;
    S.generation t.t

  let offset t =
    check_not_closed t;
    S.offset t.t

  let clear_keep_generation t =
    check_not_closed t;
    S.clear_keep_generation t.t
end
