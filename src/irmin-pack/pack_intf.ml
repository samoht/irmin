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

open! Import
open Store_properties
module Sigs = S

module type ID = sig
  include Irmin.Id.S

  val offset : t -> int64 option
  val v : offset:int64 -> hash -> t
end

module type ELT = sig
  include Irmin.Type.S

  type hash

  val hash : t -> hash
  val magic : t -> char

  val encode_bin :
    dict:(string -> int option) ->
    offset:(hash -> int64 option) ->
    t ->
    hash ->
    (string -> unit) ->
    unit

  val decode_bin :
    dict:(int -> string option) -> hash:(int64 -> hash) -> string -> int -> t
end

module type S = sig
  include Irmin.CONTENT_ADDRESSABLE_STORE

  (* FIXME: remove add/unsafe_add duplication *)

  val add : 'a t -> value -> key Lwt.t
  (** Overwrite [add] to work with a read-only database handler. *)

  val unsafe_add : 'a t -> hash -> value -> key Lwt.t
  (** Overwrite [unsafe_add] to work with a read-only database handler. *)

  type index

  val v :
    ?fresh:bool ->
    ?readonly:bool ->
    ?lru_size:int ->
    index:index ->
    string ->
    read t Lwt.t

  include BATCH with type 'a t := 'a t
  (** @inline *)

  val unsafe_append :
    ensure_unique:bool -> overcommit:bool -> 'a t -> hash -> value -> key

  val unsafe_mem : 'a t -> key -> bool
  val unsafe_find : check_integrity:bool -> 'a t -> key -> value option
  val flush : ?index:bool -> ?index_merge:bool -> 'a t -> unit

  val sync : ?on_generation_change:(unit -> unit) -> 'a t -> unit
  (** syncs a readonly instance with the files on disk. The same file instance
      is shared between several pack instances. Therefore only the first pack
      instance that checks a generation change, can see it.
      [on_generation_change] is a callback for all pack instances to react to a
      generation change. *)

  val version : 'a t -> IO.version
  val generation : 'a t -> int64
  val offset : 'a t -> int64

  include Sigs.CHECKABLE with type 'a t := 'a t and type hash := hash
  include CLOSEABLE with type 'a t := 'a t
  include CLEARABLE with type 'a t := 'a t

  val clear_caches : 'a t -> unit
  (** [clear_cache t] clears all the in-memory caches of [t]. Persistent data
      are not removed. *)

  val clear_keep_generation : 'a t -> unit Lwt.t
end

module type MAKER = sig
  type key
  type index
  type hash

  (** Save multiple kind of values in the same pack file. Values will be
      distinguished using [V.magic], so they have to all be different. *)
  module Make (V : ELT with type hash := hash) :
    S
      with type key = key
       and type value = V.t
       and type index = index
       and type hash = hash
end

module type Pack = sig
  module type ELT = ELT
  module type S = S
  module type MAKER = MAKER

  module File
      (Index : Pack_index.S)
      (Id : ID with type hash = Index.key)
      (_ : IO.VERSION) :
    MAKER with type key = Id.t and type index = Index.t and type hash = Id.hash
end
