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

module Pack = Pack
module Dict = Pack_dict
module Index = Pack_index
module Config = Config
module Inode = Inode

module type VERSION = IO.VERSION

val config :
  ?fresh:bool ->
  ?readonly:bool ->
  ?lru_size:int ->
  ?index_log_size:int ->
  ?merge_throttle:Config.merge_throttle ->
  ?freeze_throttle:Config.freeze_throttle ->
  string ->
  Irmin.config
(** Configuration options for stores.

    @param fresh whether an existing store should be overwritten.
    @param read_only whether read-only mode is enabled for this store.
    @param lru_size the maximum number of bindings in the lru cache.
    @param index_log_size the maximum number of bindings in the index cache.
    @param index_throttle the strategy to use when the index cache is full and
    an async [Index.merge] in already in progress. [Block_writes] (the default)
    blocks any new writes until the merge is completed. [Overcommit_memory] does
    not block but indefinitely expands the in-memory cache. *)

exception RO_Not_Allowed
exception Unsupported_version of IO.version

module Make_ext
    (_ : IO.VERSION)
    (Config : Config.S)
    (Metadata : Irmin.Metadata.S)
    (Contents : Irmin.Contents.S)
    (Path : Irmin.Path.S)
    (Branch : Irmin.Branch.S)
    (Hash : Irmin.Hash.S)
    (N : Irmin.Private.Node.S
           with type metadata = Metadata.t
            and type step = Path.step)
    (CT : Irmin.Private.Commit.S) : sig
  include
    Irmin.S
      with type key = Path.t
       and type contents = Contents.t
       and type branch = Branch.t
       and type hash = Hash.t
       and type step = Path.step
       and type metadata = Metadata.t
       and type Key.step = Path.step
       and type Private.Sync.endpoint = unit

  include Store.S with type repo := repo

  val reconstruct_index : ?output:string -> Irmin.config -> unit

  val integrity_check_inodes :
    ?heads:commit list ->
    repo ->
    ([> `Msg of string ], [> `Msg of string ]) result Lwt.t
end

module type MAKER = functor
  (Config : Config.S)
  (M : Irmin.Metadata.S)
  (C : Irmin.Contents.S)
  (P : Irmin.Path.S)
  (B : Irmin.Branch.S)
  (H : Irmin.Hash.S)
  -> sig
  include
    Irmin.S
      with type key = P.t
       and type step = P.step
       and type metadata = M.t
       and type contents = C.t
       and type branch = B.t
       and type hash = H.t
       and type Private.Sync.endpoint = unit

  include Store.S with type repo := repo

  val reconstruct_index : ?output:string -> Irmin.config -> unit

  val integrity_check_inodes :
    ?heads:commit list ->
    repo ->
    ([> `Msg of string ], [> `Msg of string ]) result Lwt.t
end

module Make : MAKER
module Make_V2 : MAKER
module KV (Config : Config.S) : Irmin.KV_MAKER

module Atomic_write (K : Irmin.Type.S) (V : Irmin.Hash.S) (_ : IO.VERSION) : sig
  include Irmin.ATOMIC_WRITE_STORE with type key = K.t and type value = V.t

  val v : ?fresh:bool -> ?readonly:bool -> string -> t Lwt.t
end

module Stats = Stats
module Layout = Layout
module Checks = Checks
module Store = Store

module Private : sig
  module Closeable = Closeable
  module Inode = Inode
  module IO = IO
  module Pack_index = Pack_index
  module Sigs = S
  module Utils = Utils
end
