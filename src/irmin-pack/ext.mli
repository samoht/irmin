(*
 * Copyright (c) 2013-2020 Thomas Gazagnaire <thomas@gazagnaire.org>
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

module Pack_config = Config
module Index = Pack_index

exception RO_Not_Allowed
exception Unsupported_version of IO.version

module Make
    (_ : IO.VERSION)
    (Config : Config.S)
    (Metadata : Irmin.Metadata.S)
    (Contents : Irmin.Contents.S)
    (Path : Irmin.Path.S)
    (Branch : Irmin.Branch.S)
    (Hash : Irmin.Hash.S)
    (Id : Pack_intf.ID with type hash = Hash.t)
    (N : Irmin.Private.Node.S
           with type metadata = Metadata.t
            and type key = Id.t
            and type step = Path.step)
    (CT : Irmin.Private.Commit.S with type key = Id.t) : sig
  include
    Irmin.S
      with type key = Path.t
       and type id = Id.t
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

module Id (H : Irmin.Hash.S) : Pack_intf.ID with type hash = H.t
