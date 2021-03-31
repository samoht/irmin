(*
 * Copyright (c) 2013-2021 Thomas Gazagnaire <thomas@gazagnaire.org>
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

(** Private module: turn a Git store into an Irmin backend for Git trees. *)

open Import
open Store_properties

module Make (G : Git.S) (P : Irmin.Path.S) : sig
  include
    Irmin.Content_addressable.S
      with type key = G.Hash.t
       and type value = G.Value.Tree.t

  (** @inline *)
  include Checkable with type 'a t := 'a t and type 'a raw := G.t

  module Key : Irmin.Hash.S with type t = key

  module Val :
    Irmin.Private.Node.S
      with type t = value
       and type hash = key
       and type step = P.step
       and type metadata = Metadata.t
end
