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

open! Import
include Irmin_layers_intf

module Layer_id = struct
  type t = layer_id [@@deriving irmin]

  let to_string = function
    | `Upper0 -> "upper0"
    | `Upper1 -> "upper1"
    | `Lower -> "lower"

  let pp = Fmt.of_to_string to_string
end

module Maker_ext
    (CA : Irmin.Content_addressable.Maker)
    (AW : Irmin.Atomic_write.Maker)
    (N : Irmin.Private.Node.Maker)
    (CT : Irmin.Private.Commit.Maker) =
struct
  module Make
      (M : Irmin.Metadata.S)
      (C : Irmin.Contents.S)
      (P : Irmin.Path.S)
      (B : Irmin.Branch.S)
      (H : Irmin.Hash.S) =
  struct
    module Maker = Irmin.Maker_ext (CA) (AW) (N) (CT)
    include Maker.Make (M) (C) (P) (B) (H)

    let freeze ?min_lower:_ ?max_lower:_ ?min_upper:_ ?max_upper:_ ?recovery:_
        _repo =
      Lwt.fail_with "not implemented"

    type store_handle =
      | Commit_t : commit_id -> store_handle
      | Node_t : node_id -> store_handle
      | Content_t : contents_id -> store_handle

    let layer_id _repo _store_handle = Lwt.fail_with "not implemented"
    let async_freeze _ = failwith "not implemented"
    let upper_in_use _repo = failwith "not implemented"
    let self_contained ?min:_ ~max:_ _repo = failwith "not implemented"
    let check_self_contained ?heads:_ _ = failwith "not implemented"
    let needs_recovery _ = failwith "not implemented"

    module Private_layer = struct
      module Hook = struct
        type 'a t = unit

        let v _ = failwith "not implemented"
      end

      let wait_for_freeze _ = Lwt.fail_with "not implemented"

      let freeze' ?min_lower:_ ?max_lower:_ ?min_upper:_ ?max_upper:_
          ?recovery:_ ?hook:_ _repo =
        Lwt.fail_with "not implemented"

      let upper_in_use = upper_in_use
    end
  end
end

module Maker
    (CA : Irmin.Content_addressable.Maker)
    (AW : Irmin.Atomic_write.Maker) =
  Maker_ext (CA) (AW) (Irmin.Private.Node) (Irmin.Private.Commit)

module Stats = Stats
