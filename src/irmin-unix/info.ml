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

let read_line cmd =
  let ic = Unix.open_process_in cmd in
  let line = input_line ic in
  close_in ic;
  line

let user = lazy
  (try read_line "git config user.name"
   with Unix.Unix_error _ -> Unix.gethostname())

let email = lazy
  (try read_line "git config user.email"
   with Unix.Unix_error _ -> "irmin@mirage.io")

let v ?extra ?author fmt =
  Fmt.kstrf (fun msg () ->
      let date = Int64.of_float (Unix.gettimeofday ()) in
      let author = match author with
        | Some a -> a
        | None   -> Fmt.strf "%s <%s>" (Lazy.force user) (Lazy.force email)
      in
      Irmin.Info.v ~date ~author ?extra msg
    ) fmt
