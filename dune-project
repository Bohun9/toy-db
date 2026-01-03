(lang dune 3.11)

(name toydb)

(using menhir 2.1)

(generate_opam_files true)

(source
 (github username/reponame))

(authors "Author Name")

(maintainers "Maintainer Name")

(license LICENSE)

(documentation https://url/to/documentation)

(package
 (name toydb)
 (synopsis "A short synopsis")
 (description "A longer description")
 (depends
   (ocaml (>= 5.4))
   dune
   ocamlgraph
   domainslib
   menhir
   ppx_deriving
   (ounit2 :with-test)
   (ocamlformat (and (= 0.28.1) :with-dev-setup)))
 (tags
  (topics "to describe" your project)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
