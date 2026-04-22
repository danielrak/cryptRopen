globalVariables(unique(c(
  ".", "to_encrypt", "encrypted_file",
  "dupl_encrypted_file", "ndupl_encrypted_file",
  # .inspect_column() uses tibble()'s lazy self-referencing columns
  # (e.g. prop_distinct = nb_distinct / rows). R CMD check does not
  # see these as locals; declare them here to silence the NOTE.
  "nb_distinct", "nb_na", "nb_void"
)))
