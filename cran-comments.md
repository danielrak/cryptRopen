## Submission type

First CRAN submission of cryptRopen.

## Test environments

* Local Windows 11, R 4.5.1
* GitHub Actions:
    * ubuntu-latest, R-release and R-devel
    * windows-latest, R-release
    * macos-latest, R-release
* win-builder (devel + release)
* R-hub (linux, windows, macos, nosuggests)

## R CMD check results

0 errors | 0 warnings | 0 notes

(On Windows, one environmental NOTE may appear:
`checking for future file timestamps ... unable to verify current time`
— it is unrelated to the package and only reflects the runner's
inability to reach an NTP server.)

## Notes for the reviewers

* The package implements salted-hash pseudonymization, not reversible
  encryption. The title and description make this explicit and the
  Getting Started vignette has a dedicated "Pseudonymization, not
  encryption" section. The legacy `crypt` in the package name is
  acknowledged as historical.
* The spell checker flags a few entries that are correct in English
  (US): "Pseudonymization", "Industrialized", "deterministic",
  "GDPR", "MD5", "RAM", and the parquet / CSV format names. Single
  quotes are used in DESCRIPTION only around external package names
  (`'digest'`, `'mirai'`, `'arrow'`), per CRAN convention.
* The asynchronous test suite (`tests/testthat/test-crypt_r_async.R`)
  spawns `mirai` daemons and is skipped on CRAN via
  `testthat::skip_on_cran()`. Full coverage of the orchestration
  contract is exercised on every push in the GitHub Actions
  R-CMD-check workflow.
* The byte-level non-regression fixtures under `tests/baseline/` are
  excluded from the tarball via `.Rbuildignore`. The matching test
  file (`tests/testthat/test-baseline.R`) auto-skips through
  `skip_if_no_baseline()` when the directory is absent.

## Reverse dependencies

None — this is the first submission.
