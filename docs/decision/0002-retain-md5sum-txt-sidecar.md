# 2. Retain the md5sum.txt checksum sidecar

## Status

Accepted

## Context

When the harmoniser publishes a harmonised file to the FTP area (`ftp_copy.nf`), it had been
writing a small `md5sum.txt` sidecar next to it, containing the md5 checksums of
`<GCST>.h.tsv.gz` and `<GCST>.h.tsv.gz.tbi` in standard `md5sum` format:

```
<md5>  <GCST>.h.tsv.gz
<md5>  <GCST>.h.tsv.gz.tbi
```

This sidecar was removed in commit `d6abed1` ("stop updating md5sum.txt") on the `dev` branch,
on the grounds that the harmonised file's md5 is *also* computed and written into the harmonised
metadata YAML (`<GCST>.h.tsv.gz-meta.yaml`, via `update_meta_yaml.nf`), so the sidecar looked
like redundant duplication.

That reasoning misses that the two outputs serve different purposes and have different
consumers:

- The **metadata YAML** carries the md5 as one field among many descriptive fields; it is a
  record/description artifact whose schema and lifecycle may change.
- The **`md5sum.txt` sidecar** is a machine-readable, self-describing checksum file in the
  conventional `md5sum -c` format. It lets any downstream consumer verify a downloaded file, or
  read the harmonised file's checksum, **without parsing the full metadata YAML** and
  **independently of any future change to that YAML's schema or retention**.

A concrete downstream consumer relies on this: the GWAS Catalog's metadata publishing reads the
harmonised file's md5 from `md5sum.txt` rather than re-hashing large `.tsv.gz` files or coupling
to the metadata-YAML schema. Removing the sidecar would force that consumer (and any external
one) to parse the whole metadata YAML for a single value.

## Decision

**Reinstate writing `md5sum.txt`** in `ftp_copy.nf`, and treat it as a **published, supported
output** of the workflow. The two `echo` lines that write the `.h.tsv.gz` and `.h.tsv.gz.tbi`
checksums are restored.

The harmonised md5 intentionally appears in two outputs — the metadata YAML (descriptive
context) and the `md5sum.txt` sidecar (standalone file verification). Both are derived from the
same `md5sum` of the same file within the same process, so they cannot diverge in practice. This
is deliberate, conventional redundancy (a checksum file alongside a data file), not accidental
duplication.

The Python 3.13 upgrade that was bundled into commit `d6abed1` is unaffected and retained.

## Consequences

**Positive:**

- Restores the standard "checksum file next to the data file" convention, usable directly with
  `md5sum -c`.
- Downstream consumers (including the GWAS Catalog) can verify downloads and obtain the
  harmonised file's checksum without parsing the metadata YAML, decoupling them from that YAML's
  schema and lifecycle.
- The sidecar is a stable, minimal public contract that the workflow commits to producing.

**Negative / risks:**

- The harmonised md5 is emitted in two places that must remain consistent. In practice they
  cannot diverge: both are produced from the same `md5sum` of the same file in the same run.
