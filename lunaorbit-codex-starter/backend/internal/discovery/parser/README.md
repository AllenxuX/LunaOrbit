# Restricted MM2 declaration parser

`Parse(key, text)` selects YAML for `.yaml`/`.yml` keys and Java properties
otherwise. It returns only aliases, validated bootstrap endpoints, directed
rules, fixed issue codes and a SHA-256 fingerprint of that safe result.
Unknown fields and credentials never enter the result or fingerprint.

Properties supports comments, Java escapes (including Unicode surrogate pairs),
escaped separators and continued lines. Topic selectors retain their semantic
string after properties decoding; YAML scalar selectors retain their decoded
value. No regular expression evaluation or actual topic counting occurs.

YAML supports flat properties-equivalent mappings, `clusters` scalar/sequence,
`data`/`config`/`mm2` wrappers and `*.properties` scalar blocks. `config` and `mm2`
scalar blocks may also contain properties. Other wrapper names are ignored.
Only explicit directional keys create rules; absent `enabled` stays unspecified
(`EnabledExplicit=false`). A topics rule alone does not identify an MM2 workload.
Global replication policy is copied to rules without a directional policy.
`DeclaredAliases` contains only explicit valid `clusters` members. `Aliases`
also contains aliases inferred from directions or bootstrap keys. Both fields
participate in the fingerprint, preserving the distinction in declaration evidence.

All errors contain only `invalid_configuration` or `configuration_limit`.
Invalid aliases, endpoints, selectors and policies produce fixed issue codes.
Repeated semantic keys fail closed instead of selecting one declaration.
Anchors/aliases, additional YAML documents and non-scalar semantic values fail
closed. Unknown YAML structure is bounded and validated; sensitive key subtrees
are skipped without inspection. No alias resolution or arbitrary unmarshalling
occurs. YAML input is capped before decoding.

Limits: key 256 bytes, input 1 MiB, properties 4096 physical lines, semantic
entries 2048, YAML 24 levels/8192 nodes, aliases 128, directed rules 256,
bootstrap endpoints 64 per alias, alias 63 bytes, topic selector 8192 bytes,
policy 256 bytes. Hosts must be DNS-like names or IP literals with an explicit
port 1–65535; URLs and userinfo are rejected. Validation does not authorize
network connections or assert host reachability.

Fixtures use synthetic names and ignored synthetic credential markers only.
