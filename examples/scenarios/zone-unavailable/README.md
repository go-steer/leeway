# zone-unavailable — a whole zone goes; one finding, not one per workload

**kind only.** Cordons `zone-b` (`${CLUSTER_NAME}-worker2`), leaving the zone
with no usable node. Asserts that `leeway report` and `leeway watch` emit
exactly one `leeway.domain_unavailable` finding (`taint_exclusion`) and
suppress per-workload `contract_violated` / `placement_drift` findings.

```sh
examples/e2e zone-unavailable
```
