# placement-drift — replicas stuck in one zone after the other comes back

**kind only.** Labels the workers into two placement domains on
`lookout-examples/zone` (`zone-a`, `zone-b`) and briefly taints `zone-b`.
Exercises `topology-drift` and `LeewayPolicy` end-to-end across both
`leeway report` and `leeway watch` (real `kube-scheduler`, real informers,
real HTTP webhook).

```sh
examples/e2e placement-drift
```
