# compute-class-fallback — fallback rank degradation, wedged scale-up, and resolution

Exercises the `compute-class` source end-to-end across both `leeway report`
and `leeway watch`:

1. Installs the `ComputeClass` CRD (`cloud.google.com/v1`) and creates
   `gold-ccc` (`whenUnsatisfiable: DoNotScaleUp`, rank 0 `n4`, rank 1 `n2`).
2. Schedules `pod-running-rank1` onto rank 1 -> fires `leeway.rank_degraded` (`last-rank`).
3. Creates `pod-wedged` (`NotTriggerScaleUp`) alongside `pod-provisioning`
   (`TriggeredScaleUp`) -> fires `leeway.rank_wedged` (`pendingDeclined=1`,
   `pendingProvisioning=1`).
4. Deletes `pod-wedged` while `pod-provisioning` remains `Pending` -> verifies
   `leeway watch` emits `kind=resolved` (`resolution=recovered`) to the webhook.

```sh
examples/e2e compute-class-fallback
```
