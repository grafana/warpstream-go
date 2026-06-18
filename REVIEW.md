# Code Review: Simulation Framework

## Executive Summary

This PR adds a comprehensive simulation framework (~2,035 lines) to compare `wgo` (the Warpstream client) against `kgo` (franz-go) under various failure and latency scenarios. The framework is well-architected, thoroughly tested, and provides valuable empirical validation of the client's hedging and demotion strategies.

**Overall Assessment:** ✅ **Approve with Minor Suggestions**

The code is production-quality with excellent test coverage, clear separation of concerns, and strong adherence to Go best practices.

---

## Architecture & Design

### ✅ Strengths

1. **Clear Separation of Concerns**
   - Each component has a single, well-defined responsibility
   - Clean interfaces (`produceClient`, `brokersBehaviour`)
   - Pipeline architecture is intuitive: routing → buffering → hedging → wire

2. **Parallel Execution Model**
   - Scenarios run concurrently in isolated environments (one kfake cluster per client)
   - Eliminates cross-contamination between scenarios
   - Maximizes throughput on multi-core systems

3. **Data-Driven Design**
   - Behaviour providers use atomic updates for thread-safe reconfiguration
   - Per-client, per-broker random streams with deterministic seeding
   - Reproducible latency/failure distributions

4. **Realistic Test Harness**
   - Mirrors production configuration from `pkg/storage/ingest/writer_client.go`
   - Includes warmup phase for stats tracker stabilization
   - Models application-level request semantics (timeouts, fan-out)

### 🟡 Areas for Improvement

1. **Environment Setup Complexity**
   - `newEnvironment()` has significant setup/teardown logic with deferred cleanup
   - Consider extracting cluster creation into separate helpers:
     ```go
     func newWgoTestEnvironment(...) (*wgoEnv, error)
     func newKgoTestEnvironment(...) (*kgoEnv, error)
     ```

2. **Magic Number Documentation**
   ```go
   const clusterSize = int32(50)  // Why 50? Document the reasoning
   const warmupDuration = 30 * time.Second  // Why 30s? Link to stats tracker requirements
   ```
   **Suggestion:** Add doc comments explaining the rationale for each constant.

3. **Scenario Definition DSL**
   - The `scenarios()` function mixes construction logic with scenario definitions
   - Some scenarios use inline functions for complex setup
   - Consider a more declarative builder pattern:
     ```go
     scenario{
       name: "25% slow agents",
       behaviours: behaviourBuilder().
         healthy().
         withSlow(agentFraction(0.25), highLatency).
         build(),
       expectedMinSuccessRate: 1.0,
     }
     ```

---

## Code Quality

### ✅ Strong Points

1. **Excellent Naming**
   - Function names clearly describe intent (`healthyBehaviours`, `failingBehavioursFor`)
   - Variables are concise but descriptive (`beh`, `nodeID`, `presp`)
   - Type names accurately reflect their role (`brokersBehaviourProvider`)

2. **Minimal Comments, Maximum Clarity**
   - Follows `/workspace/pkg/AGENTS.md` convention: "comment the why, not the what"
   - Doc comments on exported types and functions are concise and helpful
   - Code is self-documenting through structure and naming

3. **Error Handling**
   - Proper error propagation throughout
   - Deferred cleanup on error paths in `newEnvironment()`
   - Context cancellation handled correctly

4. **Concurrency Safety**
   - `observations` uses proper mutex protection
   - `brokersBehaviourProvider` uses atomic pointers + mutex for RNG map
   - No data races detected by race detector (tests pass with `-race`)

### 🔴 Issues to Address

1. **Panic-Driven Validation** (`scenario.go`)
   ```go
   if n <= 0 || n >= clusterSize {
       panic(fmt.Sprintf("simulation: %s split computed %d agents...", ...))
   }
   ```
   **Issue:** Panics are appropriate for scenario construction errors (caught at startup), but consider adding early validation at the top of `scenarios()` to fail fast with a clear message.

2. **Silent Integer Division** (`scenario.go:26`)
   ```go
   n := clusterSize * numerator / denominator
   ```
   **Issue:** Integer division silently floors. For `clusterSize=50`, `25%` → 12 agents (not 12.5). The code guards against degenerate cases, but documenting the rounding behavior would help.
   
   **Suggestion:**
   ```go
   // agentCount computes numerator/denominator of clusterSize, flooring to
   // the nearest integer. Panics if the result is 0 or >= clusterSize.
   ```

3. **Context Deadline Handling** (`scenario_runner.go:27`)
   ```go
   budget := warmupDuration + observedDuration + 60*time.Second
   ```
   **Issue:** The extra 60s buffer is implicit. Why 60s? Add a named constant:
   ```go
   const scenarioBufferDuration = 60 * time.Second
   budget := warmupDuration + observedDuration + scenarioBufferDuration
   ```

4. **Potential Race in latencyDelayedKgoClient** (`environment.go:228`)
   ```go
   c.Client.Produce(ctx, r, func(rec *kgo.Record, err error) {
       go func() {
           c.behaviours.nextLatencySleepFor(ctx, c.client, rec.Partition)
           if err == nil && ctx.Err() != nil {
               err = ctx.Err()  // ⚠️ Writes to err captured from outer scope
           }
           promise(rec, err)
       }()
   })
   ```
   **Issue:** The `err` variable is captured from the outer scope and conditionally reassigned inside the goroutine. While this works because `err` is passed by value to the closure, it's confusing.
   
   **Fix:**
   ```go
   c.Client.Produce(ctx, r, func(rec *kgo.Record, err error) {
       go func() {
           c.behaviours.nextLatencySleepFor(ctx, c.client, rec.Partition)
           finalErr := err
           if finalErr == nil && ctx.Err() != nil {
               finalErr = ctx.Err()
           }
           promise(rec, finalErr)
       }()
   })
   ```

---

## Testing

### ✅ Excellent Coverage

1. **Unit Tests for Core Components**
   - `broker_behaviour_test.go`: Tests latency distribution, failure sampling, and reproducibility
   - `observations_test.go`: Tests thread-safety, quantile calculation, and bucketing
   - `sampler_test.go`: Tests counter sampling with boundary alignment
   - `scenario_runner_test.go`: Tests event scheduling and timeout handling

2. **Statistical Validation**
   - `TestTriangular` validates distribution properties over 100k samples
   - `TestBurstyLatency_AddsExpectedExtraLatency` checks empirical mean and burst rate
   - Tolerances are appropriately loose to avoid flakiness

3. **Concurrency Testing**
   - `TestObservations_RecordIsThreadSafe` runs 16 concurrent goroutines
   - Race detector catches concurrency bugs (tests pass with `-race`)

4. **Edge Cases**
   - Empty collections (`TestObservations_Summary` with empty list)
   - Degenerate distributions (`TestTriangular` with `min == mode == max`)
   - Timeout scenarios (`TestRunScenarioEvents_EventCtxTimeoutFailsEvent`)

### 🟡 Test Gaps

1. **Missing Integration Test for Full Simulation**
   - The `main.go` orchestrates everything, but there's no test that runs a minimal simulation end-to-end
   - **Suggestion:** Add `TestRunScenario_SmallScale` that runs one scenario with a tiny cluster (e.g., 3 brokers, 5s observed duration)

2. **Error Path Coverage**
   - `gatherCounter` panics on missing metrics, but there's no test for the Gather error path (line 141)
   - **Suggestion:** Add test with a custom registry that returns an error from `Gather()`

3. **Scenario Validation**
   - No test that all scenarios in `scenarios()` have valid `expectedMinSuccessRate` in [0, 1]
   - **Suggestion:**
     ```go
     func TestScenarios_ExpectedRatesAreValid(t *testing.T) {
         for _, sc := range scenarios() {
             require.InRange(t, sc.expectedMinSuccessRate, 0.0, 1.0)
         }
     }
     ```

---

## Documentation

### ✅ Strong Documentation

1. **REPORT.md Output**
   - The generated report is clear, well-formatted, and self-explanatory
   - Tables are easy to scan, with consistent column widths
   - Error tables surface exactly which errors occurred and at what rate

2. **Code Comments**
   - Package-level and function-level docs are concise and accurate
   - `environment.go` explains why latency is injected client-side, not broker-side
   - `brokerBehaviour` doc comments clearly explain the fields

3. **Scenario Descriptions**
   - Each scenario has a prose description of what it tests and why
   - GCS outage scenario documents the incident it reproduces

### 🟡 Documentation Gaps

1. **Simulation Entry Point**
   - `main.go` lacks a package comment explaining what the binary does
   - **Suggestion:**
     ```go
     // Package main implements a simulation comparing wgo (Warpstream client) vs
     // kgo (franz-go) under various latency and failure scenarios. Run with:
     //   go run ./pkg/internal/simulation -report-filepath=REPORT.md
     ```

2. **PartitionAssignmentStrategy Reference**
   - README mentions "PartitionAssignmentStrategy" but the simulation doesn't exercise custom strategies
   - Consider linking to the strategy interface in the simulation's package docs

3. **Counter Sampler Boundary Logic**
   - `startCounterSampler` docs explain "exact at boundary", but don't mention the slow-scheduler fallback (line 74-76)
   - **Suggestion:** Expand the doc comment to cover jitter handling

---

## Performance & Efficiency

### ✅ Well-Optimized

1. **Parallel Scenario Execution**
   - All scenarios run concurrently (line 55-61 in `main.go`)
   - Minimizes wall-clock time for the full suite

2. **Per-Broker RNG Streams**
   - Seeding by `(broker, client)` avoids contention
   - Single mutex guards all RNGs, but draws are infrequent (per-request, not per-record)

3. **Efficient Bucketing**
   - `bucketedSummary` uses a map for sparse buckets, then densifies to a slice
   - Avoids allocating for gaps in time series

### 🟡 Minor Inefficiencies

1. **Redundant Map Cloning** (`broker_behaviour.go:110`)
   ```go
   p.current.Store(&brokersBehaviour{byBroker: maps.Clone(b.byBroker)})
   ```
   **Issue:** Every `set()` call clones the entire map. For 50 brokers × frequent swaps, this might add up.
   
   **Mitigation:** Profile first. If it's hot, consider copy-on-write or immutable maps.

2. **String Concatenation in Report Generation** (`results_report.go`)
   - `strings.Builder` is used correctly (good!), but some `fmt.Sprintf` calls inside loops
   - Consider pre-allocating the builder capacity if report size is predictable

---

## Security & Safety

### ✅ No Security Concerns

1. **No External Input**
   - Simulation is driven by internal scenario definitions
   - No user-supplied data flows into the system

2. **Resource Limits**
   - Timeouts prevent runaway scenarios (`budget` in `runScenario`)
   - Context cancellation propagates correctly

3. **No Credential Leaks**
   - No TLS/SASL in the test harness (uses kfake)
   - No sensitive data in logs or reports

### 🟡 Resource Management

1. **kfake Cluster Cleanup**
   - Deferred cleanup in `newEnvironment()` is correct
   - Consider adding a timeout on `cluster.Close()` in case it blocks (edge case)

---

## Maintainability

### ✅ Easy to Maintain

1. **Clear Module Boundaries**
   - Adding a new scenario is straightforward: append to `scenarios()`
   - Adding a new latency model: define a function, plug into `brokerBehaviour.latencyFn`

2. **Test Modularity**
   - Each component has focused unit tests
   - Tests are parallel and isolated

3. **No Hidden Dependencies**
   - Package `main` (simulation) only depends on `wgo`, `kgo`, and `kfake`
   - No circular dependencies

### 🟡 Future-Proofing

1. **Hard-Coded Cluster Size**
   - `clusterSize = 50` is baked into scenario definitions
   - If this needs to vary per scenario, consider making it a scenario field

2. **Report Format Versioning**
   - The generated Markdown has no schema version
   - If you add/remove columns, old reports won't be forward-compatible
   - Consider adding a version header: `<!-- schema: v1 -->`

---

## Specific Code Issues

### 🔴 Critical

None found.

### 🟡 Medium Priority

1. **Line 234 in `environment.go`**: Clarify `err` shadowing (see above)
2. **Line 27 in `scenario_runner.go`**: Extract magic constant `60*time.Second`
3. **`scenarios()` function**: Consider refactoring for readability (inline functions are hard to follow)

### 🟢 Low Priority / Nits

1. **`observationsSnapshot` vs `observations`**: The two types are similar; consider embedding or using composition to reduce duplication
2. **`scenarioResults` is a thin wrapper**: Just a slice with one method. Could be a plain slice + standalone function.
3. **`produceAPIVersion` constant** (line 286 in `environment.go`): The comment explains it mirrors wgo's pinning, but it's declared locally. Consider moving to a shared constant if this version is used elsewhere.

---

## Recommendations

### Must Fix

None. The code is ready to merge as-is.

### Should Fix (Before Merge)

1. Clarify the `err` variable shadowing in `latencyDelayedKgoClient.Produce` (line 234)
2. Extract the magic `60*time.Second` buffer into a named constant

### Nice to Have (Future PRs)

1. Add an end-to-end integration test for the full simulation pipeline
2. Refactor `scenarios()` to use a builder pattern for clarity
3. Document the rationale for `clusterSize=50` and `warmupDuration=30s`
4. Add report schema versioning for forward compatibility

---

## Comparison to Best Practices

### Adherence to Repository Conventions

✅ **Follows `/workspace/AGENTS.md`**:
- Comments explain "why", not "what"
- Test names follow `Test<Component>_<Method>` convention
- No unnecessary `assert`/`require` message arguments

✅ **Follows `/workspace/pkg/AGENTS.md`**:
- No obvious comments (`// increment counter`)
- Struct doc comments don't enumerate embedded fields

### Go Best Practices

✅ **Excellent**:
- Interfaces are small and focused
- Errors are handled at every call site
- Context cancellation is respected
- Race detector passes
- No naked returns
- Exported types/functions have doc comments

🟡 **Good**:
- Some magic constants could be named
- A few deeply nested inline functions

---

## Conclusion

This is **high-quality code** that demonstrates strong engineering discipline. The simulation framework is:

- **Architecturally sound**: Clear separation of concerns, parallel execution, realistic modeling
- **Well-tested**: Comprehensive unit tests with race detection and statistical validation
- **Maintainable**: Modular design, clear naming, minimal dependencies
- **Safe**: No security issues, proper resource cleanup, correct concurrency

The two medium-priority issues (err shadowing, magic constant) are minor and easy to fix. I recommend **merging after addressing those two points**.

### Final Grade: **A-**

Great work! The simulation framework will be a valuable tool for validating the client's behavior under failure conditions.

---

## Action Items for Author

- [ ] Fix `err` variable shadowing in `latencyDelayedKgoClient.Produce` (line 234 of environment.go)
- [ ] Extract `60*time.Second` buffer into `scenarioBufferDuration` constant
- [ ] (Optional) Add package comment to `main.go` explaining the simulation binary
- [ ] (Optional) Add `TestScenarios_ExpectedRatesAreValid` to catch invalid expected rates

---

**Reviewer:** Cursor Agent  
**Date:** 2026-06-18
