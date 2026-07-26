# Ordering

The planner uses dependency edges, not source-file order. It performs a
deterministic topological sort with lexicographic tie-breaking. Identical input
therefore produces identical plans across replicas and deployments.

Missing dependencies and cycles are deployment errors. Dependencies must be
succeeded or explicitly skipped before the store admits a dependent claim.
Allowed failure does not silently satisfy a dependency.

Plan size, direct dependencies, and graph depth are bounded. Split very large
graphs into reviewed deployment phases instead of raising limits blindly.
