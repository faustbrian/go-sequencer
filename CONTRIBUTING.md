# Contributing

Behavior changes start with a failing focused test. Keep operation discovery,
dependencies, time, persistence, authorization, and transport explicit. Do not
add global mutable state, reflection registration, filesystem scans, hidden
workers, or unbounded collections.

Run `make check` before submitting a change. PostgreSQL integration tests use a
temporary Testcontainers database. Public API changes require documentation,
compatibility review, and a changelog entry.
