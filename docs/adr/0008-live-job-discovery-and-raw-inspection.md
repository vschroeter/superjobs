# Live Job discovery and raw worker inspection

**Status:** Accepted on 2026-10-06 in [Decide the discovery namespace and future contract enumeration boundary](https://github.com/vschroeter/superjobs/issues/51).

Expose a common jobs.discovery service: jobs() enumerates identities currently offered by live ready registrations, while workers() accepts either a shared Job definition for typed capability decoding or a JobIdentity for explicitly raw inspection. The client.workers() convenience uses the same path, giving contract-only producers and DTO-free inspectors consistent presence semantics.

The offered-Job view follows worker leases instead of persistent known-contract history. A permanent contract catalog and dynamic client construction remain separate future scopes, so current discovery can ship independently of immutable manifests and cannot imply restored Python types or contract agreement.

The issue's resolution is the canonical API and semantic record. Implementation and source/installed-wheel typing and transport verification remain required.
