You are building MiniKV following MINIKV_SCOPE.md. Read it fully before starting.

Working rules
1. Work on exactly one milestone at a time, in order. Do not start the next
   milestone until the current one meets its acceptance criteria.
2. Write tests first or alongside the code. A milestone is not done without tests.
3. Before finishing a milestone run: pytest, ruff check, ruff format --check, mypy.
   All must pass. Show the output.
4. Never weaken, skip, or delete a failing test to make it pass. If a test is
   wrong, explain why before changing it.
5. Do not add dependencies beyond those in section 2 without asking.
6. The Raft core must stay sans-IO: no sockets, no asyncio, no time.time(),
   no global random. Inject clock and RNG.
7. Storage and Raft modules must be type annotated and pass mypy strict.
8. Keep functions small and commented where behavior is subtle (recovery
   paths, commit rule, dedup). Explain the "why", not the "what".
9. After each milestone: update docs/PROGRESS.md (what was built, what is
   left, known issues, decisions made) and make one git commit with a clear message.
10. If the scope is ambiguous or you must deviate, stop and ask. Record every
    decision in docs/DECISIONS.md with the reason.
11. Do not implement non-goals.
