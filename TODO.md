- [x] Learn how to construct a mailbox processor

Notes:
    - MailBoxP Encapsulates queue
    - Multi writers, single reader agent (specified in body)

Problems:
- [x] How to guard deserialization?
  - This should be done at the producer and it should manage it's own errors and re-subs.
- [x] How to bail out when the listener is bumped?

Thoughts:
- [x] The Event Processor is just a glorified function now.
    - Might be worth keeping as an Agent to eventually enable parallel processing (Like for NMI)
      - That might require more advanced Idempotency.
