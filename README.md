# DiscordGo yeongaori fork

This is fork of [ozraru/discordgo](https://github.com/ozraru/discordgo).

Original repo of this project: [bwmarrin/discordgo](https://github.com/bwmarrin/discordgo).


This fork focuses on stability and simply “working well.”

## Autopatch branch

The `autopatch` branch builds on `dev` and retains the context-based voice API
and call-lifecycle fixes used by Discord Autopatch. Its reliability changes
include:

- WebSocket writes and writer-lock waits have a five-second limit. Voice
  join/disconnect writes also respect their caller's cancellation.
- Gateway discovery, connection and initial handshake share a twenty-second
  deadline. Explicit closure cancels reconnect retries; repeated failed
  resumes still fall back to a fresh identification after three attempts.
- Voice session-ID, status and DAVE readiness waits stop on cancellation or
  connection death. Readiness notifications synchronize with the waiters.
- Interaction callback buckets expire after a minute of inactivity once the
  server's reset time has passed. Active requests retain their shared bucket.
- REST requests share one deadline across bucket waits, rate-limit delays and
  HTTP retries: the effective client's timeout, or twenty seconds when unset.
  Earlier caller deadlines take precedence. HTTP 429 and retryable server
  errors share `MaxRestRetries`; exhausted rate-limit retries return
  `RateLimitError`.

Run `go test ./...` and `go vet ./...` on Windows or Linux. On Linux with a C
toolchain available, also run `go test -race ./...`. These tests do not require
libopus.
